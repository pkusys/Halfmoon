#include "gateway/http_connection.h"

#include "gateway/server.h"
#include "gateway/io_worker.h"

#include <absl/time/time.h>
#include <absl/time/clock.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

HttpConnection::HttpConnection(Server* server, int connection_id)
    : Connection(Connection::Type::Http, server),
      connection_id_(connection_id), io_worker_(nullptr),
      state_(kCreated), log_header_(absl::StrFormat("HttpConnection[%d]: ", connection_id)),
      within_async_request_(false) {
    http_parser_init(&http_parser_, HTTP_REQUEST);
    http_parser_.data = this;
    http_parser_settings_init(&http_parser_settings_);
    http_parser_settings_.on_message_begin = &HttpConnection::HttpParserOnMessageBeginCallback;
    http_parser_settings_.on_url = &HttpConnection::HttpParserOnUrlCallback;
    http_parser_settings_.on_header_field = &HttpConnection::HttpParserOnHeaderFieldCallback;
    http_parser_settings_.on_header_value = &HttpConnection::HttpParserOnHeaderValueCallback;
    http_parser_settings_.on_headers_complete = &HttpConnection::HttpParserOnHeadersCompleteCallback;
    http_parser_settings_.on_body = &HttpConnection::HttpParserOnBodyCallback;
    http_parser_settings_.on_message_complete = &HttpConnection::HttpParserOnMessageCompleteCallback;
}

HttpConnection::~HttpConnection() {
    CHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* HttpConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_CHECK_OK(uv_tcp_init(uv_loop, &uv_tcp_handle_));
    return reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_);
}

void HttpConnection::Start(IOWorker* io_worker) {
    CHECK(state_ == kCreated);
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    io_worker_ = io_worker;
    uv_tcp_handle_.data = this;
    response_write_req_.data = this;
    UV_CHECK_OK(uv_async_init(uv_tcp_handle_.loop,
                              &async_request_finished_event_,
                              &HttpConnection::AsyncRequestFinishCallback));
    async_request_finished_event_.data = this;
    state_ = kRunning;
    StartRecvData();
}

void HttpConnection::Reset(int connection_id) {
    CHECK(state_ == kClosed);
    connection_id_ = connection_id;
    log_header_ = absl::StrFormat("HttpConnection[%d]: ", connection_id);
    ResetHttpParser();
    state_ = kCreated;
}

void HttpConnection::ScheduleClose() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Already scheduled for closing";
        return;
    }
    CHECK(state_ == kRunning);
    if (within_async_request_) {
        async_request_context_->OnConnectionClose();
        async_request_context_ = nullptr;
        within_async_request_ = false;
    }
    closed_uv_handles_ = 0;
    uv_handles_is_closing_ = 2;
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_tcp_handle_),
             &HttpConnection::CloseCallback);
    uv_close(reinterpret_cast<uv_handle_t*>(&async_request_finished_event_),
             &HttpConnection::CloseCallback);
    state_ = kClosing;
}

void HttpConnection::StartRecvData() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not enable read event";
        return;
    }
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_),
                              &HttpConnection::BufferAllocCallback,
                              &HttpConnection::RecvDataCallback));
}

void HttpConnection::StopRecvData() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not enable read event";
        return;
    }
    UV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_)));
}

UV_READ_CB_FOR_CLASS(HttpConnection, RecvData) {
    if (nread > 0) {
        const char* data = buf->base;
        size_t length = static_cast<size_t>(nread);
        size_t parsed = http_parser_execute(&http_parser_, &http_parser_settings_, data, length);
        if (parsed < length) {
            HLOG(WARNING) << "HTTP parsing failed: "
                          << http_errno_name(static_cast<http_errno>(http_parser_.http_errno))
                          << ",  will close the connection";
            ScheduleClose();
        }

    } else if (nread < 0) {
        if (nread == UV_EOF || nread == UV_ECONNRESET) {
            HLOG(INFO) << "HttpConnection closed by client";
        } else {
            HLOG(WARNING) << "Read error, will close the connection: "
                          << uv_strerror(nread);
        }
        ScheduleClose();
    }
    if (buf->base != 0) {
        io_worker_->ReturnReadBuffer(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(HttpConnection, DataWritten) {
    HVLOG(1) << "Successfully write response, will resume receiving new data";
    if (status == 0) {
        StartRecvData();
    } else {
        HLOG(WARNING) << "Write error, will close the connection: " << uv_strerror(status);
        ScheduleClose();
    }
}

UV_ALLOC_CB_FOR_CLASS(HttpConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_ASYNC_CB_FOR_CLASS(HttpConnection, AsyncRequestFinish) {
    HttpAsyncRequestContext* context = async_request_context_.get();
    CHECK(context != nullptr);
    if (!within_async_request_) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not handle the finish of async request";
        return;
    }
    context->body_buffer_.Swap(body_buffer_);
    response_status_ = context->status_;
    response_content_type_ = context->content_type_;
    context->response_body_buffer_.Swap(response_body_buffer_);
    within_async_request_ = false;
    if (state_ != kRunning) {
        HLOG(WARNING) << "HttpConnection is closing or has closed, will not send response";
        return;
    }
    SendHttpResponse();
}

UV_CLOSE_CB_FOR_CLASS(HttpConnection, Close) {
    CHECK(state_ == kClosing);
    closed_uv_handles_++;
    if (closed_uv_handles_ == uv_handles_is_closing_) {
        state_ = kClosed;
        IOWorker* io_worker = io_worker_;
        io_worker_ = nullptr;
        io_worker->OnConnectionClose(this);
    }
}

void HttpConnection::HttpParserOnMessageBegin() {
    header_field_value_flag_ = -1;
    header_field_buffer_.Reset();
    header_value_buffer_.Reset();
    header_field_buffer_pos_ = 0;
    header_value_buffer_pos_ = 0;
    url_buffer_.Reset();
    headers_.clear();
}

void HttpConnection::HttpParserOnUrl(const char* data, size_t length) {
    url_buffer_.AppendData(data, length);
}

void HttpConnection::HttpParserOnHeaderField(const char* data, size_t length) {
    if (header_field_value_flag_ == 1) {
        HttpParserOnNewHeader();
    }
    header_field_buffer_.AppendData(data, length);
    header_field_value_flag_ = 0;
}

void HttpConnection::HttpParserOnHeaderValue(const char* data, size_t length) {
    header_value_buffer_.AppendData(data, length);
    header_field_value_flag_ = 1;
}

void HttpConnection::HttpParserOnHeadersComplete() {
    if (header_field_value_flag_ == 1) {
        HttpParserOnNewHeader();
    }
    body_buffer_.Reset();
}

void HttpConnection::HttpParserOnBody(const char* data, size_t length) {
    body_buffer_.AppendData(data, length);
}

namespace {
static bool ReadParsedUrlField(const http_parser_url* parsed_url, http_parser_url_fields field,
                               const char* url_buf, absl::string_view* result) {
    if ((parsed_url->field_set & (1 << field)) == 0) {
        return false;
    } else {
        *result = absl::string_view(url_buf + parsed_url->field_data[field].off,
                                    parsed_url->field_data[field].len);
        return true;
    }
}
}

void HttpConnection::HttpParserOnMessageComplete() {
    StopRecvData();
    HVLOG(1) << "Start parsing URL: " << std::string(url_buffer_.data(), url_buffer_.length());
    http_parser_url parsed_url;
    if (http_parser_parse_url(url_buffer_.data(), url_buffer_.length(), 0, &parsed_url) != 0) {
        HLOG(WARNING) << "Failed to parse URL, will close the connection";
        ScheduleClose();
        return;
    }
    absl::string_view path;
    if (!ReadParsedUrlField(&parsed_url, UF_PATH, url_buffer_.data(), &path)) {
        HLOG(WARNING) << "Parsed URL misses some fields";
        ScheduleClose();
        return;
    }
    OnNewHttpRequest(http_method_str(static_cast<http_method>(http_parser_.method)), path);
    ResetHttpParser();   
}

void HttpConnection::HttpParserOnNewHeader() {
    absl::string_view field(header_field_buffer_.data() + header_field_buffer_pos_,
                            header_field_buffer_.length() - header_field_buffer_pos_);
    header_field_buffer_pos_ = header_field_buffer_.length();
    absl::string_view value(header_value_buffer_.data() + header_value_buffer_pos_,
                            header_value_buffer_.length() - header_value_buffer_pos_);
    header_value_buffer_pos_ = header_value_buffer_.length();
    HVLOG(1) << "Parse new HTTP header: " << field << " = " << value;
    headers_[field] = value;
}

void HttpConnection::ResetHttpParser() {
    http_parser_init(&http_parser_, HTTP_REQUEST);
}

void HttpConnection::OnNewHttpRequest(absl::string_view method, absl::string_view path) {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    HVLOG(1) << "New HTTP request: " << method << " " << path;
    response_status_ = 200;
    response_content_type_ = kDefaultContentType;
    response_body_buffer_.Reset();
    const Server::RequestHandler* request_handler;
    if (!server_->MatchRequest(method, path, &request_handler)) {
        response_status_ = 404;
        SendHttpResponse();
        return;
    }
    if (request_handler->async()) {
        if (async_request_context_ == nullptr) {
            async_request_context_.reset(new HttpAsyncRequestContext());
        }
        HttpAsyncRequestContext* context = async_request_context_.get();
        context->method_ = std::string(method);
        context->path_ = std::string(path);
        for (const auto& item : headers_) {
            context->headers_[std::string(item.first)] = std::string(item.second);
        }
        context->body_buffer_.Swap(body_buffer_);
        context->status_ = 200;
        context->content_type_ = kDefaultContentType;
        context->response_body_buffer_.Swap(response_body_buffer_);
        context->connection_ = this;
        within_async_request_ = true;
        request_handler->CallAsync(async_request_context_);
    } else {
        HttpSyncRequestContext context;
        context.method_ = method;
        context.path_ = path;
        context.headers_ = &headers_;
        context.body_ = body_buffer_.data();
        context.body_length_ = body_buffer_.length();
        context.status_ = &response_status_;
        context.content_type_ = &response_content_type_;
        context.response_body_buffer_ = &response_body_buffer_;
        request_handler->CallSync(&context);
        SendHttpResponse();
    }
}

void HttpConnection::AsyncRequestFinish(HttpAsyncRequestContext* context) {
    UV_CHECK_OK(uv_async_send(&async_request_finished_event_));
}

void HttpConnection::SendHttpResponse() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    HVLOG(1) << "Send HTTP response with status " << response_status_;
    static const char* CRLF = "\r\n";
    std::ostringstream header;
    header << "HTTP/1.1 " << response_status_ << " ";
    switch (response_status_) {
    case 200:
        header << "OK";
        break;
    case 400:
        header << "Bad Request";
        break;
    case 404:
        header << "Not Found";
        break;
    case 500:
        header << "Internal Server Error";
        break;
    default:
        LOG(FATAL) << "Unsupported status code " << response_status_;
    }
    header << CRLF;
    header << "Date: " << absl::FormatTime(absl::RFC1123_full, absl::Now(), absl::UTCTimeZone()) << CRLF;
    header << "Server: FaaS/0.1" << CRLF;
    header << "Content-Type: " << response_content_type_ << CRLF;
    header << "Content-Length: " << response_body_buffer_.length() << CRLF;
    header << "HttpConnection: Keep-Alive" << CRLF;
    header << CRLF;
    response_header_buffer_.Reset();
    response_header_buffer_.AppendStr(header.str());
    uv_buf_t bufs[] = {
        { .base = response_header_buffer_.data(), .len = response_header_buffer_.length() },
        { .base = response_body_buffer_.data(), .len = response_body_buffer_.length() }
    };
    UV_CHECK_OK(uv_write(&response_write_req_, reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_),
                            bufs, 2, &HttpConnection::DataWrittenCallback));
}

int HttpConnection::HttpParserOnMessageBeginCallback(http_parser* http_parser) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnMessageBegin();
    return 0;
}

int HttpConnection::HttpParserOnUrlCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnUrl(data, length);
    return 0;
}

int HttpConnection::HttpParserOnHeaderFieldCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnHeaderField(data, length);
    return 0;
}

int HttpConnection::HttpParserOnHeaderValueCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnHeaderValue(data, length);
    return 0;
}

int HttpConnection::HttpParserOnHeadersCompleteCallback(http_parser* http_parser) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnHeadersComplete();
    return 0;
}

int HttpConnection::HttpParserOnBodyCallback(http_parser* http_parser, const char* data, size_t length) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnBody(data, length);
    return 0;
}

int HttpConnection::HttpParserOnMessageCompleteCallback(http_parser* http_parser) {
    HttpConnection* self = reinterpret_cast<HttpConnection*>(http_parser->data);
    self->HttpParserOnMessageComplete();
    return 0;
}

}  // namespace gateway
}  // namespace faas