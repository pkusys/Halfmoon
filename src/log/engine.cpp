#include "log/engine.h"

#include "engine/engine.h"
#include "utils/bits.h"

namespace faas {
namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr),
      current_view_active_(false) {}

Engine::~Engine() {}

void Engine::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG(INFO) << fmt::format("New view {} created", view->id());
    bool contains_myself = view->contains_engine_node(my_node_id());
    if (!contains_myself) {
        HLOG(WARNING) << fmt::format("View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    std::vector<std::pair<LogMetaData, LocalOp*>> new_appends;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            const View::Engine* engine_node = view->GetEngineNode(my_node_id());
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                producer_collection_.InstallLogSpace(std::make_unique<LogProducer>(
                    my_node_id(), view, sequencer_id));
                if (engine_node->HasIndexFor(sequencer_id)) {
                    index_collection_.InstallLogSpace(std::make_unique<Index>(
                        view, sequencer_id));
                }
            }
        }
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        if (contains_myself) {
            current_view_active_ = true;
            std::vector<std::pair<uint64_t, LocalOp*>> appends;
            pending_appends_.PollAllSorted(&appends);
            for (const auto& [op_id, op] : appends) {
                LogMetaData log_metadata = MetaDataFromAppendOp(op);
                uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
                log_metadata.seqnum = bits::JoinTwo32(logspace_id, 0);
                auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
                producer_ptr.Lock()->LocalAppend(op, &log_metadata.localid);
                new_appends.push_back(std::make_pair(log_metadata, op));
            }
        }
        views_.push_back(view);
        log_header_ = fmt::format("LogEngine[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG(INFO) << fmt::format("{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] () {
                ProcessRequests(requests);
            }
        );
    }
    if (!new_appends.empty()) {
        HLOG(INFO) << fmt::format("{} appends for the new view", new_appends.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, view, new_appends = std::move(new_appends)] () {
                for (const auto& [log_metadata, op] : new_appends) {
                    ReplicateLogEntry(view, log_metadata, op->data.to_span());
                }
            }
        );
    }
}

void Engine::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG(INFO) << fmt::format("View {} frozen", view->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(view->id(), current_view_->id());
    if (view->contains_engine_node(my_node_id())) {
        DCHECK(current_view_active_);
        current_view_active_ = false;
    }
}

void Engine::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG(INFO) << fmt::format("View {} finalized", finalized_view->view()->id());
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        producer_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &append_results, this] (uint32_t logspace_id,
                                                     LockablePtr<LogProducer> producer_ptr) {
                auto locked_producer = producer_ptr.Lock();
                bool success = locked_producer->Finalize(
                    finalized_view->final_metalog_position(logspace_id),
                    finalized_view->tail_metalogs(logspace_id));
                if (!success) {
                    HLOG(FATAL) << fmt::format("Failed to finalize log space {}",
                                                bits::HexStr0x(logspace_id));
                }
                LogProducer::AppendResultVec tmp;
                locked_producer->PollAppendResults(&tmp);
                append_results.insert(append_results.end(), tmp.begin(), tmp.end());
            }
        );
        if (!append_results.empty()) {
            LogProducer::AppendResultVec tmp;
            for (const LogProducer::AppendResult& result : append_results) {
                if (result.seqnum == kInvalidLogSeqNum) {
                    LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
                    pending_appends_.PutChecked(op->id, op);
                } else {
                    tmp.push_back(result);
                }
            }
            append_results = std::move(tmp);
        }
        index_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &query_results, this] (uint32_t logspace_id,
                                                    LockablePtr<Index> index_ptr) {
                auto locked_index = index_ptr.Lock();
                bool success = locked_index->Finalize(
                    finalized_view->final_metalog_position(logspace_id),
                    finalized_view->tail_metalogs(logspace_id));
                if (!success) {
                    HLOG(FATAL) << fmt::format("Failed to finalize log space {}",
                                                bits::HexStr0x(logspace_id));
                }
                Index::QueryResultVec tmp;
                locked_index->PollQueryResults(&tmp);
                query_results.insert(query_results.end(), tmp.begin(), tmp.end());
            }
        );
    }
    ProcessAppendResults(append_results);
    ProcessIndexQueryResults(query_results);
}

// Start handlers for local requests (from functions)

#define ONHOLD_IF_FROM_FUTURE_VIEW(LOCAL_OP_VAR)                        \
    do {                                                                \
        if (current_view_ == nullptr                                    \
                || GetLastViewId(LOCAL_OP_VAR) > current_view_->id()) { \
            future_requests_.OnHoldRequest(                             \
                GetLastViewId(LOCAL_OP_VAR),                            \
                SharedLogRequest(LOCAL_OP_VAR));                        \
            return;                                                     \
        }                                                               \
    } while (0)

void Engine::HandleLocalAppend(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::APPEND);
    HVLOG(1) << fmt::format("Handle local append: op_id={}, logspace={}, tag={}, size={}",
                            op->id, op->user_logspace, op->user_tag, op->data.length());
    const View* view = nullptr;
    LogMetaData log_metadata = MetaDataFromAppendOp(op);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            pending_appends_.PutChecked(op->id, op);
            return;
        }
        view = current_view_;
        uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        log_metadata.seqnum = bits::JoinTwo32(logspace_id, 0);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid);
        }
    }
    ReplicateLogEntry(view, log_metadata, op->data.to_span());
}

void Engine::HandleLocalTrim(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Engine::HandleLocalRead(LocalOp* op) {
    DCHECK(  op->type == SharedLogOpType::READ_NEXT
          || op->type == SharedLogOpType::READ_PREV);
    int direction = (op->type == SharedLogOpType::READ_NEXT) ? 1 : -1;
    HVLOG(1) << fmt::format("Handle local read: op_id={}, logspace={}, tag={}, direction={}",
                            op->id, op->user_logspace, op->user_tag, direction);
    onging_reads_.PutChecked(op->id, op);
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(op);
        uint64_t query_seqnum = op->seqnum;
        uint16_t view_id = bits::HighHalf32(bits::HighHalf64(query_seqnum));
        if (direction < 0 && query_seqnum == kMaxLogSeqNum) {
            // This is a CheckTail read
            view_id = current_view_->id();
        }
        if (view_id != current_view_->id()) {
            NOT_IMPLEMENTED();
        }
        uint32_t logspace_id = current_view_->LogSpaceIdentifier(op->user_logspace);
        const View::Engine* engine_node = current_view_->GetEngineNode(my_node_id());
        if (!engine_node->HasIndexFor(bits::LowHalf32(logspace_id))) {
            NOT_IMPLEMENTED();
        }
        IndexQuery query = {
            .direction = (direction > 0) ? IndexQuery::kReadNext : IndexQuery::kReadPrev,
            .origin_node_id = my_node_id(),
            .hop_times = 0,
            .client_data = op->id,
            .user_logspace = op->user_logspace,
            .user_tag = op->user_tag,
            .query_seqnum = query_seqnum,
            .metalog_progress = op->metalog_progress
        };
        auto index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW

// Start handlers for remote messages

#define ONHOLD_IF_FROM_FUTURE_VIEW(MESSAGE_VAR, PAYLOAD_VAR)        \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            future_requests_.OnHoldRequest(                         \
                (MESSAGE_VAR).view_id,                              \
                SharedLogRequest(MESSAGE_VAR, PAYLOAD_VAR));        \
            return;                                                 \
        }                                                           \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG(WARNING) << "Receive outdate request from view "   \
                          << (MESSAGE_VAR).view_id;                 \
            return;                                                 \
        }                                                           \
    } while (0)

void Engine::HandleRemoteRead(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(  op_type == SharedLogOpType::READ_NEXT
          || op_type == SharedLogOpType::READ_PREV);
    NOT_IMPLEMENTED();
}

void Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_producer->ProvideMetaLog(metalog_proto);
            }
            locked_producer->PollAppendResults(&append_results);
        }
        if (current_view_->GetEngineNode(my_node_id())->HasIndexFor(message.sequencer_id)) {
            auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                    locked_index->ProvideMetaLog(metalog_proto);
                }
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessAppendResults(append_results);
    ProcessIndexQueryResults(query_results);
}

void Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                                std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(), payload.size())) {
        LOG(FATAL) << "Failed to parse IndexDataProto";
    }
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        const View* view = views_.at(message.view_id);
        if (!view->contains_engine_node(my_node_id())) {
            HLOG(FATAL) << fmt::format("View {} does not contain myself", view->id());
        }
        const View::Engine* engine_node = view->GetEngineNode(my_node_id());
        if (!engine_node->HasIndexFor(message.sequencer_id)) {
            HLOG(FATAL) << fmt::format("This node is not index node for log space {}",
                                       bits::HexStr0x(message.logspace_id));
        }
        auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            locked_index->ProvideIndexData(index_data_proto);
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void Engine::OnRecvResponse(const SharedLogMessage& message,
                            std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::RESPONSE);
    SharedLogResultType result = SharedLogMessageHelper::GetResultType(message);
    if (    result == SharedLogResultType::READ_OK
         || result == SharedLogResultType::EMPTY
         || result == SharedLogResultType::DATA_LOST) {
        uint64_t op_id = message.client_data;
        LocalOp* op;
        if (!onging_reads_.Poll(op_id, &op)) {
            HLOG(WARNING) << fmt::format("Cannot find read op with id {}", op_id);
            return;
        }
        if (result == SharedLogResultType::READ_OK) {
            uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
            Message response = MessageHelper::NewSharedLogOpSucceeded(
                SharedLogResultType::READ_OK, seqnum);
            response.log_tag = message.user_tag;
            if (payload.size() > MESSAGE_INLINE_DATA_SIZE) {
                HLOG(FATAL) << fmt::format("Log data too large: size={}", payload.size());
            }
            MessageHelper::SetInlineData(&response, payload);
            FinishLocalOpWithResponse(op, &response, message.user_metalog_progress);
        } else if (result == SharedLogResultType::EMPTY) {
            FinishLocalOpWithFailure(
                op, SharedLogResultType::EMPTY, message.user_metalog_progress);
        } else if (result == SharedLogResultType::DATA_LOST) {
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        } else {
            UNREACHABLE();
        }
    } else {
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
    }
}

void Engine::ProcessAppendResults(const LogProducer::AppendResultVec& results) {
    for (const LogProducer::AppendResult& result : results) {
        DCHECK_NE(result.seqnum, kInvalidLogSeqNum);
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogResultType::APPEND_OK, result.seqnum);
        FinishLocalOpWithResponse(op, &response, result.metalog_progress);
    }
}

void Engine::ProcessIndexQueryResults(const Index::QueryResultVec& results) {
    for (const IndexQueryResult& result : results) {
        const IndexQuery& query = result.original_query;
        if (query.origin_node_id != my_node_id()) {
            NOT_IMPLEMENTED();
        }
        uint64_t op_id = query.client_data;
        switch (result.state) {
        case IndexQueryResult::kFound:
            if (!SendReadRequest(result)) {
                LocalOp* op = onging_reads_.PollChecked(op_id);
                FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
            }
            break;
        case IndexQueryResult::kEmpty:
            FinishLocalOpWithFailure(
                onging_reads_.PollChecked(op_id), SharedLogResultType::EMPTY,
                result.metalog_progress);
            break;
        case IndexQueryResult::kContinue:
            NOT_IMPLEMENTED();
            break;
        default:
            UNREACHABLE();
        }
    }
}

void Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        if (request.local_op == nullptr) {
            MessageHandler(request.message, STRING_TO_SPAN(request.payload));
        } else {
            LocalOpHandler(reinterpret_cast<LocalOp*>(request.local_op));
        }
    }
}

}  // namespace log
}  // namespace faas