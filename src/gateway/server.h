#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/uv.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "server/server_base.h"
#include "gateway/func_call_context.h"
#include "gateway/http_connection.h"
#include "gateway/engine_connection.h"

namespace faas {
namespace gateway {

class Server final : public server::ServerBase {
public:
    static constexpr int kDefaultListenBackLog = 64;
    static constexpr int kDefaultNumIOWorkers = 1;

    Server();
    ~Server();

    void set_address(std::string_view address) { address_ = std::string(address); }
    void set_engine_conn_port(int port) { engine_conn_port_ = port; }
    void set_http_port(int port) { http_port_ = port; }
    void set_listen_backlog(int value) { listen_backlog_ = value; }
    void set_num_io_workers(int value) { num_io_workers_ = value; }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    FuncConfig* func_config() { return &func_config_; }

    // Must be thread-safe
    void OnNewHttpFuncCall(HttpConnection* connection, FuncCallContext* func_call_context);
    void OnRecvEngineMessage(EngineConnection* connection,
                             const protocol::GatewayMessage& message,
                             std::span<const char> payload);

private:
    std::string address_;
    int engine_conn_port_;
    int http_port_;
    int listen_backlog_;
    int num_io_workers_;
    std::string func_config_file_;
    std::string func_config_json_;
    FuncConfig func_config_;

    uv_tcp_t uv_engine_conn_handle_;
    uv_tcp_t uv_http_handle_;
    std::vector<server::IOWorker*> io_workers_;

    size_t next_http_conn_worker_id_;
    int next_http_connection_id_;

    class OngoingEngineHandshake;
    friend class OngoingEngineHandshake;

    absl::flat_hash_set<std::unique_ptr<OngoingEngineHandshake>> ongoing_engine_handshakes_;
    absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>> engine_connections_;
    utils::BufferPool read_buffer_pool_;
    absl::flat_hash_map</* node_id */ uint16_t, size_t> next_engine_conn_worker_id_;

    std::atomic<uint32_t> next_call_id_;
    std::atomic<int> inflight_requests_;

    absl::Mutex mu_;

    absl::flat_hash_map</* full_call_id */ uint64_t,
                        std::pair</* connection_id */ int, FuncCallContext*>>
        running_func_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* connection_id */ int,
                        std::shared_ptr<server::ConnectionBase>>
        connections_ ABSL_GUARDED_BY(mu_);

    int64_t last_request_timestamp_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> requests_instant_rps_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> inflight_requests_stat_ ABSL_GUARDED_BY(mu_);

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    bool OnEngineHandshake(uv_tcp_t* uv_handle, std::span<const char> data);

    DECLARE_UV_CONNECTION_CB_FOR_CLASS(HttpConnection);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(EngineConnection);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadEngineHandshake);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace gateway
}  // namespace faas
