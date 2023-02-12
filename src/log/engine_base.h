#pragma once

#include "common/protocol.h"
#include "common/zk.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/index.h"
#include "log/cache.h"
#include "server/io_worker.h"
#include "utils/object_pool.h"
#include "utils/appendable_buffer.h"
#include <cstdint>
#include <memory>

namespace faas {

// Forward declaration
namespace engine {
class Engine;
}

namespace log {

struct LocalOp {
    bool is_cond_op;
    protocol::SharedLogOpType type;
    uint16_t client_id;
    // uint32_t call_id;
    uint32_t user_logspace;
    // uint32_t logspace_id;
    uint32_t cond_pos;
    uint64_t cond_tag;
    uint64_t localid;
    uint64_t query_tag;
    uint64_t seqnum;
    uint64_t client_data;
    uint64_t metalog_progress;
    uint64_t id;
    uint64_t func_call_id;
    int64_t start_timestamp;
    UserTagVec user_tags;
    utils::AppendableBuffer data;
};

using protocol::SharedLogMessage;
using protocol::SharedLogOpType;
using protocol::SharedLogMessageHelper;

inline SharedLogMessage
NewCCReplicateMessage(log::LocalOp* op)
{
    NEW_EMPTY_SHAREDLOG_MESSAGE(message);
    // message.op_type = static_cast<uint16_t>(SharedLogOpType::REPLICATE);
    message.op_type = static_cast<uint16_t>(op->type);
    message.localid = op->localid;
    message.num_tags = gsl::narrow_cast<uint16_t>(op->user_tags.size());
    // all history tags must be cond tags too
    if (op->is_cond_op) {
        message.cond_tag = op->cond_tag;
        message.cond_pos = op->cond_pos;
        message.flags |= protocol::kSLogIsCondOpFlag;
    }
    // else, cond_tag=0(kEmtpy)
    return message;
}

inline SharedLogMessage
NewCCTxnWriteMessage(log::LocalOp* op)
{
    NEW_EMPTY_SHAREDLOG_MESSAGE(message);
    message.op_type = static_cast<uint16_t>(op->type);
    message.localid = op->localid;
    message.query_tag = op->query_tag;
    message.seqnum_lowhalf = bits::LowHalf64(op->seqnum);
    // message.logspace_id = bits::HighHalf64(op->seqnum);
    return message;
}

inline SharedLogMessage
NewCCReadLogMessage(log::LocalOp* op)
{
    NEW_EMPTY_SHAREDLOG_MESSAGE(message);
    message.op_type = static_cast<uint16_t>(SharedLogOpType::CC_READ_LOG);
    message.logspace_id = bits::HighHalf64(op->seqnum);
    message.seqnum_lowhalf = bits::LowHalf64(op->seqnum); // though not used
    message.localid = op->localid;
    // message.origin_node_id = node_id_;
    message.client_data = op->id;
    return message;
}

inline SharedLogMessage
NewCCReadKVSMessage(log::LocalOp* op)
{
    NEW_EMPTY_SHAREDLOG_MESSAGE(message);
    message.op_type = static_cast<uint16_t>(SharedLogOpType::CC_READ_KVS);
    message.query_seqnum = op->seqnum;
    message.query_tag = op->query_tag;
    // message.origin_node_id = node_id_;
    message.client_data = op->id;
    return message;
}

class EngineBase {
public:
    explicit EngineBase(engine::Engine* engine);
    virtual ~EngineBase();

    void Start();
    void Stop();

    void OnRecvSharedLogMessage(int conn_type,
                                uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);

    void OnNewExternalFuncCall(const protocol::FuncCall& func_call,
                               uint32_t log_space);
    void OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call);
    void OnMessageFromFuncWorker(const protocol::Message& message);

    bool use_txn_engine_;

    bool ReplicateCCLogEntry(const View* view,
                             protocol::SharedLogMessage& message,
                             std::span<const uint64_t> user_tags,
                             std::span<const char> log_data);

    void CCLogCachePut(uint64_t localid, std::span<const char> log_data);
    void CCLogCachePut(uint64_t seqnum,
                       uint64_t key,
                       std::span<const char> log_data);
    std::optional<std::string> CCLogCacheGet(uint64_t localid);
    std::optional<std::string> CCLogCacheGet(uint64_t seqnum, uint64_t key);

    void CCLogCachePutAuxData(uint64_t seqnum,
                              //   uint64_t key,
                              std::span<const char> aux_data);
    std::optional<std::string> CCLogCacheGetAuxData(uint64_t seqnum);

    bool SendStorageCCReadRequest(protocol::SharedLogMessage& request,
                                  const View::Engine* engine_node);

protected:
    uint16_t my_node_id() const { return node_id_; }
    inline void RecycleLocalOp(LocalOp* op) { log_op_pool_.Return(op); }

    zk::ZKSession* zk_session();
    virtual void OnViewCreated(const View* view) = 0;
    virtual void OnViewFrozen(const View* view) = 0;
    virtual void OnViewFinalized(const FinalizedView* finalized_view) = 0;

    virtual void HandleRemoteRead(const protocol::SharedLogMessage& request) = 0;
    virtual void OnRecvNewMetaLog(const protocol::SharedLogMessage& message,
                                  std::span<const char> payload) = 0;
    virtual void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                                    std::span<const char> payload) = 0;
    virtual void OnRecvResponse(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) = 0;

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    virtual void HandleLocalAppend(LocalOp* op) = 0;
    virtual void HandleLocalTrim(LocalOp* op) = 0;
    virtual void HandleLocalRead(LocalOp* op) = 0;
    virtual void HandleLocalSetAuxData(LocalOp* op) = 0;

    // virtual void HandleLocalCCTxnStart(LocalOp* op) = 0;
    virtual void HandleLocalCCTxnCommit(LocalOp* op) = 0;
    virtual void HandleLocalCCTxnWrite(LocalOp* op) = 0;

    void LocalOpHandler(LocalOp* op);

    void ReplicateLogEntry(const View* view,
                           const LogMetaData& log_metadata,
                           std::span<const uint64_t> user_tags,
                           std::span<const char> log_data);
    void PropagateAuxData(const View* view,
                          const LogMetaData& log_metadata,
                          std::span<const char> aux_data);

    void FinishLocalOpWithResponse(LocalOp* op,
                                   protocol::Message* response,
                                   uint64_t metalog_progress = 0,
                                   bool recycle = true);
    void FinishLocalOpWithFailure(LocalOp* op,
                                  protocol::SharedLogResultType result,
                                  uint64_t metalog_progress = 0);

    void LogCachePut(const LogMetaData& log_metadata,
                     std::span<const uint64_t> user_tags,
                     std::span<const char> log_data);
    std::optional<LogEntry> LogCacheGet(uint64_t seqnum);
    void LogCachePutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> LogCacheGetAuxData(uint64_t seqnum);

    bool SendIndexReadRequest(const View::Sequencer* sequencer_node,
                              protocol::SharedLogMessage* request);
    bool SendStorageReadRequest(const IndexQueryResult& result,
                                const View::Engine* engine_node);
    void SendReadResponse(const IndexQuery& query,
                          protocol::SharedLogMessage* response,
                          std::span<const char> user_tags_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> data_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> aux_data_payload = EMPTY_CHAR_SPAN);
    void SendReadFailureResponse(const IndexQuery& query,
                                 protocol::SharedLogResultType result_type,
                                 uint64_t metalog_progress = 0);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN);

    server::IOWorker* SomeIOWorker();

private:
    const uint16_t node_id_;
    engine::Engine* engine_;

    ViewWatcher view_watcher_;

    utils::ThreadSafeObjectPool<LocalOp> log_op_pool_;
    std::atomic<uint64_t> next_local_op_id_;

    struct FnCallContext {
        uint32_t user_logspace;
        uint64_t metalog_progress;
        uint64_t parent_call_id;
    };

    absl::Mutex fn_ctx_mu_;
    absl::flat_hash_map</* full_call_id */ uint64_t, FnCallContext> fn_call_ctx_
        ABSL_GUARDED_BY(fn_ctx_mu_);

    std::optional<LRUCache> log_cache_;

    void SetupZKWatchers();
    void SetupTimers();

    void PopulateLogTagsAndData(const protocol::Message& message, LocalOp* op);

    DISALLOW_COPY_AND_ASSIGN(EngineBase);
};

} // namespace log
} // namespace faas
