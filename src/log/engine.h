#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "common/protocol.h"
#include "log/common.h"
#include "log/engine_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/log_space_base.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
#include "utils/bits.h"
#include "utils/object_pool.h"
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace faas {

// Forward declaration
namespace engine {
class Engine;
}

namespace log {

// class Engine;

struct TxnProgress {
    LocalOp* pending_commit;
    std::atomic<size_t> remaining_ops = 0;
    // // only used if direct response from storage
    // absl::flat_hash_map<uint64_t, /* expected_replicas */ std::atomic<uint16_t>>
    //     pending_writes;
    // UserTagVec read_set;

    TxnProgress(LocalOp* commit_op /* uint16_t expected_replicas */)
    {
        pending_commit = commit_op;
        remaining_ops = commit_op->user_tags.size();
        // pending_writes.clear();
        // for (uint64_t tag: write_set) {
        //     pending_writes[tag] = expected_replicas;
        // }
        // this->read_set = std::move(read_set);
    }

    LocalOp* finishOp()
    {
        // must register commit op before the last async write
        if (remaining_ops.fetch_sub(1, std::memory_order_relaxed) == 1) {
            return pending_commit;
        }
        return nullptr;
    }
};

class TxnEngine {
public:
    explicit TxnEngine(const View* view, uint16_t engine_id, uint16_t sequencer_id);
    ~TxnEngine() = default;

    const View* view_;
    uint16_t engine_id_;
    int engine_idx_;
    // size_t replica_count_;
    uint32_t logspace_id_;

    using FinishedOpVec = absl::InlinedVector<LocalOp*, 8>;
    struct CondOpResult {
        uint64_t localid;
        uint64_t seqnum; // kInvalidLogSeqnum if cond failed
    };
    using CondOpResultVec = absl::InlinedVector<CondOpResult, 8>;

    // indexed using localid
    struct IndexInfo {
        bool is_txn;
        uint16_t engine_id;
        uint32_t localid_lowhalf; // high half is engine_id
        uint64_t cond_tag;
    };
    struct IndexQueryResult {
        IndexInfo index_info;
        LocalOp* op;
        // directly modifies op->query_seqnum
    };
    using IndexQueryResultVec = absl::InlinedVector<IndexQueryResult, 8>;

    // metalogs do not need to be contiguous
    // though it is unlikely that they arrive out of order
    // we use the contiguous version here
    ProtoBuffer<MetaLogProto> metalog_buffer_;
    ProtoBuffer<PerEngineIndexProto> index_buffer_; // must be contiguous

    absl::Mutex append_mu_;
    uint64_t next_localid_;
    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ LocalOp*>
        pending_ops_; // ABSL_GUARDED_BY(append_mu_)
    absl::flat_hash_map</* seqnum */ uint32_t, std::unique_ptr<TxnProgress>>
        pending_txns_; // ABSL_GUARDED_BY(append_mu_)
    FinishedOpVec finished_ops_;

    uint64_t RegisterOp(LocalOp* op);
    void RegisterTxnCommitOp(LocalOp* op);

    void ApplyMetaLogs(std::vector<MetaLogProto*>& metalog_protos);
    void PollFinishedOps(FinishedOpVec& finished_ops);
    void FinishOp(uint64_t localid, uint64_t seqnum, bool check_cond = true);
    void FinishCondOps(CondOpResultVec& cond_op_results);

    absl::Mutex index_mu_;
    uint32_t indexed_seqnum_;
    absl::flat_hash_map</* seqnum */ uint32_t, IndexInfo> seqnum_info_;
    absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;
    // query
    std::multimap</* seqnum */ uint32_t, LocalOp*>
        pending_queries_; // ABSL_GUARDED_BY(index_mu_)
    CondOpResultVec cond_op_results_;
    IndexQueryResultVec pending_query_results_;

    void ApplyIndices(std::vector<PerEngineIndexProto*>& engine_indices);
    void ApplyIndex(const PerLogIndexProto& log_index, uint64_t localid);
    void PollCondOpResults(CondOpResultVec& cond_op_results);
    void PollIndexQueryResults(IndexQueryResultVec& query_results);
    // optional indicates whether the read op blocks on future seqnum
    // op->seqnum contains the result; invalid indicates no such entry
    std::optional<IndexInfo> LookUp(LocalOp* op);
    IndexInfo LookUpCurrentIndex(LocalOp* op);
    std::optional<IndexInfo> LookUpFutureIndex(LocalOp* op);

    // // The following fields are for index compaction
    // // 1. active_seqnums are snapshots obtained at txn_start
    // // when a txn commits, it releases its snapshot, by which we assume that
    // // all aux data has been set and we can safely reconstruct view from the
    // snapshot
    // // 2. upon committing, we trim indices for all tags in read set
    // std::set</* seqnum */ uint32_t> active_seqnums_;
    // uint32_t kMaxIndexSeqnumOverhead;
};

class Engine final: public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

    std::unique_ptr<TxnEngine> txn_engine_;
    log_utils::ThreadSafeHashBucket<uint64_t /* localid */, LocalOp*>
        pending_log_reads_;
    log_utils::ThreadSafeHashBucket<
        std::pair<uint64_t /* seqnum */, uint64_t /* key */>,
        LocalOp*>
        pending_kvs_reads_;

    void ProcessFinishedOp(LocalOp* op);
    void ProcessIndexQueryResults(TxnEngine::IndexInfo& index_info, LocalOp* op);
    void ReadFromKVS(LocalOp* op);
    void ReadFromLog(LocalOp* op);
    void ProcessKVSReadResult(const protocol::SharedLogMessage& message,
                              std::span<const char> payload);
    void ProcessLogReadResult(const protocol::SharedLogMessage& message,
                              std::span<const char> payload);

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_ ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_ ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_ ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogProducer> producer_collection_ ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<Index> index_collection_ ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;
    log_utils::ThreadedMap<LocalOp> onging_reads_;

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleLocalAppend(LocalOp* op) override;
    void SLogLocalAppend(LocalOp* op);
    void TxnEngineLocalAppend(LocalOp* op);

    void HandleLocalRead(LocalOp* op) override;
    void SLogLocalRead(LocalOp* op);
    void TxnEngineLocalRead(LocalOp* op);

    void HandleRemoteRead(const protocol::SharedLogMessage& request) override;
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalSetAuxData(LocalOp* op) override;

    void HandleLocalCCTxnCommit(LocalOp* op) override;
    void HandleLocalCCTxnWrite(LocalOp* op) override;

    void OnRecvNewMetaLog(const protocol::SharedLogMessage& message,
                          std::span<const char> payload) override;
    void SLogRecvMetaLog(const protocol::SharedLogMessage& message,
                         std::span<const char> payload);
    void TxnEngineRecvMetaLog(const protocol::SharedLogMessage& message,
                              std::span<const char> payload);

    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void SLogRecvIndex(const protocol::SharedLogMessage& message,
                       std::span<const char> payload);
    void TxnEngineRecvIndex(const protocol::SharedLogMessage& message,
                            std::span<const char> payload);

    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;
    void SLogRecvResponse(const protocol::SharedLogMessage& message,
                          std::span<const char> payload);
    void TxnEngineRecvResponse(const protocol::SharedLogMessage& message,
                               std::span<const char> payload);

    void ProcessAppendResults(const LogProducer::AppendResultVec& results);
    void ProcessIndexQueryResults(const Index::QueryResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void ProcessIndexFoundResult(const IndexQueryResult& query_result);
    void ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                    Index::QueryResultVec* more_results);

    inline LogMetaData MetaDataFromAppendOp(LocalOp* op)
    {
        DCHECK(op->type == protocol::SharedLogOpType::APPEND);
        return LogMetaData{.user_logspace = op->user_logspace,
                           .seqnum = kInvalidLogSeqNum,
                           .localid = 0,
                           .num_tags = op->user_tags.size(),
                           .data_size = op->data.length()};
    }

    protocol::SharedLogMessage BuildReadRequestMessage(LocalOp* op);
    protocol::SharedLogMessage BuildReadRequestMessage(
        const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(LocalOp* op);
    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

} // namespace log
} // namespace faas
