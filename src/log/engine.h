#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
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
#include <utility>

namespace faas {

// Forward declaration
namespace engine {
class Engine;
}

namespace log {

// class Engine;

struct TxnProgress {
    // uint64_t seqnum;
    LocalOp* pending_commit;
    std::atomic<size_t> remaining_ops;
    absl::flat_hash_map<uint64_t, /* expected_replicas */ std::atomic<uint16_t>>
        pending_writes;
    UserTagVec read_set;

    void init(UserTagVec& read_set,
              UserTagVec& write_set,
              uint16_t expected_replicas)
    {
        pending_commit = nullptr;
        remaining_ops = write_set.size();
        pending_writes.clear();
        for (uint64_t tag: write_set) {
            pending_writes[tag] = expected_replicas;
        }
        this->read_set = std::move(read_set);
    }
};

class TxnEngine {
public:
    explicit TxnEngine(const View* view, uint16_t engine_id, uint16_t sequencer_id);
    ~TxnEngine();

    uint16_t engine_id_;
    size_t engine_idx_;
    size_t replica_count_;
    uint32_t logspace_id_;

    absl::Mutex start_mu_;
    uint64_t next_localid_;
    utils::SimpleObjectPool<TxnProgress> txn_progress_pool_;
    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ LocalOp*>
        pending_txn_starts_ ABSL_GUARDED_BY(start_mu_);
    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ TxnProgress*>
        running_txns_ ABSL_GUARDED_BY(start_mu_);

    TxnProgress* RegisterTxnStart(LocalOp* op);

    absl::Mutex index_mu_;
    absl::flat_hash_map</* seqnum */ uint32_t, uint16_t> engine_ids_;
    absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;
    // The following fields are for index compaction
    // 1. active_seqnums are snapshots obtained at txn_start
    // when a txn commits, it releases its snapshot, by which we assume that
    // all aux data has been set and we can safely reconstruct view from the snapshot
    // 2. upon committing, we trim indices for all tags in read set
    std::set</* seqnum */ uint32_t> active_seqnums_;
    uint32_t kMaxIndexSeqnumOverhead;

    uint32_t applied_seqnum_position_;
    uint32_t buffered_seqnum_position_;
    utils::ProtobufMessagePool<TxnIndexProto> index_pool_;
    // apply when buffered position == indexproto.end_of_batch
    std::map</* start_seqnum */ uint32_t, TxnIndexProto*> pending_indices_;
};

class Engine final: public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

    LogSpaceCollection<CCIndex> cc_index_collection_;

    void ProcessTxnStartResult(
        const LogProducer::TxnStartResultVec& txn_start_results);
    void ProcessTxnCommitResult(
        const LogProducer::TxnCommitBatchResult& txn_commit_results);

    std::unique_ptr<TxnEngine> txn_engine_;

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
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalRead(LocalOp* op) override;
    void HandleLocalSetAuxData(LocalOp* op) override;

    void HandleLocalCCTxnStart(LocalOp* op) override;
    void HandleLocalCCTxnCommit(LocalOp* op) override;
    void HandleCCLocalRead(LocalOp* op);
    void ProcessCCIndexLookupResult(LocalOp* op, CCIndex::IndexEntry& idx_entry);
    void HandleLocalSetCCAuxData(LocalOp* op);

    void HandleRemoteRead(const protocol::SharedLogMessage& request) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;

    void OnRecvCCGlobalBatch(const protocol::SharedLogMessage& message,
                             std::span<const char> payload) override;
    void OnRecvCCReadResponse(LocalOp* op,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload);
    void OnCCOpReplicated(const protocol::SharedLogMessage& message,
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
