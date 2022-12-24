#pragma once

#include "log/common.h"
#include "log/engine_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/log_space_base.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
#include "utils/bits.h"

namespace faas {

// Forward declaration
namespace engine {
class Engine;
}

namespace log {

class Engine final: public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

    LogSpaceCollection<CCIndex> cc_index_collection_;

    void ProcessTxnStartResult(
        const LogProducer::TxnStartResultVec& txn_start_results);
    void ProcessTxnCommitResult(
        const LogProducer::TxnCommitBatchResult& txn_commit_results);
    
    // std::atomic<uint64_t> finished_snapshot_;

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
