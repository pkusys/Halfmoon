#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "base/logging.h"
#include "common/protocol.h"
#include "log/sequencer_base.h"
#include "log/log_space.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
#include "utils/object_pool.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace faas { namespace log {

using WriteTable = absl::flat_hash_set<uint64_t>;

class CCReorderEngine;
struct Node {
    // uint64_t txn_id;
    absl::flat_hash_set<uint64_t> in_edges;
    absl::flat_hash_set<uint64_t> out_edges;
    // absl::flat_hash_set<uint64_t> read_txns;
    // absl::flat_hash_set<uint64_t> write_txns;
    void Clear()
    {
        in_edges.clear();
        out_edges.clear();
    }
};

struct ConflictGraph {
    std::vector<TxnCommitProto*>* batch_for_reorder;
    // absl::InlinedVector<TxnCommitProto, 64> batch_for_reorder;
    absl::flat_hash_map<uint64_t /* offset */, Node> txn_nodes;
    absl::flat_hash_map<uint64_t, WriteTable> write_table;
    absl::InlinedVector<uint64_t, 64> active_txn_offsets;
    absl::InlinedVector<uint64_t, 64> head;
    absl::InlinedVector<uint64_t, 64> tail;
    CCReorderEngine* cc_reorder_engine_;
    // absl::Mutex* abort_mu_;
    // utils::SimpleObjectPool<TxnCommitProto>* txn_commit_proto_pool_;
    // absl::InlinedVector<uint64_t, 64> pending_aborts; // guarded by abort_mu_
    size_t kQuickSelect = 1;

    // inline void InsertBatch(TxnCommitProto** begin, TxnCommitProto** end)
    // {
    //     // batch_for_reorder.insert(batch_for_reorder.end(),
    //     //                          std::make_move_iterator(begin),
    //     //                          std::make_move_iterator(end));
    //     batch_for_reorder.insert(batch_for_reorder.end(), begin, end);
    // }

    // inline size_t BatchSize() const { return batch_for_reorder.size(); }

    inline void Clear(/* size_t quickselect_k */)
    {
        head.clear();
        tail.clear();
        active_txn_offsets.clear();
        // txn_nodes.clear();
        for (auto iter = txn_nodes.begin(); iter != txn_nodes.end(); iter++) {
            // CHECK(&iter->second == &txn_nodes[iter->first]);
            iter->second.Clear();
        }
        write_table.clear();
        // per_engine_aborts.clear();
        // kQuickSelect = quickselect_k;
    }

    void AbortTxnByOffset(uint64_t offset);

    // state view should be readlocked
    void PreValidate(const absl::flat_hash_map<uint64_t, uint64_t>& state_view);

    void BuildConflictGraph();

    void Trim(absl::InlinedVector<uint64_t, 64>* head = nullptr,
              absl::InlinedVector<uint64_t, 64>* tail = nullptr);

    void RemoveNode(uint64_t offset,
                    absl::InlinedVector<uint64_t, 64>* head,
                    absl::InlinedVector<uint64_t, 64>* tail);

    void Acyclic();

    void FillTxnCommitLocalBatch(TxnCommitLocalBatchProto* batch_proto);
};

class Sequencer;

class CCReorderEngine {
public:
    // explicit CCReorderEngine(const View* view,
    //                          uint16_t sequencer_id,
    //                          size_t num_io_workers);
    explicit CCReorderEngine(const View* view,
                             Sequencer* sequencer,
                             size_t num_io_workers);
    ~CCReorderEngine() {}
    Sequencer* sequencer_;
    const View* current_view_;
    uint32_t logspace_id_;
    std::atomic<bool> pending_{false};

    // uint32_t identifier_;
    // uint32_t identifier() const { return identifier_; }
    size_t kQuickSelect;
    size_t kBatchSize;
    size_t num_io_workers_;

    struct TxnStarts {
        uint64_t max_txn_id = 0;
        absl::Mutex mu_;
    };
    absl::flat_hash_map<uint16_t, std::unique_ptr<TxnStarts>> pending_txn_starts_;
    // absl::flat_hash_map<uint16_t, uint64_t> last_txn_starts_;
    // absl::flat_hash_map<uint16_t, absl::InlinedVector<uint64_t, 16>>
    //     per_engine_aborts_;

    struct TxnAborts {
        absl::Mutex mu_;
        absl::InlinedVector<uint64_t, 16> txn_ids;
    };
    absl::flat_hash_map<uint16_t, std::unique_ptr<TxnAborts>> pending_txn_aborts_;

    // Txn Starts and aborts are local ops, not shared with peer sequencers
    // engines are shared among sequencers

    absl::Mutex append_mu_;
    // absl::InlinedVector<TxnCommitProto, 64> pending_requests;
    std::vector<TxnCommitProto*> pending_requests;
    int64_t last_batch_time_ = 0;
    int64_t max_interval_;
    std::atomic<int> timer_count_{0};
    int max_interval_rel_;
    void CollectLocalBatch(std::vector<TxnCommitProto*>* batch_for_reorder,
                           int64_t now);

    // absl::Mutex abort_mu_;
    // std::vector<uint64_t> pending_aborts;

    utils::ThreadSafeObjectPool<TxnCommitLocalBatchProto> local_batch_pool_;
    utils::ThreadSafeObjectPool<ConflictGraph> conflict_graph_pool_;

    inline ConflictGraph* GetConflictGraph()
    {
        auto graph = conflict_graph_pool_.Get();
        graph->Clear();
        graph->cc_reorder_engine_ = this;
        graph->kQuickSelect = kQuickSelect;
        return graph;
    }
    struct CommitRecord {
        uint64_t txn_id;
        uint32_t offset;
    };

    template <class T>
    using PerEngineVec = absl::flat_hash_map<uint16_t, absl::InlinedVector<T, 16>>;
    // struct PerWorkerLogSpace {
    //     // utils::Arena arena_;
    //     // absl::InlinedVector<TxnCommitProto, 16> worker_batch; // append_mu_
    //     // ConflictGraph conflict_graph;

    //     // utils::ProtobufMessagePoolWithArena<TxnCommitProto> txn_commit_pool;
    //     // utils::SimpleObjectPool<TxnCommitProto> txn_commit_pool;
    //     // utils::SimpleObjectPool<TxnCommitLocalBatchProto> local_batch_pool;

    //     absl::flat_hash_map<uint16_t, absl::InlinedVector<uint64_t, 16>>
    //         per_engine_commits;
    //     absl::flat_hash_map<uint16_t, absl::InlinedVector<uint64_t, 16>>
    //         per_engine_aborts;
    //     absl::flat_hash_map<uint64_t, uint64_t> flat_index;
    // };
    // std::vector<std::unique_ptr<PerWorkerLogSpace>> per_worker_log_spaces_;

    // inline PerWorkerLogSpace& MyWorkerSpace()
    // {
    //     return *per_worker_log_spaces_[MyWorkerId()];
    // }

    inline size_t MyWorkerId() { return server::IOWorker::current()->worker_id(); }

    std::atomic<uint64_t> next_local_batch_id_{0};
    // std::atomic<uint64_t> next_global_batch_id_{0};
    uint64_t next_global_batch_id_{0};
    uint64_t current_seqnum_{1};

    absl::Mutex apply_mu_;
    // uint64_t next_local_batch_id_to_apply_{0}; // entirely local for now
    // absl::flat_hash_map<uint64_t, CCLocalBatchProto>
    //     pending_cc_batches; // key is local_batch_id
    absl::flat_hash_map<uint64_t, uint64_t> state_view ABSL_GUARDED_BY(apply_mu_);

    // get proto from per worker pool
    bool AppendTxnCommitProto(TxnCommitProto& txn_commit_proto,
                              std::vector<TxnCommitProto*>* batch_for_reorder);
    // get proto from per worker pool, first pre validation
    TxnCommitLocalBatchProto* DoReorder(
        std::vector<TxnCommitProto*>* batch_for_reorder);
    // just a single sequencer for now
    CCGlobalBatchProto ApplyLocalBatch(TxnCommitLocalBatchProto* batch_proto,
                                       PerEngineVec<uint64_t>& aborts,
                                       PerEngineVec<CommitRecord>& commits);
    // global id should be set before this call
    void FillGlobalBatchForEngine(CCGlobalBatchProto* batch_proto,
                                  uint16_t engine_id,
                                  absl::InlinedVector<CommitRecord, 16> commits,
                                  absl::InlinedVector<uint64_t, 16> aborts);
    void OnReorderRequest(const protocol::SharedLogMessage& message,
                          std::span<const char> payload);
    void ProcessLocalBatch(std::vector<TxnCommitProto*>& batch_for_reorder);
};

class Sequencer final: public SequencerBase {
public:
    explicit Sequencer(uint16_t node_id);
    ~Sequencer();

    absl::flat_hash_map<uint32_t, std::unique_ptr<CCReorderEngine>>
        cc_reorder_engine_collections_;

    void OnRecvTxnStartRequest(const protocol::SharedLogMessage& message) override;
    void OnRecvReorderRequest(const protocol::SharedLogMessage& message,
                              std::span<const char> payload) override;
    void GrantTxnStartIds() override;

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_ ABSL_GUARDED_BY(view_mu_);
    LockablePtr<MetaLogPrimary> current_primary_ ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<MetaLogPrimary> primary_collection_ ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<MetaLogBackup> backup_collection_ ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleTrimRequest(const protocol::SharedLogMessage& request) override;
    void OnRecvMetaLogProgress(const protocol::SharedLogMessage& message) override;
    void OnRecvShardProgress(const protocol::SharedLogMessage& message,
                             std::span<const char> payload) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;

    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void MarkNextCutIfDoable() override;

    DISALLOW_COPY_AND_ASSIGN(Sequencer);
};

}} // namespace faas::log
