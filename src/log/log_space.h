#pragma once

#include "base/logging.h"
#include "log/common.h"
#include "log/log_space_base.h"
#include "proto/shared_log.pb.h"
#include "log/engine_base.h"
#include <cstdint>
#include <deque>
#include <memory>

namespace faas { namespace log {

// Used in Sequencer
class MetaLogPrimary final: public LogSpaceBase {
public:
    MetaLogPrimary(const View* view, uint16_t sequencer_id);
    ~MetaLogPrimary();

    uint32_t replicated_metalog_position() const
    {
        return replicated_metalog_position_;
    }
    bool all_metalog_replicated() const
    {
        return replicated_metalog_position_ == metalog_position();
    }

    void UpdateStorageProgress(uint16_t storage_id,
                               const std::vector<uint32_t>& progress);
    void UpdateReplicaProgress(uint16_t sequencer_id, uint32_t metalog_position);
    std::optional<MetaLogProto> MarkNextCut();

private:
    absl::flat_hash_set</* engine_id */ uint16_t> dirty_shards_;
    absl::flat_hash_map</* engine_id */ uint16_t, uint32_t> last_cut_;
    absl::flat_hash_map<std::pair</* engine_id */ uint16_t,
                                  /* storage_id */ uint16_t>,
                        uint32_t>
        shard_progrsses_;

    absl::flat_hash_map</* sequencer_id */ uint16_t, uint32_t> metalog_progresses_;
    uint32_t replicated_metalog_position_;

    uint32_t GetShardReplicatedPosition(uint16_t engine_id) const;
    void UpdateMetaLogReplicatedPosition();

    DISALLOW_COPY_AND_ASSIGN(MetaLogPrimary);
};

// Used in Sequencer
class MetaLogBackup final: public LogSpaceBase {
public:
    MetaLogBackup(const View* view, uint16_t sequencer_id);
    ~MetaLogBackup();

private:
    DISALLOW_COPY_AND_ASSIGN(MetaLogBackup);
};

// Used in Engine
class LogProducer final: public LogSpaceBase {
public:
    LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id);
    ~LogProducer();

    void LocalAppend(void* caller_data, uint64_t* localid);

    struct AppendResult {
        uint64_t seqnum; // seqnum == kInvalidLogSeqNum indicates failure
        uint64_t localid;
        uint64_t metalog_progress;
        void* caller_data;
    };
    using AppendResultVec = absl::InlinedVector<AppendResult, 4>;
    void PollAppendResults(AppendResultVec* results);

    uint64_t next_txn_localid_;
    struct TxnStartRequest {
        uint64_t txn_id;
        void* caller_data;
        // bool operator<(const TxnStartRequest& other) const
        // {
        //     return txn_id < other.txn_id;
        // }
    };
    std::deque<TxnStartRequest> pending_txn_starts_;
    void RegisterTxnStart(LocalOp* caller_data, uint64_t* txn_id);

    struct UnfinishedTxn {
        UnfinishedTxn* next;
        bool finished;
        uint64_t snapshot;
        UnfinishedTxn(uint64_t batch_id)
            : next(nullptr),
              finished(false),
              snapshot(batch_id)
        {}
    };
    UnfinishedTxn *head, *tail;
    absl::flat_hash_map</* txn_id */ uint64_t,
                        /* UnfinishedTxn* */ UnfinishedTxn*>
        unfinished_txns_;
    uint64_t finished_snapshot = 0;

    inline void AddUnfinishedTxn(uint64_t txn_id, uint64_t global_batch_id)
    {
        auto txn = new UnfinishedTxn(global_batch_id);
        if (head == nullptr) {
            head = tail = txn;
        } else {
            tail->next = txn;
            tail = txn;
        }
        unfinished_txns_[txn_id] = txn;
        VLOG(1) << "add unfinished txn " << txn_id << " at " << global_batch_id
                << " total " << unfinished_txns_.size();
    }

    inline uint64_t MarkTxnFinished(uint64_t txn_id)
    {
        uint64_t snapshot = 0;
        auto txn = unfinished_txns_.at(txn_id);
        txn->finished = true;
        unfinished_txns_.erase(txn_id);
        auto ptr = head;
        while (ptr != nullptr && ptr->finished) {
            snapshot = ptr->snapshot;
            head = ptr->next;
            if (ptr == tail) {
                tail = nullptr;
            }
            delete ptr;
            ptr = head;
        }
        return snapshot;
    }

    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ void*>
        pending_cc_ops_;
    absl::flat_hash_map</* localid */ uint64_t,
                        /* replica_cnt */ uint32_t>
        pending_replicas_;
    void RegisterCCOp(LocalOp* op, uint64_t* txn_localid);
    void SetExpectedReplicas(uint64_t txn_localid, uint32_t num_replicas);
    void* OnRecvReplicateOK(uint64_t txn_localid);

    struct TxnStartResult {
        void* caller_data;
        // uint64_t global_batch_id;
        uint64_t snapshot_seqnum;
    };
    using TxnStartResultVec = absl::InlinedVector<TxnStartResult, 64>;
    TxnStartResultVec pending_txn_start_results_;
    void PollTxnStartResults(TxnStartResultVec* results);

    struct TxnCommitResult {
        void* caller_data;
        uint64_t seqnum;
    };
    using TxnCommitBatchResult = absl::InlinedVector<TxnCommitResult, 64>;
    TxnCommitBatchResult pending_txn_commit_batch_results_;
    void ApplyCCGlobalBatch(const CCGlobalBatchProto& batch_proto);
    uint64_t ProvideCCGlobalBatch(const CCGlobalBatchProto& batch_proto);
    void PollTxnCommitBatchResults(TxnCommitBatchResult* results);

private:
    uint64_t next_localid_;
    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ void*>
        pending_appends_;

    AppendResultVec pending_append_results_;

    void OnNewLogs(uint32_t metalog_seqnum,
                   uint64_t start_seqnum,
                   uint64_t start_localid,
                   uint32_t delta) override;
    void OnFinalized(uint32_t metalog_position) override;

    DISALLOW_COPY_AND_ASSIGN(LogProducer);
};

// Used in Storage
class LogStorage final: public LogSpaceBase {
public:
    LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id);
    ~LogStorage();

    bool Store(const LogMetaData& log_metadata,
               std::span<const uint64_t> user_tags,
               std::span<const char> log_data);
    void ReadAt(const protocol::SharedLogMessage& request);

    void GrabLogEntriesForPersistence(
        std::vector<std::shared_ptr<const LogEntry>>* log_entries,
        uint64_t* new_position) const;
    void LogEntriesPersisted(uint64_t new_position);

    struct ReadResult {
        enum Status { kOK, kLookupDB, kFailed };
        Status status;
        std::shared_ptr<const LogEntry> log_entry;
        protocol::SharedLogMessage original_request;
    };
    using ReadResultVec = absl::InlinedVector<ReadResult, 4>;
    void PollReadResults(ReadResultVec* results);

    std::optional<IndexDataProto> PollIndexData();
    std::optional<std::vector<uint32_t>> GrabShardProgressForSending();

    size_t max_live_log_entries;

    // bool has_cc_logs_to_flush;
    // size_t persisted_cc_log_offset;
    size_t max_live_cc_entries;
    std::deque<uint64_t> live_txn_localids_;
    absl::flat_hash_map</* txn_localid */ uint64_t, std::shared_ptr<CCLogEntry>>
        live_cc_log_entries_;
    bool StoreCCLogEntry(CCLogEntry* cc_entry);
    void GrabCCLogEntriesForPersistence(
        std::vector<std::shared_ptr<CCLogEntry>>* cc_log_entries,
        size_t* num_entries) const;
    void CCLogEntriesPersisted(size_t num_entries);
    std::optional<std::shared_ptr<CCLogEntry>> ReadCCLogEntry(
        const protocol::SharedLogMessage& request);

private:
    const View::Storage* storage_node_;

    bool shard_progrss_dirty_;
    absl::flat_hash_map</* engine_id */ uint16_t,
                        /* localid */ uint32_t>
        shard_progrsses_;

    uint64_t persisted_seqnum_position_;
    std::deque<uint64_t> live_seqnums_;
    absl::flat_hash_map</* seqnum */ uint64_t, std::shared_ptr<const LogEntry>>
        live_log_entries_;

    absl::flat_hash_map</* localid */ uint64_t, std::unique_ptr<LogEntry>>
        pending_log_entries_;

    std::multimap</* seqnum */ uint64_t, protocol::SharedLogMessage>
        pending_read_requests_;
    ReadResultVec pending_read_results_;

    IndexDataProto index_data_;

    void OnNewLogs(uint32_t metalog_seqnum,
                   uint64_t start_seqnum,
                   uint64_t start_localid,
                   uint32_t delta) override;
    void OnFinalized(uint32_t metalog_position) override;

    void AdvanceShardProgress(uint16_t engine_id);
    void ShrinkLiveEntriesIfNeeded();

    DISALLOW_COPY_AND_ASSIGN(LogStorage);
};

}} // namespace faas::log
