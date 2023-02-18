#pragma once

#include "base/logging.h"
#include "log/common.h"
#include "log/log_space_base.h"
#include "proto/shared_log.pb.h"
#include "log/engine_base.h"
#include "utils/object_pool.h"
#include <cstdint>
#include <deque>
#include <memory>

namespace faas { namespace log {

template <class T>
class ProtoBuffer {
public:
    // absl::Mutex buffer_mu_;
    // always starts from 0; seqnum of proto should be lowhalf
    size_t buffer_position_ = 0;
    utils::SimpleObjectPool<T> proto_pool_;
    absl::flat_hash_map</* buffer_position */ size_t, T*>
        pending_protos_; // ABSL_GUARDED_BY(buffer_mu_)

    static size_t currentPosition(T* proto);
    static size_t nextPosition(T* proto);

    void ProvideRaw(std::span<const char> payload);
    void ProvideAllocated(T* proto);
    std::optional<std::vector<T*>> PollBuffer();
    void Recycle(std::vector<T*>& protos);
};

template <class T>
void
ProtoBuffer<T>::ProvideRaw(std::span<const char> payload)
{
    T* proto = proto_pool_.Get();
    proto->ParseFromArray(payload.data(), static_cast<int>(payload.size()));
    size_t pos = currentPosition(proto);
    if (pos < buffer_position_ || pending_protos_.contains(pos)) {
        proto_pool_.Return(proto);
        return;
    }
    pending_protos_[pos] = proto;
}

template <class T>
void
ProtoBuffer<T>::ProvideAllocated(T* proto)
{
    size_t pos = currentPosition(proto);
    if (pos < buffer_position_ || pending_protos_.contains(pos)) {
        return;
    }
    T* buf = proto_pool_.Get();
    // buf->Swap(proto);
    *buf = std::move(*proto);
    pending_protos_[pos] = buf;
}

template <class T>
std::optional<std::vector<T*>>
ProtoBuffer<T>::PollBuffer()
{
    size_t pos = buffer_position_;
    size_t cnt = 0;
    while (pending_protos_.contains(pos)) {
        pos = nextPosition(pending_protos_[pos]);
        cnt++;
    }
    if (cnt == 0) {
        return std::nullopt;
    }
    std::vector<T*> protos;
    protos.reserve(cnt);
    for (size_t i = 0; i < cnt; i++) {
        protos.push_back(pending_protos_[buffer_position_]);
        pending_protos_.erase(buffer_position_);
        buffer_position_ = nextPosition(protos[i]);
    }
    return std::move(protos);
}

template <class T>
void
ProtoBuffer<T>::Recycle(std::vector<T*>& protos)
{
    // absl::MutexLock lk(&buffer_mu_);
    for (T* proto: protos) {
        proto_pool_.Return(proto);
    }
}

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
