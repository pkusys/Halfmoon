#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "common/protocol.h"
#include "log/common.h"
#include "log/storage_base.h"
#include "log/log_space.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
// #include "tsl/ordered_map.h"
#include "utils/object_pool.h"
#include <memory>
#include <utility>

namespace faas { namespace log {

using protocol::SharedLogMessage;

template <class KeyType, class ValueType>
class InMemStore {
public:
    using ItemType = std::pair<KeyType, std::shared_ptr<ValueType>>;
    const size_t max_size;
    // not thread safe
    size_t persisted_offset_;
    std::deque<KeyType> live_keys_;
    absl::flat_hash_map<KeyType, std::shared_ptr<ValueType>> store_;

    explicit InMemStore(size_t max_size)
        : max_size(max_size),
          persisted_offset_(0)
    {}

    void OnItemReplicated(const KeyType& key) { live_keys_.push_back(key); }
    void PollItemsForPersistence(std::vector<ItemType>& items);
    void TrimPersistedItems();

    inline void OnItemsPersisted(size_t num_persisted)
    {
        persisted_offset_ += num_persisted;
        TrimPersistedItems();
    }

    inline std::shared_ptr<ValueType> Get(const KeyType& key) const
    {
        if (!store_.contains(key)) {
            return nullptr;
        }
        return store_.at(key);
    }

    inline void Remove(const KeyType& key) { store_.erase(key); }

    inline void PutAllocated(const KeyType& key,
                             std::shared_ptr<ValueType>& value,
                             bool live = false)
    {
        store_.emplace(key, value);
        if (live) {
            live_keys_.push_back(key);
        }
    }

    inline bool Contains(const KeyType& key) const { return store_.contains(key); }
};

class CCStorage {
public:
    explicit CCStorage(const View* view, uint16_t storage_id, uint16_t sequencer_id);

    const View* view_;
    const View::Storage* storage_node_;
    uint32_t logspace_id_;

    // metalogs must be applied in order, because we use indexed_seqnum_ to determine
    // whether a kvs read should block or query the DB
    ProtoBuffer<MetaLogProto> metalog_buffer_;

    absl::Mutex store_mu_;
    bool shard_progress_dirty_;
    // implies interested shards
    absl::flat_hash_map</* engine_id */ uint16_t,
                        /* localid */ uint32_t>
        shard_progresses_;
    uint64_t indexed_seqnum_;
    PerStorageIndexProto index_data_;

    using VersionedKeyType = std::pair<uint64_t /* seqnum */, uint64_t /* key */>;
    InMemStore<uint64_t /* localid */, CCLogEntry> log_store_;
    InMemStore<VersionedKeyType, std::string> kv_store_;

    using LogItemType = InMemStore<uint64_t /* localid */, CCLogEntry>::ItemType;
    using KVItemType = InMemStore<VersionedKeyType, std::string>::ItemType;

    void ApplyMetaLogs(std::vector<MetaLogProto*>& metalog_protos);
    void ApplyMetaLogForEngine(uint32_t start_seqnum,
                               uint64_t start_localid,
                               uint32_t shard_delta);
    std::optional<PerStorageIndexProto> PollIndexData();
    // log read using localid never blocks, since logs are always replicated
    // before publishing the corresponding metalog
    std::shared_ptr<CCLogEntry> GetLog(const SharedLogMessage& request);
    // if optional is empty then the read blocks
    std::optional<std::shared_ptr<std::string>> GetKV(
        const SharedLogMessage& request);
    // void StoreLog(uint64_t localid, CCLogEntry* log_entry);
    void StoreLog(uint64_t localid, std::shared_ptr<CCLogEntry> log_entry);
    // void StoreKV(uint64_t seqnum, uint64_t key, std::string* value);
    void StoreKV(uint64_t seqnum, uint64_t key, std::shared_ptr<std::string> value);
    void AdvanceShardProgress(uint16_t engine_id);
    void PollShardProgress(std::vector<uint32_t>& progress);

    absl::flat_hash_map<VersionedKeyType,
                        std::vector<SharedLogMessage> /* blocking reads */>
        blocking_kvs_reads_;

    void PollBlockingKVSReads(const VersionedKeyType& versioned_key,
                              std::vector<SharedLogMessage>& blocking_reads);
};

class Storage final: public StorageBase {
public:
    explicit Storage(uint16_t node_id);
    ~Storage();

    std::unique_ptr<CCStorage> cc_storage_;

    // void OnRecvSLogReplicateRequest(const protocol::SharedLogMessage& message,
    //                                 std::span<const char> payload);
    // void OnRecvCCReplicateRequest(const protocol::SharedLogMessage& message,
    //                               std::span<const char> payload);

    // void OnRecvSLogReadAtRequest(const protocol::SharedLogMessage& request);
    // void OnRecvCCReadAtRequest(const protocol::SharedLogMessage& request);

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_ ABSL_GUARDED_BY(view_mu_);
    bool view_finalized_ ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogStorage> storage_collection_ ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleReadAtRequest(const protocol::SharedLogMessage& request) override;
    void HandleCCReadKVSRequest(const protocol::SharedLogMessage& request) override;
    void HandleCCReadLogRequest(const protocol::SharedLogMessage& request) override;

    void HandleReplicateRequest(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) override;
    void SLogHandleReplicateRequest(const protocol::SharedLogMessage& message,
                                    std::span<const char> payload);
    void CCHandleReplicateRequest(const protocol::SharedLogMessage& message,
                                  std::span<const char> payload);
    void HandleCCTxnWriteRequest(const protocol::SharedLogMessage& message,
                                 std::span<const char> payload) override;

    void OnRecvNewMetaLog(const protocol::SharedLogMessage& message,
                          std::span<const char> payload) override;
    void SLogRecvMetaLog(const protocol::SharedLogMessage& message,
                         std::span<const char> payload);
    void CCRecvMetaLog(const protocol::SharedLogMessage& message,
                       std::span<const char> payload);
    void OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                          std::span<const char> payload) override;

    void ProcessReadFromDB(const protocol::SharedLogMessage& request);
    void ProcessCCReadLogFromDB(const protocol::SharedLogMessage& request,
                                protocol::SharedLogMessage& response);
    void ProcessCCReadKVSFromDB(const protocol::SharedLogMessage& request,
                                protocol::SharedLogMessage& response);

    void ProcessReadResults(const LogStorage::ReadResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             std::span<const char> tags_data,
                             std::span<const char> log_data);

    void SendShardProgressIfNeeded() override;
    void SLogSendShardProgress();
    void CCSendShardProgress();

    void FlushLogEntries();
    void SLogFlushToDB();
    void CCFlushToDB();

    void BackgroundThreadMain() override;

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}} // namespace faas::log
