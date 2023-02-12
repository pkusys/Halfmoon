#pragma once

#include "absl/synchronization/mutex.h"
#include "base/logging.h"
#include "common/protocol.h"
#include "log/common.h"
#include "log/engine_base.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "proto/shared_log.pb.h"
#include "utils/lockable_ptr.h"
#include <cstdint>
#include <cstring>
#include <string>
#include <sys/types.h>
#include <vector>

namespace faas { namespace log_utils {

using log::UserTagVec;
using log::CCLogMetaData;
using log::CCLogEntry;

uint16_t GetViewId(uint64_t value);

// Used for on-holding requests for future views
class FutureRequests {
public:
    FutureRequests();
    ~FutureRequests();

    // Both `OnNewView` and `OnHoldRequest` are thread safe

    // If `ready_requests` is nullptr, will panic if there are on-hold requests
    void OnNewView(const log::View* view,
                   std::vector<log::SharedLogRequest>* ready_requests);
    void OnHoldRequest(uint16_t view_id, log::SharedLogRequest request);

private:
    absl::Mutex mu_;

    uint16_t next_view_id_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* view_id */ uint16_t, std::vector<log::SharedLogRequest>>
        onhold_requests_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(FutureRequests);
};

template <class T>
class ThreadedMap {
public:
    ThreadedMap();
    ~ThreadedMap();

    // All these APIs are thread safe
    void Put(uint64_t key, T* value);        // Override if the given key exists
    bool Poll(uint64_t key, T** value);      // Remove the given key if it is found
    void PutChecked(uint64_t key, T* value); // Panic if key exists
    T* PollChecked(uint64_t key);            // Panic if key does not exist
    void RemoveChecked(uint64_t key);        // Panic if key does not exist
    void PollAll(std::vector<std::pair<uint64_t, T*>>* values);
    void PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values);

private:
    absl::Mutex mu_;
    absl::flat_hash_map<uint64_t, T*> rep_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadedMap);
};

// Start implementation of ThreadedMap
template <class T>
ThreadedMap<T>::ThreadedMap()
{}

template <class T>
ThreadedMap<T>::~ThreadedMap()
{
#if DCHECK_IS_ON()
    if (!rep_.empty()) {
        LOG_F(WARNING, "There are {} elements left", rep_.size());
    }
#endif
}

template <class T>
void
ThreadedMap<T>::Put(uint64_t key, T* value)
{
    absl::MutexLock lk(&mu_);
    rep_[key] = value;
}

template <class T>
bool
ThreadedMap<T>::Poll(uint64_t key, T** value)
{
    absl::MutexLock lk(&mu_);
    if (rep_.contains(key)) {
        *value = rep_.at(key);
        rep_.erase(key);
        return true;
    } else {
        return false;
    }
}

template <class T>
void
ThreadedMap<T>::PutChecked(uint64_t key, T* value)
{
    absl::MutexLock lk(&mu_);
    DCHECK(!rep_.contains(key));
    rep_[key] = value;
}

template <class T>
T*
ThreadedMap<T>::PollChecked(uint64_t key)
{
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    T* value = rep_.at(key);
    rep_.erase(key);
    return value;
}

template <class T>
void
ThreadedMap<T>::RemoveChecked(uint64_t key)
{
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    rep_.erase(key);
}

template <class T>
void
ThreadedMap<T>::PollAll(std::vector<std::pair<uint64_t, T*>>* values)
{
    absl::MutexLock lk(&mu_);
    values->resize(rep_.size());
    if (values->empty()) {
        return;
    }
    size_t i = 0;
    for (const auto& [key, value]: rep_) {
        (*values)[i++] = std::make_pair(key, value);
    }
    DCHECK_EQ(i, rep_.size());
    rep_.clear();
}

template <class T>
void
ThreadedMap<T>::PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values)
{
    PollAll(values);
    if (values->empty()) {
        return;
    }
    std::sort(values->begin(),
              values->end(),
              [](const std::pair<uint64_t, T*>& lhs,
                 const std::pair<uint64_t, T*>& rhs) -> bool {
                  return lhs.first < rhs.first;
              });
}

log::MetaLogProto MetaLogFromPayload(std::span<const char> payload);

log::LogMetaData GetMetaDataFromMessage(const protocol::SharedLogMessage& message);
void SplitPayloadForMessage(const protocol::SharedLogMessage& message,
                            std::span<const char> payload,
                            std::span<const uint64_t>* user_tags,
                            std::span<const char>* log_data,
                            std::span<const char>* aux_data);

void PopulateMetaDataToMessage(const log::LogMetaData& metadata,
                               protocol::SharedLogMessage* message);
void PopulateMetaDataToMessage(const log::LogEntryProto& log_entry,
                               protocol::SharedLogMessage* message);

template <class T>
inline bool
is_aligned(const void* ptr) noexcept
{
    auto iptr = reinterpret_cast<std::uintptr_t>(ptr);
    return !(iptr % alignof(T));
}

template <class KeyType, class ValueType>
class ThreadSafeHashBucket {
public:
    ThreadSafeHashBucket() = default;
    bool Put(KeyType key, ValueType value)
    {
        absl::MutexLock lk(&mu_);
        bool exists = buckets_.contains(key);
        buckets_[key].push_back(value);
        return exists;
    }
    void Poll(KeyType key, std::vector<ValueType>& result)
    {
        absl::MutexLock lk(&mu_);
        if (buckets_.contains(key)) {
            result = std::move(buckets_[key]);
            buckets_.erase(key);
        }
    }

private:
    absl::Mutex mu_;
    absl::flat_hash_map<KeyType, std::vector<ValueType>> buckets_
        ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadSafeHashBucket);
};

template <class T>
void
FinalizedLogSpace(LockablePtr<T> logspace_ptr,
                  const log::FinalizedView* finalized_view)
{
    auto locked_logspace = logspace_ptr.Lock();
    uint32_t logspace_id = locked_logspace->identifier();
    bool success = locked_logspace->Finalize(
        finalized_view->final_metalog_position(logspace_id),
        finalized_view->tail_metalogs(logspace_id));
    if (!success) {
        LOG_F(FATAL, "Failed to finalize log space {}", bits::HexStr0x(logspace_id));
    }
}

// inline CCLogMetaData
// CCMetaDataFromOp(log::LocalOp* op)
// {
//     auto metadata = CCLogMetaData{
//         .op_type = op->type,
//         .num_tags = op->user_tags.size(),
//         .cond_pos = op->cond_pos,
//         .cond_tag = op->cond_tag,
//     };
//     if (op->type == protocol::SharedLogOpType::CC_TXN_WRITE) {
//         metadata.write_tag = op->query_tag;
//     }
// }

// TODO: set an option in read to indicate if full log entry(in recovery) or only the
// data is needed. For engine cache, only the data is stored

inline void
PopulateCCMetaData(const protocol::SharedLogMessage& message,
                   CCLogMetaData* metadata)
{
    metadata->op_type = message.op_type;
    metadata->num_tags = message.num_tags;
    metadata->cond_pos = message.cond_pos;
    metadata->cond_tag = message.cond_tag;
    // omit txn write; these are persisted in KVS
    // if (message.op_type ==
    //     static_cast<uint16_t>(protocol::SharedLogOpType::CC_TXN_WRITE))
    // {
    //     metadata->write_tag = message.query_tag;
    // }
}

inline void
PopulateCCLogEntry(const protocol::SharedLogMessage& message,
                   std::span<const char> payload,
                   CCLogEntry* log_entry)
{
    PopulateCCMetaData(message, &log_entry->metadata);
    const char* ptr = payload.data();
    const uint64_t* tags_ptr = reinterpret_cast<const uint64_t*>(ptr);
    log_entry->user_tags.insert(log_entry->user_tags.end(),
                                tags_ptr,
                                tags_ptr + message.num_tags);
    size_t tags_data_size = message.num_tags * sizeof(uint64_t);
    ptr += tags_data_size;
    DCHECK(message.payload_size >= tags_data_size);
    log_entry->data.assign(ptr, message.payload_size - tags_data_size);
}

// layout: data | tags | metadata
// tags must be 8bytes-aligned
inline void
EncodeCCLogEntry(std::string& dst, const CCLogEntry& src)
{
    // cannot use move since the log entry may still be present in memory
    // dst = std::move(src.data);
    dst.resize(sizeof(CCLogMetaData) + src.user_tags.size() * sizeof(uint64_t) +
               src.data.size());
    dst.replace(0, src.data.size(), src.data);
    char* ptr = dst.data();
    memcpy(ptr, src.data.data(), src.data.size());
    ptr += src.data.size();
    memcpy(ptr, src.user_tags.data(), src.user_tags.size() * sizeof(uint64_t));
    ptr += src.user_tags.size() * sizeof(uint64_t);
    memcpy(ptr, &src.metadata, sizeof(CCLogMetaData));
}

inline void
DecodeCCLogEntry(std::string& src, CCLogEntry& dst)
{
    char* metadata_ptr = src.data() + src.size() - sizeof(CCLogMetaData);
    memcpy(&dst.metadata, metadata_ptr, sizeof(CCLogMetaData));
    CHECK(is_aligned<uint64_t>(metadata_ptr)) << "tags must be 8bytes-aligned";
    uint64_t* tags_ptr =
        reinterpret_cast<uint64_t*>(metadata_ptr) - dst.metadata.num_tags;
    dst.user_tags.assign(tags_ptr, tags_ptr + dst.metadata.num_tags);
    src.resize(src.size() - sizeof(CCLogMetaData) -
               dst.metadata.num_tags * sizeof(uint64_t));
    dst.data = std::move(src);
}

}} // namespace faas::log_utils
