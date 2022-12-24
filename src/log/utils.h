#pragma once

#include "absl/synchronization/mutex.h"
#include "base/logging.h"
#include "log/common.h"
#include "log/engine_base.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "proto/shared_log.pb.h"
#include "utils/lockable_ptr.h"
#include <cstdint>
#include <sys/types.h>

namespace faas { namespace log_utils {

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

log::MetaLogsProto MetaLogsFromPayload(std::span<const char> payload);

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

// log::CCMetaData GetCCMetaDataFromMessage(const protocol::SharedLogMessage&
// message); void PopulateCCMetaDataToMessage(const log::CCMetaData& cc_metadata,
//                                  protocol::SharedLogMessage* message);

template <class T>
inline bool
is_aligned(const void* ptr) noexcept
{
    auto iptr = reinterpret_cast<std::uintptr_t>(ptr);
    return !(iptr % alignof(T));
}

void FillReplicateMsgWithOp(protocol::SharedLogMessage* message, log::LocalOp* op);
void ReorderMsgFromReplicateMsg(protocol::SharedLogMessage* message,
                                log::LocalOp* op);

// void FillCCLogEntryWithOp(log::CCLogEntry* cc_entry, log::LocalOp* op);
// void FillCCLogEntryWithReplicateMsg(log::CCLogEntry* cc_entry,
//                                     protocol::SharedLogMessage* message,
//                                     std::span<const char> payload);

inline void
FillCCLogEntryWithOp(log::CCLogEntry* cc_entry, log::LocalOp* op)
{
    auto header_ptr = &cc_entry->common_header;
    // header_ptr->cc_type = static_cast<uint16_t>(op->type);
    header_ptr->user_logspace = op->user_logspace;
    header_ptr->logspace_id = op->logspace_id;
    header_ptr->txn_localid = op->txn_localid;
    header_ptr->payload_size = op->data.length();
    cc_entry->data.assign(op->data.data(), op->data.length());
}

inline void
FillCCLogEntryWithReplicateMsg(log::CCLogEntry* cc_entry,
                               const protocol::SharedLogMessage* message,
                               std::span<const char> payload)
{
    auto header_ptr = &cc_entry->common_header;
    // header_ptr->cc_type = message->cc_type;
    header_ptr->user_logspace = message->user_logspace;
    header_ptr->logspace_id = message->logspace_id;
    header_ptr->txn_localid = message->txn_localid;
    header_ptr->payload_size = payload.size();
    cc_entry->data.assign(payload.data(), payload.size());
}

inline void
FillResponseMsgWithCCLogEntry(protocol::SharedLogMessage* message,
                              log::CCLogEntry* cc_entry)
{
    auto header_ptr = &cc_entry->common_header;
    // message->cc_type = header_ptr->cc_type;
    message->user_logspace = header_ptr->user_logspace;
    message->logspace_id = header_ptr->logspace_id;
    // no need to fill local id, fix replicate message to use txn_id as client_data
    // message->txn_localid = header_ptr->txn_localid;
}

inline void
EncodeCCLogEntry(std::string* encoded, log::CCLogEntry* cc_entry)
{
    size_t header_size = sizeof(cc_entry->common_header);
    size_t payload_size = cc_entry->data.size();
    CHECK(payload_size == cc_entry->common_header.payload_size);
    cc_entry->data.resize(header_size + payload_size);
    *encoded = std::move(cc_entry->data);
    auto ptr = encoded->data() + payload_size;
    memcpy(ptr, &cc_entry->common_header, header_size);
    CHECK(encoded->size() == header_size + payload_size);
}

inline void
DecodeCCLogEntry(log::CCLogEntry* cc_entry, std::string encoded)
{
    size_t header_size = sizeof(cc_entry->common_header);
    size_t payload_size = encoded.size() - header_size;
    auto ptr = encoded.data() + payload_size;
    // CHECK(is_aligned<log::CCCommonHeader>(ptr));
    memcpy(&cc_entry->common_header, ptr, header_size);
    CHECK(payload_size == cc_entry->common_header.payload_size);
    encoded.resize(payload_size);
    cc_entry->data = std::move(encoded);
    CHECK(cc_entry->data.size() == payload_size);
}

inline void
TxnCommitProtoFromReorderMsg(const protocol::SharedLogMessage& message,
                             std::span<const char> payload,
                             log::TxnCommitProto& commit_proto)
{
    // log::TxnCommitProto commit_proto;
    commit_proto.Clear();
    CHECK(is_aligned<protocol::TxnCommitHeader>(payload.data()));
    auto commit_header =
        reinterpret_cast<const protocol::TxnCommitHeader*>(payload.data());
    commit_proto.set_txn_id(commit_header->txn_id);
    commit_proto.set_start_seqnum(commit_header->seqnum);
    CHECK(is_aligned<uint64_t>(payload.data() + sizeof(protocol::TxnCommitHeader)));
    auto readset_ptr = reinterpret_cast<const uint64_t*>(
        payload.data() + sizeof(protocol::TxnCommitHeader));
    commit_proto.mutable_read_set()->Add(readset_ptr,
                                         readset_ptr + commit_header->read_set_size);
    auto writeset_ptr = readset_ptr + commit_header->read_set_size;
    commit_proto.mutable_write_set()->Add(writeset_ptr,
                                          writeset_ptr +
                                              commit_header->write_set_size);
    // return commit_proto;
}

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

}} // namespace faas::log_utils
