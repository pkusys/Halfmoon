#include "log/storage.h"

#include "absl/synchronization/mutex.h"
#include "base/std_span.h"
#include "common/protocol.h"
#include "fmt/core.h"
#include "gsl/gsl_util"
#include "log/common.h"
#include "log/flags.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
#include "utils/bits.h"
#include "utils/io.h"
#include "utils/timerfd.h"
#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

namespace faas { namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

template <class KeyType, class ValueType>
void
InMemStore<KeyType, ValueType>::PollItemsForPersistence(std::vector<ItemType>& items)
{
    if (live_keys_.size() <= persisted_offset_) {
        return;
    }
    items.reserve(live_keys_.size() - persisted_offset_);
    for (size_t i = persisted_offset_; i < live_keys_.size(); i++) {
        auto& key = live_keys_[i];
        items.emplace_back(key, store_.at(key));
    }
    return;
}

template <class KeyType, class ValueType>
void
InMemStore<KeyType, ValueType>::TrimPersistedItems()
{
    if (live_keys_.size() <= max_size) {
        return;
    }
    size_t num_to_trim = std::min(live_keys_.size() - max_size, persisted_offset_);
    for (size_t i = 0; i < num_to_trim; i++) {
        store_.erase(live_keys_[i]);
    }
    live_keys_.erase(live_keys_.begin(), live_keys_.begin() + num_to_trim);
    persisted_offset_ -= num_to_trim;
}

CCStorage::CCStorage(const View* view, uint16_t storage_id, uint16_t sequencer_id)
    : view_(view),
      storage_node_(view_->GetStorageNode(storage_id)),
      shard_progress_dirty_(false),
      log_store_(absl::GetFlag(FLAGS_slog_storage_max_live_entries)),
      kv_store_(absl::GetFlag(FLAGS_slog_storage_max_live_entries))
{
    for (uint16_t engine_id: storage_node_->GetSourceEngineNodes()) {
        shard_progresses_[engine_id] = 0;
    }
    logspace_id_ = bits::JoinTwo16(view->id(), sequencer_id);
}

void
CCStorage::ApplyMetaLogs(std::vector<MetaLogProto*>& metalog_protos)
{
    const auto& engine_node_ids = view_->GetEngineNodes();
    index_data_.Clear();
    index_data_.mutable_engine_indices()->Reserve(
        static_cast<int>(metalog_protos.size() * shard_progresses_.size()));
    for (MetaLogProto* metalog_proto: metalog_protos) {
        // VLOG_F(1,
        //        "Apply metalog: metalog_seqnum={}, start_seqnum={}",
        //        metalog_proto->metalog_seqnum(),
        //        start_seqnum);
        const auto& progress_vec = metalog_proto->new_logs_proto();
        uint32_t start_seqnum = progress_vec.start_seqnum();
        for (size_t i = 0; i < engine_node_ids.size(); i++) {
            uint16_t engine_id = engine_node_ids[i];
            uint32_t shard_start = progress_vec.shard_starts(static_cast<int>(i));
            uint32_t shard_delta = progress_vec.shard_deltas(static_cast<int>(i));
            if (shard_progresses_.contains(engine_id)) {
                ApplyMetaLogForEngine(start_seqnum,
                                      bits::JoinTwo32(engine_id, shard_start),
                                      shard_delta);
            }
            start_seqnum += shard_delta;
            indexed_seqnum_ += shard_delta;
        }
    }
}

void
CCStorage::ApplyMetaLogForEngine(uint32_t start_seqnum,
                                 uint64_t start_localid,
                                 uint32_t shard_delta)
{
    auto* engine_index = index_data_.mutable_engine_indices()->Add();
    engine_index->set_start_seqnum(start_seqnum);
    engine_index->set_start_localid(start_localid);
    engine_index->mutable_log_indices()->Reserve(static_cast<int>(shard_delta));
    for (uint32_t i = 0; i < shard_delta; i++) {
        auto* log_index = engine_index->mutable_log_indices()->Add();
        CHECK(log_index->user_tags_size() == 0 &&
              log_index->cond_tag() == kEmptyLogTag);
        // mark as live, become candidate for persistence
        uint64_t localid = start_localid + i;
        uint64_t seqnum = bits::JoinTwo32(logspace_id_, start_seqnum + i);
        auto log_entry = log_store_.Get(localid);
        if (!log_entry) {
            log_store_.Remove(localid);
            // NOTE: txn write still has a empty entry in the index
            continue;
        }
        // it is possible to shrink live entries here, but we leave that completely
        // to the background thread
        log_store_.OnItemsPersisted(localid);
        // populate log index
        log_index->set_is_txn(log_entry->metadata.op_type ==
                              static_cast<uint16_t>(SharedLogOpType::CC_TXN_START));
        log_index->set_cond_pos(log_entry->metadata.cond_pos);
        log_index->set_cond_tag(log_entry->metadata.cond_tag);
        log_index->mutable_user_tags()->Add(log_entry->user_tags.begin(),
                                            log_entry->user_tags.end());
        // install pending writes for CC_TXN_START
        if (log_entry->metadata.op_type ==
            static_cast<uint16_t>(SharedLogOpType::CC_TXN_START))
        {
            for (uint64_t key: log_entry->user_tags) {
                auto versioned_key = std::make_pair(seqnum, key);
                if (!blocking_kvs_reads_.contains(versioned_key)) {
                    blocking_kvs_reads_[versioned_key] = {};
                }
            }
        }
    }
}

std::optional<PerStorageIndexProto>
CCStorage::PollIndexData()
{
    if (index_data_.engine_indices_size() == 0) {
        return std::nullopt;
    }
    PerStorageIndexProto index_data;
    index_data.Swap(&index_data_);
    return index_data;
}

// void
// CCStorage::StoreLog(uint64_t localid, CCLogEntry* log_entry)
// {
//     uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
//     log_store_.PutAllocated(localid, log_entry);
//     AdvanceShardProgress(engine_id);
// }

void
CCStorage::StoreLog(uint64_t localid, std::shared_ptr<CCLogEntry> log_entry)
{
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
    log_store_.PutAllocated(localid, log_entry);
    AdvanceShardProgress(engine_id);
}

// void
// CCStorage::StoreKV(uint64_t seqnum, uint64_t key, std::string* value)
// {
//     kv_store_.PutAllocated(std::make_pair(seqnum, key), value);
// }

void
CCStorage::StoreKV(uint64_t seqnum, uint64_t key, std::shared_ptr<std::string> value)
{
    // kvs writes from txns are deterministic, no need to wait for replication,
    // immediately mark as live
    kv_store_.PutAllocated(std::make_pair(seqnum, key), value, true);
}

void
CCStorage::PollBlockingKVSReads(const VersionedKeyType& versioned_key,
                                std::vector<SharedLogMessage>& blocking_reads)
{
    if (!blocking_kvs_reads_.contains(versioned_key)) {
        LOG(FATAL) << fmt::format("KVS write seqnum={:#x} key={} not registered",
                                  versioned_key.first,
                                  versioned_key.second);
        return;
    }
    blocking_reads = std::move(blocking_kvs_reads_[versioned_key]);
    blocking_kvs_reads_.erase(versioned_key);
}

void
CCStorage::AdvanceShardProgress(uint16_t engine_id)
{
    uint32_t current = shard_progresses_[engine_id];
    while (log_store_.Contains(bits::JoinTwo32(engine_id, current))) {
        current++;
    }
    if (current > shard_progresses_[engine_id]) {
        VLOG_F(1,
               "Update shard progres for engine {}: from={}, to={}",
               engine_id,
               bits::HexStr0x(shard_progresses_[engine_id]),
               bits::HexStr0x(current));
        shard_progress_dirty_ = true;
        shard_progresses_[engine_id] = current;
    }
}

std::shared_ptr<CCLogEntry>
CCStorage::GetLog(const SharedLogMessage& request)
{
    absl::ReaderMutexLock lk(&store_mu_);
    return log_store_.Get(request.localid);
}

std::optional<std::shared_ptr<std::string>>
CCStorage::GetKV(const SharedLogMessage& request)
{
    absl::MutexLock lk(&store_mu_);
    auto versioned_key = std::make_pair(request.query_seqnum, request.query_tag);
    auto value = kv_store_.Get(versioned_key);
    if (value) {
        return value;
    }
    // 1st condition: txn not yet registered. txn writes may also arrive before
    // registration, but we assume that KV store is large enough that these writes
    // won't be flushed to DB before registration
    // 2nd condition: txn registered but not yet written
    if (request.query_seqnum >= indexed_seqnum_ ||
        blocking_kvs_reads_.contains(versioned_key))
    {
        blocking_kvs_reads_[versioned_key].emplace_back(request);
        return std::nullopt;
    }
    // lookup DB
    return nullptr;
}

void
CCStorage::PollShardProgress(std::vector<uint32_t>& progress)
{
    if (!shard_progress_dirty_) {
        return;
    }
    progress.reserve(shard_progresses_.size());
    for (uint16_t engine_id: storage_node_->GetSourceEngineNodes()) {
        progress.push_back(shard_progresses_[engine_id]);
    }
    // NOTE: must preserve order(sequencer use the order of SourceEngineNodes too)
    // the following is wrong, as the order of flat_hash_map iter may be different
    // for (auto& [engine_id, prog]: shard_progresses_) {
    //     progress.push_back(prog);
    // }
    shard_progress_dirty_ = false;
}

Storage::Storage(uint16_t node_id)
    : StorageBase(node_id),
      log_header_(fmt::format("Storage[{}-N]: ", node_id)),
      current_view_(nullptr),
      view_finalized_(false)
{}

Storage::~Storage() {}

void
Storage::OnViewCreated(const View* view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_storage_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            for (uint16_t sequencer_id: view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                storage_collection_.InstallLogSpace(
                    std::make_unique<LogStorage>(my_node_id(), view, sequencer_id));
                cc_storage_.reset(new CCStorage(view, my_node_id(), sequencer_id));
            }
        }
        future_requests_.OnNewView(view,
                                   contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        view_finalized_ = false;
        log_header_ = fmt::format("Storage[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr,
            [this, requests = std::move(ready_requests)]() {
                ProcessRequests(requests);
            });
    }
}

void
Storage::OnViewFinalized(const FinalizedView* finalized_view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    LogStorage::ReadResultVec results;
    std::vector<IndexDataProto> index_data_vec;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        storage_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [&, finalized_view](uint32_t logspace_id,
                                LockablePtr<LogStorage> storage_ptr) {
                log_utils::FinalizedLogSpace<LogStorage>(storage_ptr,
                                                         finalized_view);
                auto locked_storage = storage_ptr.Lock();
                LogStorage::ReadResultVec tmp;
                locked_storage->PollReadResults(&tmp);
                results.insert(results.end(), tmp.begin(), tmp.end());
                if (auto index_data = locked_storage->PollIndexData();
                    index_data.has_value())
                {
                    index_data_vec.push_back(std::move(*index_data));
                }
            });
        view_finalized_ = true;
    }
    if (!results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr,
            [this, results = std::move(results)] { ProcessReadResults(results); });
    }
    if (!index_data_vec.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr,
            [this,
             view = finalized_view->view(),
             index_data_vec = std::move(index_data_vec)] {
                for (const IndexDataProto& index_data: index_data_vec) {
                    SLogSendIndexData(view, index_data);
                }
            });
    }
}

#define ONHOLD_IF_FROM_FUTURE_VIEW(MESSAGE_VAR, PAYLOAD_VAR) \
    do {                                                     \
        if (current_view_ == nullptr ||                      \
            (MESSAGE_VAR).view_id > current_view_->id()) {   \
            future_requests_.OnHoldRequest(                  \
                (MESSAGE_VAR).view_id,                       \
                SharedLogRequest(MESSAGE_VAR, PAYLOAD_VAR)); \
            return;                                          \
        }                                                    \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)              \
    do {                                                   \
        if (current_view_ != nullptr &&                    \
            (MESSAGE_VAR).view_id < current_view_->id()) { \
            HLOG_F(WARNING,                                \
                   "Receive outdate request from view {}", \
                   (MESSAGE_VAR).view_id);                 \
            return;                                        \
        }                                                  \
    } while (0)

#define RETURN_IF_LOGSPACE_FINALIZED(LOGSPACE_PTR)               \
    do {                                                         \
        if ((LOGSPACE_PTR)->finalized()) {                       \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier(); \
            HLOG_F(WARNING,                                      \
                   "LogSpace {} is finalized",                   \
                   bits::HexStr0x(logspace_id));                 \
            return;                                              \
        }                                                        \
    } while (0)

void
Storage::HandleReadAtRequest(const SharedLogMessage& request)
{
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::READ_AT);
    LockablePtr<LogStorage> storage_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        storage_ptr = storage_collection_.GetLogSpace(request.logspace_id);
    }
    if (storage_ptr == nullptr) {
        ProcessReadFromDB(request);
        return;
    }
    LogStorage::ReadResultVec results;
    {
        auto locked_storage = storage_ptr.Lock();
        locked_storage->ReadAt(request);
        locked_storage->PollReadResults(&results);
    }
    ProcessReadResults(results);
}

// KVS reads can directly query the requested seqnum even if it is above indexed
// seqnum, like txn writes
// If the versioned key is present in InMemStore, it is immediately returned;
// otherwise it blocks on the versioned key as in the normal case.
// Correctness: (skip normal cases, only consider the case where the requested
// seqnum even if it is above indexed seqnum)
// 1. If txn write arrive after txn registration, then it must have seen the
// blocking read
// 2. Otherwise, either the read sees the write in InMemStore, or the write sees
// the blocking read
void
Storage::HandleCCReadKVSRequest(const SharedLogMessage& request)
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    auto optional = cc_storage_->GetKV(request);
    if (!optional) {
        // read blocks on future writes
        return;
    }
    auto response = request;
    auto value = optional->get();
    if (!value) {
        ProcessCCReadKVSFromDB(request, response);
    } else {
        response.op_result = static_cast<uint16_t>(SharedLogResultType::READ_OK);
        SendEngineResponse(request, &response, STRING_AS_SPAN(*value));
    }
}

void
Storage::HandleCCReadLogRequest(const SharedLogMessage& request)
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    auto response = request;
    auto log_entry = cc_storage_->GetLog(request);
    if (!log_entry) {
        ProcessCCReadLogFromDB(request, response);
    } else {
        response.op_result = static_cast<uint16_t>(SharedLogResultType::READ_OK);
        SendEngineResponse(request, &response, STRING_AS_SPAN(log_entry->data));
    }
}

// void
// Storage::OnRecvCCReadAtRequest(const protocol::SharedLogMessage& request)
// {
//     LockablePtr<LogStorage> storage_ptr;
//     {
//         absl::ReaderMutexLock view_lk(&view_mu_);
//         // ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
//         storage_ptr = storage_collection_.GetLogSpace(request.logspace_id);
//     }
//     if (storage_ptr == nullptr) {
//         ProcessCCReadFromDB(request);
//         return;
//     }
//     std::optional<std::shared_ptr<CCLogEntry>> cc_log_entry;
//     LogStorage::ReadResultVec results;
//     {
//         auto locked_storage = storage_ptr.Lock();
//         cc_log_entry = locked_storage->ReadCCLogEntry(request);
//         // locked_storage->PollReadResults(&results);
//     }
//     if (cc_log_entry.has_value()) {
//         auto response = SharedLogMessageHelper::NewReadOkResponse();
//         log_utils::FillResponseMsgWithCCLogEntry(&response, cc_log_entry->get());
//         response.user_metalog_progress = request.user_metalog_progress;
//         response.txn_id = request.txn_id;
//         SendEngineResponse(request,
//                            &response,
//                            STRING_AS_SPAN(cc_log_entry.value()->data));
//     } else {
//         ProcessCCReadFromDB(request);
//     }
// }

void
Storage::HandleReplicateRequest(const SharedLogMessage& message,
                                std::span<const char> payload)
{
    if (use_txn_engine_) {
        CCHandleReplicateRequest(message, payload);
    } else {
        SLogHandleReplicateRequest(message, payload);
    }
}

void
Storage::SLogHandleReplicateRequest(const SharedLogMessage& message,
                                    std::span<const char> payload)
{
    LogMetaData metadata = log_utils::GetMetaDataFromMessage(message);
    std::span<const uint64_t> user_tags;
    std::span<const char> log_data;
    log_utils::SplitPayloadForMessage(message,
                                      payload,
                                      &user_tags,
                                      &log_data,
                                      /* aux_data= */ nullptr);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto storage_ptr =
            storage_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_storage = storage_ptr.Lock();
            RETURN_IF_LOGSPACE_FINALIZED(locked_storage);
            if (!locked_storage->Store(metadata, user_tags, log_data)) {
                HLOG(ERROR) << "Failed to store log entry";
            }
        }
    }
}

void
Storage::CCHandleReplicateRequest(const protocol::SharedLogMessage& request,
                                  std::span<const char> payload)
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    auto log_entry = std::make_shared<CCLogEntry>();
    log_utils::PopulateCCLogEntry(request, payload, log_entry.get());
    // increment shard progress
    absl::MutexLock lk(&cc_storage_->store_mu_);
    cc_storage_->StoreLog(request.localid, log_entry);
    // install pending writes for CC_TXN_START upon recving metalog
}

// Txn writes can directly install at the requested seqnum
// even if it is above indexed seqnum, like kvs reads
// When registering future kvs writes of a txn, those that are already present in
// InMemStore can be skipped
// NOTE: we assume that the InMemStore capacity is large enough that the txn
// writes stored before txn registration won't be flushed by the time
// registration is finally done
void
Storage::HandleCCTxnWriteRequest(const protocol::SharedLogMessage& request,
                                 std::span<const char> payload)
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    DCHECK(request.logspace_id == cc_storage_->logspace_id_);
    uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
    auto write_data = std::make_shared<std::string>(payload.data(), payload.size());
    // put kv, increment shard progress, poll blocking reads
    std::vector<SharedLogMessage> blocking_reads;
    {
        absl::MutexLock lk(&cc_storage_->store_mu_);
        // null log entry is ok since only localid is relevant
        cc_storage_->StoreLog(request.localid, nullptr);
        cc_storage_->StoreKV(seqnum, request.query_tag, write_data);
        // poll blocking reads on this version of key
        cc_storage_->PollBlockingKVSReads(std::make_pair(seqnum, request.query_tag),
                                          blocking_reads);
    }
    if (blocking_reads.empty()) {
        return;
    }
    auto response = SharedLogMessageHelper::NewCCReadKVSResponse();
    response.query_seqnum = seqnum;
    response.query_tag = request.query_tag;
    for (const auto& read_request: blocking_reads) {
        SendEngineResponse(read_request, &response, STRING_AS_SPAN(*write_data));
    }
}

void
Storage::OnRecvNewMetaLog(const SharedLogMessage& message,
                          std::span<const char> payload)
{
    if (use_txn_engine_) {
        CCRecvMetaLog(message, payload);
    } else {
        SLogRecvMetaLog(message, payload);
    }
}

void
Storage::CCRecvMetaLog(const SharedLogMessage& message,
                       std::span<const char> payload)
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    std::optional<std::vector<MetaLogProto*>> metalogs;
    {
        absl::MutexLock lk(&cc_storage_->metalog_buffer_.buffer_mu_);
        cc_storage_->metalog_buffer_.ProvideRaw(payload);
        metalogs = cc_storage_->metalog_buffer_.PollBuffer();
    }
    if (!metalogs) {
        return;
    }
    std::optional<PerStorageIndexProto> index_data;
    {
        absl::MutexLock lk(&cc_storage_->store_mu_);
        cc_storage_->ApplyMetaLogs(*metalogs);
        index_data = cc_storage_->PollIndexData();
    }
    cc_storage_->metalog_buffer_.Recycle(*metalogs);
    if (index_data) {
        CCSendIndexData(cc_storage_->view_, cc_storage_->logspace_id_, *index_data);
    }
}

void
Storage::SLogRecvMetaLog(const SharedLogMessage& message,
                         std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOG);
    MetaLogProto metalog_proto = log_utils::MetaLogFromPayload(payload);
    DCHECK_EQ(metalog_proto.logspace_id(), message.logspace_id);
    const View* view = nullptr;
    LogStorage::ReadResultVec results;
    std::optional<IndexDataProto> index_data;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        view = current_view_;
        auto storage_ptr =
            storage_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_storage = storage_ptr.Lock();
            RETURN_IF_LOGSPACE_FINALIZED(locked_storage);
            // for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs())
            locked_storage->ProvideMetaLog(metalog_proto);
            locked_storage->PollReadResults(&results);
            index_data = locked_storage->PollIndexData();
        }
    }
    ProcessReadResults(results);
    if (index_data.has_value()) {
        SLogSendIndexData(DCHECK_NOTNULL(view), *index_data);
    }
}

void
Storage::OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                          std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) ==
           SharedLogOpType::SET_AUXDATA);
    uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
    LogCachePutAuxData(seqnum, payload);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef RETURN_IF_LOGSPACE_FINALIZED

void
Storage::ProcessReadResults(const LogStorage::ReadResultVec& results)
{
    for (const LogStorage::ReadResult& result: results) {
        const SharedLogMessage& request = result.original_request;
        SharedLogMessage response;
        switch (result.status) {
        case LogStorage::ReadResult::kOK:
            response = SharedLogMessageHelper::NewReadOkResponse();
            log_utils::PopulateMetaDataToMessage(result.log_entry->metadata,
                                                 &response);
            DCHECK_EQ(response.logspace_id, request.logspace_id);
            DCHECK_EQ(response.seqnum_lowhalf, request.seqnum_lowhalf);
            response.user_metalog_progress = request.user_metalog_progress;
            SendEngineLogResult(request,
                                &response,
                                VECTOR_AS_CHAR_SPAN(result.log_entry->user_tags),
                                STRING_AS_SPAN(result.log_entry->data));
            break;
        case LogStorage::ReadResult::kLookupDB:
            ProcessReadFromDB(request);
            break;
        case LogStorage::ReadResult::kFailed:
            HLOG_F(ERROR,
                   "Failed to read log data (seqnum={})",
                   bits::HexStr0x(bits::JoinTwo32(request.logspace_id,
                                                  request.seqnum_lowhalf)));
            response = SharedLogMessageHelper::NewDataLostResponse();
            SendEngineResponse(request, &response);
            break;
        default:
            UNREACHABLE();
        }
    }
}

void
Storage::ProcessReadFromDB(const SharedLogMessage& request)
{
    uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
    LogEntryProto log_entry;
    if (auto tmp = GetLogEntryFromDB(seqnum); tmp.has_value()) {
        log_entry = std::move(*tmp);
    } else {
        HLOG_F(ERROR, "Failed to read log data (seqnum={})", bits::HexStr0x(seqnum));
        SharedLogMessage response = SharedLogMessageHelper::NewDataLostResponse();
        SendEngineResponse(request, &response);
        return;
    }
    SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
    log_utils::PopulateMetaDataToMessage(log_entry, &response);
    DCHECK_EQ(response.logspace_id, request.logspace_id);
    DCHECK_EQ(response.seqnum_lowhalf, request.seqnum_lowhalf);
    response.user_metalog_progress = request.user_metalog_progress;
    std::span<const char> user_tags_data(
        reinterpret_cast<const char*>(log_entry.user_tags().data()),
        static_cast<size_t>(log_entry.user_tags().size()) * sizeof(uint64_t));
    SendEngineLogResult(request,
                        &response,
                        user_tags_data,
                        STRING_AS_SPAN(log_entry.data()));
}

void
Storage::ProcessCCReadLogFromDB(const SharedLogMessage& request,
                                SharedLogMessage& response)
{
    auto log_entry = GetCCLogEntryFromDB(request.logspace_id, request.localid);
    if (!log_entry) {
        HLOG(ERROR) << fmt::format(
            "CC log seqnum={:#x} localid={:#x} not found",
            bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf),
            request.localid);
        response.op_result = static_cast<uint16_t>(SharedLogResultType::DATA_LOST);
        SendEngineResponse(request, &response);
        return;
    }
    response.op_result = static_cast<uint16_t>(SharedLogResultType::READ_OK);
    SendEngineResponse(request, &response, STRING_AS_SPAN(log_entry->data));
}

void
Storage::ProcessCCReadKVSFromDB(const SharedLogMessage& request,
                                SharedLogMessage& response)
{
    auto value = GetKVFromDB(request.query_seqnum, request.query_tag);
    if (!value) {
        HLOG(ERROR) << fmt::format("CC KV seqnum={:#x} key={} not found",
                                   request.query_seqnum,
                                   request.query_tag);
        response.op_result = static_cast<uint16_t>(SharedLogResultType::DATA_LOST);
        SendEngineResponse(request, &response);
        return;
    }
    response.op_result = static_cast<uint16_t>(SharedLogResultType::READ_OK);
    SendEngineResponse(request, &response, STRING_AS_SPAN(*value));
}

void
Storage::ProcessRequests(const std::vector<SharedLogRequest>& requests)
{
    for (const SharedLogRequest& request: requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

void
Storage::SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             std::span<const char> tags_data,
                             std::span<const char> log_data)
{
    uint64_t seqnum =
        bits::JoinTwo32(response->logspace_id, response->seqnum_lowhalf);
    std::optional<std::string> cached_aux_data = LogCacheGetAuxData(seqnum);
    std::span<const char> aux_data;
    if (cached_aux_data.has_value()) {
        size_t full_size =
            log_data.size() + tags_data.size() + cached_aux_data->size();
        if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
            aux_data = STRING_AS_SPAN(*cached_aux_data);
        } else {
            HLOG_F(WARNING,
                   "Inline buffer of message not large enough "
                   "for auxiliary data of log (seqnum {}): "
                   "log_size={}, num_tags={} aux_data_size={}",
                   bits::HexStr0x(seqnum),
                   log_data.size(),
                   tags_data.size() / sizeof(uint64_t),
                   cached_aux_data->size());
        }
    }
    response->aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
    SendEngineResponse(request, response, tags_data, log_data, aux_data);
}

void
Storage::BackgroundThreadMain()
{
    int timerfd = io_utils::CreateTimerFd();
    CHECK(timerfd != -1) << "Failed to create timerfd";
    io_utils::FdUnsetNonblocking(timerfd);
    absl::Duration interval =
        absl::Milliseconds(absl::GetFlag(FLAGS_slog_storage_bgthread_interval_ms));
    CHECK(io_utils::SetupTimerFdPeriodic(timerfd, absl::Milliseconds(100), interval))
        << "Failed to setup timerfd with interval " << interval;
    bool running = true;
    while (running) {
        uint64_t exp;
        ssize_t nread = read(timerfd, &exp, sizeof(uint64_t));
        if (nread < 0) {
            PLOG(FATAL) << "Failed to read on timerfd";
        }
        CHECK_EQ(gsl::narrow_cast<size_t>(nread), sizeof(uint64_t));
        FlushLogEntries();
        // TODO: cleanup outdated LogSpace
        running = state_.load(std::memory_order_acquire) != kStopping;
    }
}

void
Storage::SendShardProgressIfNeeded()
{
    if (use_txn_engine_) {
        CCSendShardProgress();
    } else {
        SLogSendShardProgress();
    }
}

void
Storage::CCSendShardProgress()
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    uint32_t logspace_id;
    std::vector<uint32_t> progress;
    {
        absl::MutexLock lk(&cc_storage_->store_mu_);
        cc_storage_->PollShardProgress(progress);
        logspace_id = cc_storage_->logspace_id_;
    }
    SharedLogMessage message =
        SharedLogMessageHelper::NewShardProgressMessage(logspace_id);
    SendSequencerMessage(bits::LowHalf32(logspace_id),
                         &message,
                         VECTOR_AS_CHAR_SPAN(progress));
}

void
Storage::SLogSendShardProgress()
{
    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> progress_to_send;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_view_ == nullptr || view_finalized_) {
            return;
        }
        storage_collection_.ForEachActiveLogSpace(
            current_view_,
            [&progress_to_send](uint32_t logspace_id,
                                LockablePtr<LogStorage> storage_ptr) {
                auto locked_storage = storage_ptr.Lock();
                if (!locked_storage->frozen() && !locked_storage->finalized()) {
                    auto progress = locked_storage->GrabShardProgressForSending();
                    if (progress.has_value()) {
                        progress_to_send.emplace_back(logspace_id,
                                                      std::move(*progress));
                    }
                }
            });
    }
    for (const auto& entry: progress_to_send) {
        uint32_t logspace_id = entry.first;
        SharedLogMessage message =
            SharedLogMessageHelper::NewShardProgressMessage(logspace_id);
        SendSequencerMessage(bits::LowHalf32(logspace_id),
                             &message,
                             VECTOR_AS_CHAR_SPAN(entry.second));
    }
}

void
Storage::FlushLogEntries()
{
    if (use_txn_engine_) {
        CCFlushToDB();
    } else {
        SLogFlushToDB();
    }
}

void
Storage::CCFlushToDB()
{
    if (!cc_storage_) {
        HLOG(FATAL) << "CC storage not activated";
        return;
    }
    uint32_t logspace_id;
    std::vector<CCStorage::LogItemType> log_items;
    std::vector<CCStorage::KVItemType> kv_items;
    {
        absl::ReaderMutexLock lk(&cc_storage_->store_mu_);
        logspace_id = cc_storage_->logspace_id_;
        cc_storage_->log_store_.PollItemsForPersistence(log_items);
        cc_storage_->kv_store_.PollItemsForPersistence(kv_items);
    }
    if (log_items.empty() && kv_items.empty()) {
        return;
    }
    for (auto& [localid, log_entry]: log_items) {
        if (!log_entry) {
            // txn write
            continue;
        }
        PutCCLogEntryToDB(logspace_id, localid, *log_entry);
    }
    for (auto& [versioned_key, value]: kv_items) {
        auto& [seqnum, key] = versioned_key;
        if (!value) {
            LOG(FATAL) << fmt::format("null value at seqnum={:#x} key={}",
                                      seqnum,
                                      key);
        }
        PutKVToDB(seqnum, key, *value);
    }
    {
        absl::MutexLock lk(&cc_storage_->store_mu_);
        cc_storage_->log_store_.OnItemsPersisted(log_items.size());
        cc_storage_->kv_store_.OnItemsPersisted(kv_items.size());
    }
}

void
Storage::SLogFlushToDB()
{
    std::vector<std::shared_ptr<const LogEntry>> log_entries;
    std::vector<std::pair<LockablePtr<LogStorage>, uint64_t>> storages;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        storage_collection_.ForEachActiveLogSpace(
            [&log_entries, &storages](uint32_t logspace_id,
                                      LockablePtr<LogStorage> storage_ptr) {
                auto locked_storage = storage_ptr.ReaderLock();
                // std::vector<std::shared_ptr<const LogEntry>> tmp;
                uint64_t new_position = 0;
                locked_storage->GrabLogEntriesForPersistence(&log_entries,
                                                             &new_position);
                if (new_position > 0) {
                    storages.emplace_back(storage_ptr, new_position);
                }
            });
    }

    if (log_entries.empty()) {
        return;
    }
    HVLOG_F(1, "Will flush {} log entries", log_entries.size());
    for (size_t i = 0; i < log_entries.size(); i++) {
        PutLogEntryToDB(*log_entries[i]);
    }

    std::vector<uint32_t> finalized_logspaces;
    for (auto& [storage_ptr, new_position]: storages) {
        auto locked_storage = storage_ptr.Lock();
        locked_storage->LogEntriesPersisted(new_position);
        if (locked_storage->finalized() &&
            new_position >= locked_storage->seqnum_position())
        {
            finalized_logspaces.push_back(locked_storage->identifier());
        }
    }

    if (!finalized_logspaces.empty()) {
        absl::MutexLock view_lk(&view_mu_);
        for (uint32_t logspace_id: finalized_logspaces) {
            if (storage_collection_.FinalizeLogSpace(logspace_id)) {
                HLOG_F(INFO,
                       "Finalize storage log space {}",
                       bits::HexStr0x(logspace_id));
            } else {
                HLOG_F(ERROR,
                       "Storage log space {} not active, cannot finalize",
                       bits::HexStr0x(logspace_id));
            }
        }
    }
}

}} // namespace faas::log
