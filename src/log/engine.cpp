#include "log/engine.h"

#include "base/logging.h"
#include "base/std_span.h"
#include "common/protocol.h"
#include "engine/engine.h"
#include "fmt/core.h"
#include "log/common.h"
#include "log/engine_base.h"
#include "log/flags.h"
#include "log/index.h"
#include "log/log_space.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
#include "utils/bits.h"
#include "utils/random.h"
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>

namespace faas { namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr),
      current_view_active_(false)
{}

Engine::~Engine() {}

void
Engine::OnViewCreated(const View* view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_engine_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            const View::Engine* engine_node = view->GetEngineNode(my_node_id());
            for (uint16_t sequencer_id: view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                producer_collection_.InstallLogSpace(
                    std::make_unique<LogProducer>(my_node_id(), view, sequencer_id));
                if (engine_node->HasIndexFor(sequencer_id)) {
                    index_collection_.InstallLogSpace(
                        std::make_unique<Index>(view, sequencer_id));
                    cc_index_collection_.InstallLogSpace(
                        std::make_unique<CCIndex>(view, sequencer_id));
                }
            }
        }
        future_requests_.OnNewView(view,
                                   contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        if (contains_myself) {
            current_view_active_ = true;
        }
        views_.push_back(view);
        log_header_ = fmt::format("LogEngine[{}-{}]: ", my_node_id(), view->id());
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
Engine::OnViewFrozen(const View* view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} frozen", view->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(view->id(), current_view_->id());
    if (view->contains_engine_node(my_node_id())) {
        DCHECK(current_view_active_);
        current_view_active_ = false;
    }
}

void
Engine::OnViewFinalized(const FinalizedView* finalized_view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        producer_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view,
             &append_results](uint32_t logspace_id,
                              LockablePtr<LogProducer> producer_ptr) {
                log_utils::FinalizedLogSpace<LogProducer>(producer_ptr,
                                                          finalized_view);
                auto locked_producer = producer_ptr.Lock();
                LogProducer::AppendResultVec tmp;
                locked_producer->PollAppendResults(&tmp);
                append_results.insert(append_results.end(), tmp.begin(), tmp.end());
            });
        index_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &query_results](uint32_t logspace_id,
                                             LockablePtr<Index> index_ptr) {
                log_utils::FinalizedLogSpace<Index>(index_ptr, finalized_view);
                auto locked_index = index_ptr.Lock();
                locked_index->PollQueryResults(&query_results);
            });
    }
    if (!append_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr,
            [this, results = std::move(append_results)] {
                ProcessAppendResults(results);
            });
    }
    if (!query_results.empty()) {
        SomeIOWorker()->ScheduleFunction(nullptr,
                                         [this, results = std::move(query_results)] {
                                             ProcessIndexQueryResults(results);
                                         });
    }
}

namespace {
static Message
BuildLocalReadOKResponse(uint64_t seqnum,
                         std::span<const uint64_t> user_tags,
                         std::span<const char> log_data)
{
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK, seqnum);
    if (user_tags.size() * sizeof(uint64_t) + log_data.size() >
        MESSAGE_INLINE_DATA_SIZE)
    {
        LOG_F(FATAL,
              "Log data too large: num_tags={}, size={}",
              user_tags.size(),
              log_data.size());
    }
    response.log_num_tags = gsl::narrow_cast<uint16_t>(user_tags.size());
    MessageHelper::AppendInlineData(&response, user_tags);
    MessageHelper::AppendInlineData(&response, log_data);
    return response;
}

static Message
BuildLocalReadOKResponse(const LogEntry& log_entry)
{
    return BuildLocalReadOKResponse(log_entry.metadata.seqnum,
                                    VECTOR_AS_SPAN(log_entry.user_tags),
                                    STRING_AS_SPAN(log_entry.data));
}

static Message
BuildLocalCCReadCachedResponse(uint64_t seqnum, std::span<const char> aux_data)
{
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::CACHED, seqnum);
    if (aux_data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG_F(FATAL, "Log data too large: size={}", aux_data.size());
    }
    MessageHelper::AppendInlineData(&response, aux_data);
    return response;
}

static Message
BuildLocalCCReadOkResponse(uint64_t seqnum, std::span<const char> log_data)
{
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK, seqnum);
    if (log_data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG_F(FATAL, "Log data too large: size={}", log_data.size());
    }
    MessageHelper::AppendInlineData(&response, log_data);
    return response;
}

// static Message
// BuildLocalCCReadOkResponse(const CCLogEntry& cc_log_entry)
// {
//     return BuildLocalCCReadOkResponse(cc_log_entry.common_header.txn_localid,
//                                       STRING_AS_SPAN(cc_log_entry.data));
// }

} // namespace

// Start handlers for local requests (from functions)

#define ONHOLD_IF_SEEN_FUTURE_VIEW(LOCAL_OP_VAR)                                   \
    do {                                                                           \
        uint16_t view_id = log_utils::GetViewId((LOCAL_OP_VAR)->metalog_progress); \
        if (current_view_ == nullptr || view_id > current_view_->id()) {           \
            future_requests_.OnHoldRequest(view_id,                                \
                                           SharedLogRequest(LOCAL_OP_VAR));        \
            return;                                                                \
        }                                                                          \
    } while (0)

void
Engine::HandleLocalCCTxnStart(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::CC_TXN_START);
    const View* view = nullptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
            return;
        }
        view = current_view_;
        op->logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(op->logspace_id);
        {
            // TODO: separate this from log producer
            auto locked_producer = producer_ptr.Lock();
            locked_producer->RegisterTxnStart(op, &op->txn_localid);
        }
    }
    // HVLOG_F(1, "Handle local txn start: txn_id={:#x}", op->txn_localid);
    SharedLogMessage message =
        SharedLogMessageHelper::NewCCTxnStartMessage(op->txn_localid);
    message.logspace_id = op->logspace_id;
    message.user_logspace = op->user_logspace;
    uint16_t sequencer_id = bits::LowHalf32(op->logspace_id);
    bool success = SendSequencerMessage(sequencer_id, &message);
    if (!success) {
        HLOG(WARNING) << "Failed to send txn start message";
        FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
    }
}

void
Engine::HandleLocalCCTxnCommit(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::CC_TXN_COMMIT);
    // HVLOG_F(1, "Handle local txn commit: txn_id={}", op->txn_id);

    const View* view = nullptr;
    // // storage use txn_localid as index
    // // for commit, this is the same as txn_id, since we are only going to commit
    // once cc_metadata.localid = op->txn_id;
    // uint64_t txn_localid;
    // uint32_t logspace_id;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
            return;
        }
        view = current_view_;
        op->logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        const View::Engine* engine_node = view->GetEngineNode(my_node_id());
        uint32_t expected_replicas =
            static_cast<uint32_t>(engine_node->GetStorageNodes().size());
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(op->logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->RegisterCCOp(op, &op->txn_localid);
            locked_producer->SetExpectedReplicas(op->txn_localid, expected_replicas);
        }
    }
    // TODO: for commit, set localid to txn_id
    auto message = SharedLogMessageHelper::NewEmptySharedLogMessage();
    log_utils::FillReplicateMsgWithOp(message, op);
    ReplicateCCLogEntry(view, message, op->data.to_span());
    op->message.reset(message);
}

void
Engine::HandleLocalAppend(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::APPEND);
    HVLOG_F(1,
            "Handle local append: op_id={}, logspace={}, num_tags={}, size={}",
            op->id,
            op->user_logspace,
            op->user_tags.size(),
            op->data.length());

    const View* view = nullptr;
    LogMetaData log_metadata = MetaDataFromAppendOp(op);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
            return;
        }
        view = current_view_;
        uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        log_metadata.seqnum = bits::JoinTwo32(logspace_id, 0);
        // const View::Engine* engine_node = view->GetEngineNode(my_node_id());
        // uint32_t expected_replicas =
        //     static_cast<uint32_t>(engine_node->GetStorageNodes().size());
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid);
            // locked_producer->SetExpectedReplicas(log_metadata.localid,
            //                                      expected_replicas);
        }
    }
    ReplicateLogEntry(view,
                      log_metadata,
                      VECTOR_AS_SPAN(op->user_tags),
                      op->data.to_span());
}

void
Engine::HandleLocalTrim(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void
Engine::HandleCCLocalRead(LocalOp* op)
{
    HVLOG_F(1,
            "Handle local read: call_id={} tag={} seqnum={}",
            reinterpret_cast<const protocol::FuncCall*>(&op->func_call_id)->call_id,
            op->query_tag,
            op->seqnum);
    // if (op->seqnum != kMaxLogSeqNum) {
    //     // && sub_batch_idx!=invalid
    //     // in the coming fix, should also check sub_batch_idx
    //     // snapshot for ro txns goes to another handler
    //     auto cached_aux_data = CCLogCacheGetAuxData(op->seqnum, op->query_tag);
    //     if (cached_aux_data) {
    //         Message response =
    //             BuildLocalCCReadCachedResponse(op->seqnum,
    //                                            STRING_AS_SPAN(*cached_aux_data));
    //         FinishLocalOpWithResponse(op, &response, op->seqnum);
    //         return;
    //     }
    // }
    // onging_reads_.PutChecked(op->id, op);
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<CCIndex> cc_index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_SEEN_FUTURE_VIEW(op);
        uint32_t logspace_id = current_view_->LogSpaceIdentifier(op->user_logspace);
        op->logspace_id = logspace_id;
        sequencer_node =
            current_view_->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            cc_index_ptr = cc_index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }

    // IndexQuery query = BuildIndexQuery(op);
    // Index::QueryResultVec query_results;
    CCIndex::IndexEntry idx_entry;
    // TODO: use a vec here, when the queried global id(op.seqnum) is ahead of
    // global_position_, hold it in pending queries
    // this is only relevant for txns that run across engine nodes
    {
        auto locked_index = cc_index_ptr.ReaderLock();
        idx_entry = locked_index->Lookup(op->query_tag, op->seqnum);
        // locked_index->PollQueryResults(&query_results);
    }
    ProcessCCIndexLookupResult(op, idx_entry);
}

void
Engine::ProcessCCIndexLookupResult(LocalOp* op, CCIndex::IndexEntry& idx_entry)
{
    HVLOG(1) << fmt::format(
        "cc read call_id {} at tag {} seqnum {} returns at seqnum {} txn_id {:#x}",
        reinterpret_cast<const protocol::FuncCall*>(&op->func_call_id)->call_id,
        op->query_tag,
        op->seqnum,
        idx_entry.seqnum_,
        idx_entry.txn_id);
    if (idx_entry.txn_id == 0) {
        // not found
        FinishLocalOpWithFailure(op, SharedLogResultType::EMPTY);
        return;
    } else if (idx_entry.txn_id == protocol::kInvalidLogLocalId) {
        // FinishLocalOpWithFailure(op, SharedLogResultType::EMPTY,
        // idx_entry.seqnum_);
        Message response =
            BuildLocalCCReadOkResponse(idx_entry.seqnum_, EMPTY_CHAR_SPAN);
        FinishLocalOpWithResponse(op, &response, idx_entry.seqnum_);
        return;
    }
    if (auto cached_aux_data =
            CCLogCacheGetAuxData(idx_entry.seqnum_, op->query_tag);
        cached_aux_data.has_value())
    {
        HVLOG(1) << fmt::format("cc read op {} returns cached view at seqnum {}",
                                op->id,
                                idx_entry.seqnum_);
        Message response =
            BuildLocalCCReadCachedResponse(idx_entry.seqnum_,
                                           STRING_AS_SPAN(cached_aux_data.value()));
        FinishLocalOpWithResponse(op, &response, idx_entry.seqnum_);
        return;
    }
    if (auto cached_log_entry = CCLogCacheGet(idx_entry.txn_id);
        cached_log_entry.has_value())
    {
        // Cache hits
        // HVLOG_F(1,
        //         "CC Cache hits for log entry (txn_localid {:#x} batch_id {:#x})",
        //         idx_entry.txn_id,
        //         idx_entry.global_batch_position);
        const CCLogEntry& cc_log_entry = cached_log_entry.value();
        // LocalOp* op = onging_reads_.PollChecked(op->id);
        Message response =
            BuildLocalCCReadOkResponse(idx_entry.seqnum_,
                                       STRING_AS_SPAN(cc_log_entry.data));

        // std::optional<std::string> cached_aux_data =
        //     CCLogCacheGetAuxData(idx_entry.txn_id);
        // if (cached_aux_data.has_value()) {
        //     size_t full_size = cc_log_entry.data.size() + cached_aux_data->size();
        //     if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
        //         response.log_aux_data_size =
        //             gsl::narrow_cast<uint16_t>(cached_aux_data->size());
        //         MessageHelper::AppendInlineData(&response,
        //                                         STRING_AS_SPAN(*cached_aux_data));
        //     } else {
        //         HLOG_F(WARNING,
        //                "Inline buffer of message not large enough "
        //                "for auxiliary data of log (txn_localid {} batch_id {:#x}):
        //                " "log_size={}, aux_data_size={}",
        //                bits::HexStr0x(idx_entry.txn_id),
        //                idx_entry.global_batch_position,
        //                cc_log_entry.data.size(),
        //                cached_aux_data->size());
        //     }
        // }
        FinishLocalOpWithResponse(op, &response, idx_entry.seqnum_);
    } else {
        // Cache miss
        HVLOG_F(1, "CC Cache miss for op {} txn_id {:#x}", op->id, idx_entry.txn_id);
        onging_reads_.PutChecked(op->id, op);
        const View::Engine* engine_node = nullptr;
        {
            absl::ReaderMutexLock view_lk(&view_mu_);
            // uint16_t view_id = query_result.found_result.view_id;
            // if (view_id < views_.size()) {
            //     const View* view = views_.at(view_id);
            //     engine_node =
            //         view->GetEngineNode(query_result.found_result.engine_id);
            // } else {
            //     HLOG_F(FATAL, "Cannot find view {}", view_id);
            // }
            uint16_t engine_id = bits::LowHalf32(bits::HighHalf64(idx_entry.txn_id));
            engine_node = current_view_->GetEngineNode(engine_id);
        }
        bool success = SendStorageCCReadRequest(op, idx_entry, engine_node);
        if (!success) {
            HLOG_F(WARNING,
                   "Failed to send read request for op {} txn_id {:#x}",
                   op->id,
                   idx_entry.txn_id);
            onging_reads_.RemoveChecked(op->id);
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
            // if (local_request) {
            //     LocalOp* op = onging_reads_.PollChecked(query.client_data);
            //     FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
            // } else {
            //     SendReadFailureResponse(query, SharedLogResultType::DATA_LOST);
            // }
        }
    }
}

void
Engine::HandleLocalRead(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::READ_NEXT ||
           op->type == SharedLogOpType::READ_PREV ||
           op->type == SharedLogOpType::READ_NEXT_B);
    if (op->is_cc_op) {
        HandleCCLocalRead(op);
        return;
    }
    HVLOG_F(1,
            "Handle local read: op_id={}, logspace={}, tag={}, seqnum={}",
            op->id,
            op->user_logspace,
            op->query_tag,
            bits::HexStr0x(op->seqnum));
    onging_reads_.PutChecked(op->id, op);
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_SEEN_FUTURE_VIEW(op);
        uint32_t logspace_id = current_view_->LogSpaceIdentifier(op->user_logspace);
        sequencer_node =
            current_view_->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    bool use_local_index = true;
    if (absl::GetFlag(FLAGS_slog_engine_force_remote_index)) {
        use_local_index = false;
    }
    if (absl::GetFlag(FLAGS_slog_engine_prob_remote_index) > 0.0f) {
        float coin = utils::GetRandomFloat(0.0f, 1.0f);
        if (coin < absl::GetFlag(FLAGS_slog_engine_prob_remote_index)) {
            use_local_index = false;
        }
    }
    if (index_ptr != nullptr && use_local_index) {
        // Use local index
        IndexQuery query = BuildIndexQuery(op);
        Index::QueryResultVec query_results;
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
        ProcessIndexQueryResults(query_results);
    } else {
        HVLOG_F(1,
                "There is no local index for sequencer {}, "
                "will send request to remote engine node",
                DCHECK_NOTNULL(sequencer_node)->node_id());
        SharedLogMessage request = BuildReadRequestMessage(op);
        bool send_success =
            SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            onging_reads_.RemoveChecked(op->id);
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        }
    }
}

void
Engine::HandleLocalSetCCAuxData(LocalOp* op)
{
    uint64_t seqnum = op->seqnum;
    // LockablePtr<CCIndex> cc_index_ptr;
    // {
    //     absl::ReaderMutexLock view_lk(&view_mu_);
    //     ONHOLD_IF_SEEN_FUTURE_VIEW(op);
    //     uint32_t logspace_id =
    //     current_view_->LogSpaceIdentifier(op->user_logspace); op->logspace_id =
    //     logspace_id; auto sequencer_node =
    //         current_view_->GetSequencerNode(bits::LowHalf32(logspace_id));
    //     if (sequencer_node->IsIndexEngineNode(my_node_id())) {
    //         cc_index_ptr = cc_index_collection_.GetLogSpaceChecked(logspace_id);
    //     } else {
    //         HLOG_F(FATAL,
    //                "engine node {} does not index sequencer node {}",
    //                my_node_id(),
    //                sequencer_node->node_id());
    //     }
    // }
    // {
    //     auto locked_index = cc_index_ptr.Lock();
    //     VLOG(1) << fmt::format("mark aux for key {} batch {:#x}",
    //                            op->query_tag,
    //                            global_batch_id);
    //     locked_index->MarkSnapshot(op->query_tag, global_batch_id);
    //     // locked_index->PollQueryResults(&query_results);
    // }

    CCLogCachePutAuxData(seqnum, op->query_tag, op->data.to_span());
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::AUXDATA_OK,
                                               seqnum);
    FinishLocalOpWithResponse(op, &response, /* metalog_progress= */ 0);
    // if (!absl::GetFlag(FLAGS_slog_engine_propagate_auxdata)) {
    //     return;
    // }
}

void
Engine::HandleLocalSetAuxData(LocalOp* op)
{
    if (op->is_cc_op) {
        HandleLocalSetCCAuxData(op);
        return;
    }
    uint64_t seqnum = op->seqnum;
    LogCachePutAuxData(seqnum, op->data.to_span());
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::AUXDATA_OK,
                                               seqnum);
    FinishLocalOpWithResponse(op, &response, /* metalog_progress= */ 0);
    if (!absl::GetFlag(FLAGS_slog_engine_propagate_auxdata)) {
        return;
    }
    if (auto log_entry = LogCacheGet(seqnum); log_entry.has_value()) {
        if (auto aux_data = LogCacheGetAuxData(seqnum); aux_data.has_value()) {
            uint16_t view_id = log_utils::GetViewId(seqnum);
            absl::ReaderMutexLock view_lk(&view_mu_);
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                PropagateAuxData(view, log_entry->metadata, *aux_data);
            }
        }
    }
}

#undef ONHOLD_IF_SEEN_FUTURE_VIEW

// Start handlers for remote messages

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

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                     \
    do {                                                          \
        if (current_view_ != nullptr &&                           \
            (MESSAGE_VAR).view_id < current_view_->id()) {        \
            HLOG(WARNING) << "Receive outdate request from view " \
                          << (MESSAGE_VAR).view_id;               \
            return;                                               \
        }                                                         \
    } while (0)

void
Engine::HandleRemoteRead(const SharedLogMessage& request)
{
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(op_type == SharedLogOpType::READ_NEXT ||
           op_type == SharedLogOpType::READ_PREV ||
           op_type == SharedLogOpType::READ_NEXT_B);
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    IndexQuery query = BuildIndexQuery(request);
    Index::QueryResultVec query_results;
    {
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(&query_results);
    }
    ProcessIndexQueryResults(query_results);
}

void
Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                          std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto producer_ptr =
            producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs()) {
                locked_producer->ProvideMetaLog(metalog_proto);
            }
            locked_producer->PollAppendResults(&append_results);
        }
        if (current_view_->GetEngineNode(my_node_id())
                ->HasIndexFor(message.sequencer_id))
        {
            auto index_ptr =
                index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs()) {
                    locked_index->ProvideMetaLog(metalog_proto);
                }
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessAppendResults(append_results);
    ProcessIndexQueryResults(query_results);
}

void
Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                           std::span<const char> payload)
{
    HVLOG(1) << "recv index";
    DCHECK(SharedLogMessageHelper::GetOpType(message) ==
           SharedLogOpType::INDEX_DATA);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size())))
    {
        LOG(FATAL) << "Failed to parse IndexDataProto";
    }
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        const View* view = views_.at(message.view_id);
        if (!view->contains_engine_node(my_node_id())) {
            HLOG_F(FATAL, "View {} does not contain myself", view->id());
        }
        const View::Engine* engine_node = view->GetEngineNode(my_node_id());
        if (!engine_node->HasIndexFor(message.sequencer_id)) {
            HLOG_F(FATAL,
                   "This node is not index node for log space {}",
                   bits::HexStr0x(message.logspace_id));
        }
        auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            locked_index->ProvideIndexData(index_data_proto);
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

void
Engine::OnRecvCCGlobalBatch(const SharedLogMessage& message,
                            std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) ==
           SharedLogOpType::CC_GLOBAL_BATCH);
    CCGlobalBatchProto global_batch;
    if (!global_batch.ParseFromArray(payload.data(),
                                     static_cast<int>(payload.size())))
    {
        LOG(FATAL) << "Failed to parse CCGlobalBatchProto";
    }
    HVLOG(1) << fmt::format("recv global batch {}", global_batch.global_batch_id());
    LogProducer::TxnStartResultVec txn_start_results;
    LogProducer::TxnCommitBatchResult txn_commit_batch_result;
    uint64_t finished_snapshot;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        // ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        auto producer_ptr =
            producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            finished_snapshot = locked_producer->ProvideCCGlobalBatch(global_batch);
            // HVLOG(1) << "advance snapshot to " << finished_snapshot;
            // finished_snapshot_.store(finished_snapshot,
            // std::memory_order_relaxed);
            locked_producer->PollTxnStartResults(&txn_start_results);
            locked_producer->PollTxnCommitBatchResults(&txn_commit_batch_result);
        }
        if (current_view_->GetEngineNode(my_node_id())
                ->HasIndexFor(message.sequencer_id))
        {
            auto cc_index_ptr =
                cc_index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_cc_index = cc_index_ptr.Lock();
                // for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs())
                // {
                //     locked_index->ProvideMetaLog(metalog_proto);
                // }
                // locked_index->PollQueryResults(&query_results);
                locked_cc_index->ProvideCCGlobalBatch(global_batch,
                                                      finished_snapshot);
            }
        }
    }
    ProcessTxnStartResult(txn_start_results);
    ProcessTxnCommitResult(txn_commit_batch_result);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void
Engine::OnRecvResponse(const SharedLogMessage& message,
                       std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::RESPONSE);
    SharedLogResultType result = SharedLogMessageHelper::GetResultType(message);
    if (result == SharedLogResultType::READ_OK ||
        result == SharedLogResultType::EMPTY ||
        result == SharedLogResultType::DATA_LOST)
    {
        uint64_t op_id = message.client_data;
        LocalOp* op;
        if (!onging_reads_.Poll(op_id, &op)) {
            HLOG_F(FATAL, "Cannot find read op with id {}", op_id);
            return;
        }
        if (result == SharedLogResultType::READ_OK) {
            if (op->is_cc_op) {
                OnRecvCCReadResponse(op, message, payload);
                return;
            }
            uint64_t seqnum =
                bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
            HVLOG_F(1,
                    "Receive remote read response for log (seqnum {})",
                    bits::HexStr0x(seqnum));
            std::span<const uint64_t> user_tags;
            std::span<const char> log_data;
            std::span<const char> aux_data;
            log_utils::SplitPayloadForMessage(message,
                                              payload,
                                              &user_tags,
                                              &log_data,
                                              &aux_data);
            Message response = BuildLocalReadOKResponse(seqnum, user_tags, log_data);
            if (aux_data.size() > 0) {
                response.log_aux_data_size =
                    gsl::narrow_cast<uint16_t>(aux_data.size());
                MessageHelper::AppendInlineData(&response, aux_data);
            }
            FinishLocalOpWithResponse(op, &response, message.user_metalog_progress);
            // Put the received log entry into log cache
            LogMetaData log_metadata = log_utils::GetMetaDataFromMessage(message);
            LogCachePut(log_metadata, user_tags, log_data);
            if (aux_data.size() > 0) {
                LogCachePutAuxData(seqnum, aux_data);
            }
        } else if (result == SharedLogResultType::EMPTY) {
            FinishLocalOpWithFailure(op,
                                     SharedLogResultType::EMPTY,
                                     message.user_metalog_progress);
        } else if (result == SharedLogResultType::DATA_LOST) {
            HLOG_F(WARNING,
                   "Receive DATA_LOST response for read request: seqnum={}, tag={}",
                   bits::HexStr0x(op->seqnum),
                   op->query_tag);
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        } else {
            UNREACHABLE();
        }
    } else if (result == SharedLogResultType::REPLICATE_OK) {
        OnCCOpReplicated(message, payload);
    } else {
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
    }
}

void
Engine::OnRecvCCReadResponse(LocalOp* op,
                             const protocol::SharedLogMessage& message,
                             std::span<const char> payload)
{
    // HVLOG_F(1,
    //         "Receive remote read response for op {} txn_id {:#x}",
    //         op->id,
    //         message.txn_id);
    // NOTE: message.user_metalog_progress holds the queried seqnum
    if (payload.size() == 0) {
        HLOG(FATAL) << fmt::format(
            "Empty payload from storage {} for op {} txn_id {:#x}",
            message.origin_node_id,
            op->id,
            message.txn_id);
    }
    Message response =
        BuildLocalCCReadOkResponse(message.user_metalog_progress, payload);
    // if (aux_data.size() > 0) {
    //     response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
    //     MessageHelper::AppendInlineData(&response, aux_data);
    // }
    FinishLocalOpWithResponse(op, &response, message.user_metalog_progress);
    CCLogEntry cc_entry;
    // TODO: fix this, directly caching payload would be fine
    // NOTE: common header is of no use, message.txn_localid(union with client_data)
    // actually holds op_id, but we are not using it anyway
    log_utils::FillCCLogEntryWithReplicateMsg(&cc_entry, &message, payload);
    cc_entry.common_header.txn_localid = message.txn_id;
    CCLogCachePut(&cc_entry);
    // put aux data to cache?
}

void
Engine::OnCCOpReplicated(const protocol::SharedLogMessage& message,
                         std::span<const char> payload)
{
    LocalOp* op = nullptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        auto producer_ptr =
            producer_collection_.GetLogSpaceChecked(message.logspace_id);
        auto locked_producer = producer_ptr.Lock();
        // for txn commit, localid is the same as txn_id
        void* op_ptr = locked_producer->OnRecvReplicateOK(message.txn_localid);
        if (op_ptr != nullptr) {
            op = reinterpret_cast<LocalOp*>(op_ptr);
        } else {
            // HLOG_F(WARNING, "Cannot find append op with id {}",
            // message.txn_localid);
            return;
        }
    }
    HVLOG(1) << fmt::format("txn localid {:#x} op {:#x} replicated",
                            op->txn_localid,
                            static_cast<uint16_t>(op->type));
    auto msg = op->message.get();
    log_utils::ReorderMsgFromReplicateMsg(msg, op);

    uint16_t sequencer_id = bits::LowHalf32(message.logspace_id);
    bool success =
        SendSequencerMessage(sequencer_id,
                             msg,
                             op->data.to_span().subspan(0, msg->payload_size));
    if (!success) {
        HLOG_F(ERROR,
               "Failed to send reorder message to sequencer {}",
               sequencer_id);
        FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
        return;
    }
    // CCLogEntry cc_entry;
    // log_utils::FillCCLogEntryWithOp(&cc_entry, op);
    // CCLogCachePut(&cc_entry);
}

void
Engine::ProcessTxnStartResult(
    const LogProducer::TxnStartResultVec& txn_start_results)
{
    for (auto& result: txn_start_results) {
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        Message response =
            MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::APPEND_OK,
                                                   result.snapshot_seqnum);
        response.txn_id = op->txn_localid;
        HVLOG(1) << fmt::format("txn {:#x} started seqnum={}",
                                op->txn_localid,
                                result.snapshot_seqnum);
        FinishLocalOpWithResponse(op, &response, result.snapshot_seqnum);
    }
}

void
Engine::ProcessTxnCommitResult(
    const LogProducer::TxnCommitBatchResult& txn_commit_results)
{
    // auto& actual_results = global_batch.results();
    // CHECK(static_cast<size_t>(actual_results.size()) ==
    // txn_commit_results.size());
    HVLOG(1) << fmt::format("processing {} commit results",
                            txn_commit_results.size());
    for (auto& result: txn_commit_results) {
        // auto& actual_result = actual_results[static_cast<int>(i)];
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        if (result.seqnum == kInvalidLogSeqNum) {
            FinishLocalOpWithFailure(op, SharedLogResultType::ABORTED);
            HVLOG(1) << fmt::format("txn {:#x} aborted", op->txn_localid);
            continue;
        }

        Message response =
            MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::APPEND_OK,
                                                   result.seqnum);
        HVLOG(1) << fmt::format("txn {:#x} committed at seqnum {}",
                                op->txn_localid,
                                result.seqnum);
        CCLogEntry cc_entry;
        log_utils::FillCCLogEntryWithOp(&cc_entry, op);
        CCLogCachePut(&cc_entry);
        FinishLocalOpWithResponse(op, &response, result.seqnum);
    }
}

void
Engine::ProcessAppendResults(const LogProducer::AppendResultVec& results)
{
    for (const LogProducer::AppendResult& result: results) {
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        if (result.seqnum != kInvalidLogSeqNum) {
            LogMetaData log_metadata = MetaDataFromAppendOp(op);
            log_metadata.seqnum = result.seqnum;
            log_metadata.localid = result.localid;
            LogCachePut(log_metadata,
                        VECTOR_AS_SPAN(op->user_tags),
                        op->data.to_span());
            Message response = MessageHelper::NewSharedLogOpSucceeded(
                SharedLogResultType::APPEND_OK,
                result.seqnum);
            HVLOG(1) << fmt::format("append succeed localid={:#x} seqnum={:#x}",
                                    result.localid,
                                    result.seqnum);
            FinishLocalOpWithResponse(op, &response, result.metalog_progress);
        } else {
            HVLOG(1) << fmt::format("append failed localid={:#x}", result.localid);
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
        }
    }
}

void
Engine::ProcessIndexFoundResult(const IndexQueryResult& query_result)
{
    DCHECK(query_result.state == IndexQueryResult::kFound);
    const IndexQuery& query = query_result.original_query;
    bool local_request = (query.origin_node_id == my_node_id());
    uint64_t seqnum = query_result.found_result.seqnum;
    if (auto cached_log_entry = LogCacheGet(seqnum); cached_log_entry.has_value()) {
        // Cache hits
        HVLOG_F(1, "Cache hits for log entry (seqnum {})", bits::HexStr0x(seqnum));
        const LogEntry& log_entry = cached_log_entry.value();
        std::optional<std::string> cached_aux_data = LogCacheGetAuxData(seqnum);
        std::span<const char> aux_data;
        if (cached_aux_data.has_value()) {
            size_t full_size = log_entry.data.size() +
                               log_entry.user_tags.size() * sizeof(uint64_t) +
                               cached_aux_data->size();
            if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
                aux_data = STRING_AS_SPAN(*cached_aux_data);
            } else {
                HLOG_F(WARNING,
                       "Inline buffer of message not large enough "
                       "for auxiliary data of log (seqnum {}): "
                       "log_size={}, num_tags={} aux_data_size={}",
                       bits::HexStr0x(seqnum),
                       log_entry.data.size(),
                       log_entry.user_tags.size(),
                       cached_aux_data->size());
            }
        }
        if (local_request) {
            LocalOp* op = onging_reads_.PollChecked(query.client_data);
            Message response = BuildLocalReadOKResponse(log_entry);
            response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            MessageHelper::AppendInlineData(&response, aux_data);
            FinishLocalOpWithResponse(op, &response, query_result.metalog_progress);
        } else {
            HVLOG_F(1,
                    "Send read response for log (seqnum {})",
                    bits::HexStr0x(seqnum));
            SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
            log_utils::PopulateMetaDataToMessage(log_entry.metadata, &response);
            response.user_metalog_progress = query_result.metalog_progress;
            response.aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            SendReadResponse(query,
                             &response,
                             VECTOR_AS_CHAR_SPAN(log_entry.user_tags),
                             STRING_AS_SPAN(log_entry.data),
                             aux_data);
        }
    } else {
        // Cache miss
        const View::Engine* engine_node = nullptr;
        {
            absl::ReaderMutexLock view_lk(&view_mu_);
            uint16_t view_id = query_result.found_result.view_id;
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                engine_node =
                    view->GetEngineNode(query_result.found_result.engine_id);
            } else {
                HLOG_F(FATAL, "Cannot find view {}", view_id);
            }
        }
        bool success = SendStorageReadRequest(query_result, engine_node);
        if (!success) {
            HLOG_F(WARNING,
                   "Failed to send read request for seqnum {} ",
                   bits::HexStr0x(seqnum));
            if (local_request) {
                LocalOp* op = onging_reads_.PollChecked(query.client_data);
                FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
            } else {
                SendReadFailureResponse(query, SharedLogResultType::DATA_LOST);
            }
        }
    }
}

void
Engine::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                   Index::QueryResultVec* more_results)
{
    DCHECK(query_result.state == IndexQueryResult::kContinue);
    HVLOG_F(1,
            "Process IndexContinueResult: next_view_id={}",
            query_result.next_view_id);
    const IndexQuery& query = query_result.original_query;
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.next_view_id;
        if (view_id >= views_.size()) {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
        const View* view = views_.at(view_id);
        uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        sequencer_node = view->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    if (index_ptr != nullptr) {
        HVLOG(1) << "Use local index";
        IndexQuery query = BuildIndexQuery(query_result);
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(more_results);
    } else {
        HVLOG(1) << "Send to remote index";
        SharedLogMessage request = BuildReadRequestMessage(query_result);
        bool send_success =
            SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            uint32_t logspace_id = bits::JoinTwo16(sequencer_node->view()->id(),
                                                   sequencer_node->node_id());
            HLOG_F(ERROR,
                   "Failed to send read index request for logspace {}",
                   bits::HexStr0x(logspace_id));
        }
    }
}

void
Engine::ProcessIndexQueryResults(const Index::QueryResultVec& results)
{
    Index::QueryResultVec more_results;
    for (const IndexQueryResult& result: results) {
        const IndexQuery& query = result.original_query;
        switch (result.state) {
        case IndexQueryResult::kFound:
            ProcessIndexFoundResult(result);
            break;
        case IndexQueryResult::kEmpty:
            if (query.origin_node_id == my_node_id()) {
                FinishLocalOpWithFailure(
                    onging_reads_.PollChecked(query.client_data),
                    SharedLogResultType::EMPTY,
                    result.metalog_progress);
            } else {
                SendReadFailureResponse(query,
                                        SharedLogResultType::EMPTY,
                                        result.metalog_progress);
            }
            break;
        case IndexQueryResult::kContinue:
            ProcessIndexContinueResult(result, &more_results);
            break;
        default:
            UNREACHABLE();
        }
    }
    if (!more_results.empty()) {
        ProcessIndexQueryResults(more_results);
    }
}

void
Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests)
{
    for (const SharedLogRequest& request: requests) {
        if (request.local_op == nullptr) {
            MessageHandler(request.message, STRING_AS_SPAN(request.payload));
        } else {
            LocalOpHandler(reinterpret_cast<LocalOp*>(request.local_op));
        }
    }
}

SharedLogMessage
Engine::BuildReadRequestMessage(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::READ_NEXT ||
           op->type == SharedLogOpType::READ_PREV ||
           op->type == SharedLogOpType::READ_NEXT_B);
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(op->type);
    request.origin_node_id = my_node_id();
    request.hop_times = 1;
    request.client_data = op->id;
    request.user_logspace = op->user_logspace;
    request.query_tag = op->query_tag;
    request.query_seqnum = op->seqnum;
    request.user_metalog_progress = op->metalog_progress;
    request.flags |= protocol::kReadInitialFlag;
    request.prev_view_id = 0;
    request.prev_engine_id = 0;
    request.prev_found_seqnum = kInvalidLogSeqNum;
    return request;
}

SharedLogMessage
Engine::BuildReadRequestMessage(const IndexQueryResult& result)
{
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    SharedLogMessage request =
        SharedLogMessageHelper::NewReadMessage(query.DirectionToOpType());
    request.origin_node_id = query.origin_node_id;
    request.hop_times = query.hop_times + 1;
    request.client_data = query.client_data;
    request.user_logspace = query.user_logspace;
    request.query_tag = query.user_tag;
    request.query_seqnum = query.query_seqnum;
    request.user_metalog_progress = result.metalog_progress;
    request.prev_view_id = result.found_result.view_id;
    request.prev_engine_id = result.found_result.engine_id;
    request.prev_found_seqnum = result.found_result.seqnum;
    return request;
}

IndexQuery
Engine::BuildIndexQuery(LocalOp* op)
{
    return IndexQuery{.direction = IndexQuery::DirectionFromOpType(op->type),
                      .origin_node_id = my_node_id(),
                      .hop_times = 0,
                      .initial = true,
                      .client_data = op->id,
                      .user_logspace = op->user_logspace,
                      .user_tag = op->query_tag,
                      .query_seqnum = op->seqnum,
                      .metalog_progress = op->metalog_progress,
                      .prev_found_result = {.view_id = 0,
                                            .engine_id = 0,
                                            .seqnum = kInvalidLogSeqNum}};
}

IndexQuery
Engine::BuildIndexQuery(const SharedLogMessage& message)
{
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    return IndexQuery{.direction = IndexQuery::DirectionFromOpType(op_type),
                      .origin_node_id = message.origin_node_id,
                      .hop_times = message.hop_times,
                      .initial = (message.flags | protocol::kReadInitialFlag) != 0,
                      .client_data = message.client_data,
                      .user_logspace = message.user_logspace,
                      .user_tag = message.query_tag,
                      .query_seqnum = message.query_seqnum,
                      .metalog_progress = message.user_metalog_progress,
                      .prev_found_result =
                          IndexFoundResult{.view_id = message.prev_view_id,
                                           .engine_id = message.prev_engine_id,
                                           .seqnum = message.prev_found_seqnum}};
}

IndexQuery
Engine::BuildIndexQuery(const IndexQueryResult& result)
{
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    query.initial = false;
    query.metalog_progress = result.metalog_progress;
    query.prev_found_result = result.found_result;
    return query;
}

}} // namespace faas::log
