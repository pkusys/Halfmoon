#include "log/engine.h"

#include "absl/container/inlined_vector.h"
#include "absl/synchronization/mutex.h"
#include "base/logging.h"
#include "base/std_span.h"
#include "common/protocol.h"
#include "engine/engine.h"
#include "fmt/core.h"
#include "gsl/gsl_util"
#include "log/common.h"
#include "log/engine_base.h"
#include "log/flags.h"
#include "log/index.h"
#include "log/log_space.h"
#include "log/utils.h"
#include "log/view.h"
#include "proto/shared_log.pb.h"
#include "utils/bits.h"
#include "utils/random.h"
#include "zookeeper/zookeeper.jute.h"
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace faas { namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

TxnEngine::TxnEngine(const View* view, uint16_t engine_id, uint16_t sequencer_id)
    : view_(view),
      engine_id_(engine_id),
      next_localid_(bits::JoinTwo32(engine_id, 0))
{
    const View::NodeIdVec& engine_node_ids = view->GetEngineNodes();
    engine_idx_ = static_cast<int>(absl::c_find(engine_node_ids, engine_id) -
                                   engine_node_ids.begin());
    DCHECK_LT(static_cast<size_t>(engine_idx_), engine_node_ids.size());
    // const View::Engine* engine_node = view->GetEngineNode(engine_id);
    // replica_count_ = engine_node->GetStorageNodes().size();
    logspace_id_ = bits::JoinTwo16(view->id(), sequencer_id);
}

uint64_t
TxnEngine::RegisterOp(LocalOp* op)
{
    absl::MutexLock lk(&append_mu_);
    uint64_t localid = next_localid_++;
    pending_ops_[localid] = op;
    op->localid = localid;
    return localid;
}

void
TxnEngine::RegisterTxnCommitOp(LocalOp* op)
{
    absl::MutexLock lk(&append_mu_);
    pending_txns_[bits::LowHalf64(op->seqnum)].reset(new TxnProgress(op));
}

// std::optional<std::vector<MetaLogProto*>>
// TxnEngine::ProvideRawMetaLog(std::span<const char> payload)
// {
//     absl::MutexLock lk(&metalog_mu_);
//     auto metalog_proto = metalog_pool_.Get();
//     metalog_proto->ParseFromArray(payload.data(),
//     static_cast<int>(payload.size()));
//     pending_metalogs_[metalog_proto->metalog_seqnum()] = metalog_proto;
//     uint32_t metalog_seqnum = metalog_seqnum_;
//     while (pending_metalogs_.contains(metalog_seqnum)) {
//         metalog_seqnum++;
//     }
//     if (metalog_seqnum == metalog_seqnum_) {
//         return std::nullopt;
//     }
//     std::vector<MetaLogProto*> metalogs(metalog_seqnum - metalog_seqnum_);
//     for (uint32_t i = 0; i < metalogs.size(); i++) {
//         metalogs[i] = pending_metalogs_[metalog_seqnum_ + i];
//         pending_metalogs_.erase(metalog_seqnum_ + i);
//     }
//     metalog_seqnum_ = metalog_seqnum;
//     return metalogs;
// }

void
TxnEngine::ApplyMetaLogs(std::vector<MetaLogProto*>& metalog_protos)
{
    // size_t num_ops = 0;
    // for (MetaLogProto* metalog_proto: metalog_protos) {
    //     num_ops += metalog_proto->new_logs_proto().shard_deltas(engine_idx_);
    // }
    // finished_ops_.reserve(num_ops);
    // absl::MutexLock lk(&append_mu_);
    for (MetaLogProto* metalog_proto: metalog_protos) {
        DCHECK(metalog_proto->logspace_id() == logspace_id_);
        const auto& progress_vec = metalog_proto->new_logs_proto();
        uint64_t start_seqnum =
            bits::JoinTwo32(logspace_id_, progress_vec.start_seqnum());
        CHECK(progress_vec.shard_deltas_size() ==
              static_cast<int>(view_->num_engine_nodes()));
        for (int i = 0; i < progress_vec.shard_deltas_size(); i++) {
            if (i != engine_idx_) {
                start_seqnum += progress_vec.shard_deltas(i);
                continue;
            }
            uint64_t start_localid =
                bits::JoinTwo32(engine_id_, progress_vec.shard_starts(i));
            for (uint32_t j = 0; j < progress_vec.shard_deltas(i); j++) {
                FinishOp(start_localid + j, start_seqnum + j);
            }
        }
    }
    // finished_ops.swap(finished_ops_);
}

void
TxnEngine::FinishOp(uint64_t localid, uint64_t seqnum, bool check_cond)
{
    if (!pending_ops_.contains(localid)) {
        return;
    }
    LocalOp* op = pending_ops_.at(localid);
    if (check_cond && op->is_cond_op) {
        return;
    }
    pending_ops_.erase(localid);
    finished_ops_.push_back(op);
    // Note: async_write has already returned to caller, no need to call
    // FinishOpWithResponse
    if (op->type == protocol::SharedLogOpType::CC_TXN_WRITE) {
        auto& txn_progress = pending_txns_[bits::LowHalf64(op->seqnum)];
        LocalOp* commit_op = txn_progress->finishOp();
        if (commit_op != nullptr) {
            finished_ops_.push_back(commit_op);
        }
    } else {
        op->seqnum = seqnum;
    }
}

void
TxnEngine::FinishCondOps(CondOpResultVec& cond_op_results)
{
    // absl::MutexLock lk(&append_mu_);
    for (auto& [localid, seqnum]: cond_op_results) {
        FinishOp(localid, seqnum, false);
    }
}

void
TxnEngine::PollFinishedOps(FinishedOpVec& finished_ops)
{
    finished_ops.swap(finished_ops_);
}

// void
// TxnEngine::RecycleMetaLogs(std::vector<MetaLogProto*>& metalog_protos)
// {
//     absl::MutexLock lk(&metalog_mu_);
//     for (MetaLogProto* metalog_proto: metalog_protos) {
//         metalog_pool_.Return(metalog_proto);
//     }
// }

void
TxnEngine::ApplyIndices(std::vector<PerEngineIndexProto*>& engine_indices)
{
    for (auto engine_index: engine_indices) {
        // uint64_t start_seqnum =
        //     bits::JoinTwo32(logspace_id_, engine_index.start_seqnum());
        uint64_t start_localid = engine_index->start_localid();
        for (int i = 0; i < engine_index->log_indices_size(); i++) {
            auto& log_index = engine_index->log_indices(i);
            ApplyIndex(log_index, start_localid + static_cast<uint64_t>(i));
        }
    }
    // TODO: check pending reads
    auto iter = pending_queries_.begin();
    uint32_t indexed_seqnum = indexed_seqnum_.load();
    while (iter != pending_queries_.end()) {
        if (iter->first >= indexed_seqnum) {
            break;
        }
        LocalOp* op = iter->second;
        IndexInfo index_info = LookUpCurrentIndex(op);
        pending_query_results_.push_back(IndexQueryResult{index_info, op});
    }
}

void
TxnEngine::ApplyIndex(const PerLogIndexProto& log_index, uint64_t localid)
{
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
    uint32_t seqnum_lowhalf = indexed_seqnum_++;
    // txn write has no tags
    if (log_index.user_tags_size() == 0 && log_index.cond_tag() == kEmptyLogTag) {
        return;
    }
    if (log_index.cond_tag() != kEmptyLogTag) {
        size_t pos = seqnums_by_tag_[log_index.cond_tag()].size();
        bool cond_ok = pos == log_index.cond_pos();
        if (engine_id == engine_id_) {
            cond_op_results_.push_back(CondOpResult{
                .localid = localid,
                .seqnum = cond_ok ? bits::JoinTwo32(logspace_id_, seqnum_lowhalf) :
                                    kInvalidLogSeqNum,
            });
        }
        if (!cond_ok) {
            return;
        }
        seqnums_by_tag_[log_index.cond_tag()].push_back(seqnum_lowhalf);
    }
    for (uint64_t user_tag: log_index.user_tags()) {
        seqnums_by_tag_[user_tag].push_back(seqnum_lowhalf);
    }
    seqnum_info_[seqnum_lowhalf] = IndexInfo{
        .is_txn = log_index.is_txn(),
        .engine_id = engine_id,
        .localid_lowhalf = bits::LowHalf64(localid),
        .cond_tag = log_index.cond_tag(),
    };
}

void
TxnEngine::PollCondOpResults(CondOpResultVec& cond_op_results)
{
    cond_op_results.swap(cond_op_results_);
}

void
TxnEngine::PollIndexQueryResults(IndexQueryResultVec& query_results)
{
    query_results.swap(pending_query_results_);
}

// NOTE: txns and functions should read prev with snapshot-1
std::optional<TxnEngine::IndexInfo>
TxnEngine::LookUp(LocalOp* op)
{
    uint32_t indexed_seqnum_lowhalf = indexed_seqnum_.load();
    // currently not supporting snapshot and single obj read prev
    // snapshot(read-only txn) and single obj(non-txn) returns the current tail
    if (bits::JoinTwo32(logspace_id_, indexed_seqnum_lowhalf) <= op->seqnum) {
        absl::MutexLock lk(&index_mu_);
        return LookUpFutureIndex(op);
    }
    absl::ReaderMutexLock lk(&index_mu_);
    return LookUpCurrentIndex(op);
}

TxnEngine::IndexInfo
TxnEngine::LookUpCurrentIndex(LocalOp* op)
{
    // absl::ReaderMutexLock lk(&index_mu_);
    DCHECK(logspace_id_ == bits::HighHalf64(op->seqnum));
    // currently not supporting snapshot and single obj read
    DCHECK(op->query_tag != kEmptyLogTag);
    if (!seqnums_by_tag_.contains(op->query_tag)) {
        op->seqnum = kInvalidLogSeqNum;
        return IndexInfo{};
    }
    uint32_t query_seqnum_lowhalf = bits::LowHalf64(op->seqnum);
    auto& index_of_tag = seqnums_by_tag_.at(op->query_tag);
    auto iter = absl::c_upper_bound(
        index_of_tag,
        query_seqnum_lowhalf,
        [](uint32_t query_seqnum_lowhalf, uint32_t index_seqnum) {
            return query_seqnum_lowhalf < index_seqnum;
        });
    if (iter == index_of_tag.begin()) {
        op->seqnum = kInvalidLogSeqNum;
        return IndexInfo{};
    }
    uint32_t result_seqnum_lowhalf = *(--iter);
    op->seqnum = bits::JoinTwo32(logspace_id_, result_seqnum_lowhalf);
    return seqnum_info_[result_seqnum_lowhalf];
}

std::optional<TxnEngine::IndexInfo>
TxnEngine::LookUpFutureIndex(LocalOp* op)
{
    // absl::MutexLock lk(&index_mu_);
    DCHECK(logspace_id_ == bits::HighHalf64(op->seqnum));
    // currently not supporting snapshot and single obj read prev
    // snapshot(read-only txn) and single obj(non-txn) returns the current tail
    DCHECK(op->query_tag != kEmptyLogTag);
    uint32_t indexed_seqnum_lowhalf = indexed_seqnum_.load();
    uint32_t query_seqnum_lowhalf = bits::LowHalf64(op->seqnum);
    if (indexed_seqnum_lowhalf <= query_seqnum_lowhalf) {
        pending_queries_.insert(std::make_pair(query_seqnum_lowhalf, op));
        return std::nullopt;
    }
    return LookUpCurrentIndex(op);
}

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
                    // cc_index_collection_.InstallLogSpace(
                    //     std::make_unique<CCIndex>(view, sequencer_id));
                    txn_engine_.reset(
                        new TxnEngine(view, my_node_id(), sequencer_id));
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

// static Message
// BuildLocalCCReadCachedResponse(uint64_t seqnum, std::span<const char> aux_data)
// {
//     Message response =
//         MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::CACHED,
//         seqnum);
//     if (aux_data.size() > MESSAGE_INLINE_DATA_SIZE) {
//         LOG_F(FATAL, "Log data too large: size={}", aux_data.size());
//     }
//     MessageHelper::AppendInlineData(&response, aux_data);
//     return response;
// }

// static Message
// BuildLocalCCReadOkResponse(uint64_t seqnum, std::span<const char> log_data)
// {
//     Message response =
//         MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK,
//         seqnum);
//     if (log_data.size() > MESSAGE_INLINE_DATA_SIZE) {
//         LOG_F(FATAL, "Log data too large: size={}", log_data.size());
//     }
//     MessageHelper::AppendInlineData(&response, log_data);
//     return response;
// }

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
Engine::HandleLocalCCTxnCommit(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::CC_TXN_COMMIT);
    // HVLOG_F(1, "Handle local txn commit: txn_id={}", op->txn_id);
    if (!txn_engine_) {
        HLOG(FATAL) << "Txn engine not activated";
        return;
    }
    txn_engine_->RegisterTxnCommitOp(op);
}

void
Engine::HandleLocalCCTxnWrite(LocalOp* op)
{
    if (!txn_engine_) {
        HLOG(FATAL) << "Txn engine not activated";
        return;
    }
    uint64_t localid = txn_engine_->RegisterOp(op);
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::APPEND_OK);
    FinishLocalOpWithResponse(op, &response, false);
    HVLOG_F(1,
            "Handle local txn write: localid={:#x} seqnum={:#x} key={:#x}",
            localid,
            op->seqnum,
            op->query_tag);
    auto message = NewCCTxnWriteMessage(op);
    message.logspace_id = txn_engine_->logspace_id_;
    bool success = ReplicateCCLogEntry(txn_engine_->view_,
                                       message,
                                       VECTOR_AS_SPAN(op->user_tags),
                                       op->data.to_span());
    if (!success) {
        HLOG(FATAL) << "Failed to replicate txn write message";
        return;
        // FinishLocalOpWithFailure(op, SharedLogOpType::DISCARDED);
    }
    CCLogCachePut(op->seqnum, op->query_tag, op->data.to_span());
}

void
Engine::HandleLocalAppend(LocalOp* op)
{
    if (use_txn_engine_) {
        TxnEngineLocalAppend(op);
    } else {
        SLogLocalAppend(op);
    }
}

void
Engine::TxnEngineLocalAppend(LocalOp* op)
{
    if (!txn_engine_) {
        HLOG(FATAL) << "Txn engine not activated";
        return;
    }
    // HVLOG_F(1, "Handle local txn start: txn_id={:#x}", op->txn_localid);
    uint64_t localid = txn_engine_->RegisterOp(op);
    auto message = NewCCReplicateMessage(op);
    message.logspace_id = txn_engine_->logspace_id_;
    bool success = ReplicateCCLogEntry(txn_engine_->view_,
                                       message,
                                       VECTOR_AS_SPAN(op->user_tags),
                                       op->data.to_span());
    if (!success) {
        HLOG(FATAL) << "Failed to replicate txn start message";
    }
    CCLogCachePut(localid, op->data.to_span());
}

void
Engine::SLogLocalAppend(LocalOp* op)
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
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid);
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
Engine::HandleLocalRead(LocalOp* op)
{
    if (use_txn_engine_) {
        TxnEngineLocalRead(op);
    } else {
        SLogLocalRead(op);
    }
}

void
Engine::TxnEngineLocalRead(LocalOp* op)
{
    if (!txn_engine_) {
        HLOG(FATAL) << "Txn engine not activated";
        return;
    }
    HVLOG_F(1,
            "Handle local read: op_id={}, logspace={}, tag={}, seqnum={}",
            op->id,
            op->user_logspace,
            op->query_tag,
            bits::HexStr0x(op->seqnum));
    auto index_info = txn_engine_->LookUp(op);
    if (!index_info) {
        // index query blocks on future index data
        return;
    }
    ProcessIndexQueryResult(*index_info, op);
}

void
Engine::SLogLocalRead(LocalOp* op)
{
    DCHECK(op->type == SharedLogOpType::READ_NEXT ||
           op->type == SharedLogOpType::READ_PREV ||
           op->type == SharedLogOpType::READ_NEXT_B);
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
Engine::ProcessIndexQueryResult(TxnEngine::IndexInfo& index_info, LocalOp* op)
{
    if (op->seqnum == kInvalidLogSeqNum) {
        FinishLocalOpWithFailure(op, SharedLogResultType::EMPTY);
        return;
    }
    // TODO: process query_tag == kEmptyLogTag; currently not supported
    if (index_info.is_txn && op->query_tag != index_info.cond_tag) {
        ReadFromKVS(op);
    } else {
        op->localid =
            bits::JoinTwo32(index_info.engine_id, index_info.localid_lowhalf);
        ReadFromLog(op);
    }
}

void
Engine::ReadFromKVS(LocalOp* op)
{
    auto txn_write = CCLogCacheGet(op->seqnum, op->query_tag);
    if (txn_write) {
        Message response =
            MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK,
                                                   op->seqnum);
        MessageHelper::AppendInlineData(&response, STRING_AS_SPAN(*txn_write));
        FinishLocalOpWithResponse(op, &response);
        return;
    }
    // Cache miss
    if (pending_kvs_reads_.Put(std::make_pair(op->seqnum, op->query_tag), op)) {
        // read request already sent
        return;
    }
    auto request = NewCCReadKVSMessage(op);
    const View::Engine* engine_node =
        txn_engine_->view_->GetEngineNode(my_node_id());
    bool success = SendStorageCCReadRequest(request, engine_node);
    if (!success) {
        HLOG_F(FATAL,
               "Failed to send kvs read request for seqnum {:#x} tag {}",
               op->seqnum,
               op->query_tag);
        // FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
    }
}

void
Engine::ReadFromLog(LocalOp* op)
{
    auto log_data = CCLogCacheGet(op->localid);
    if (log_data) {
        Message response =
            MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK,
                                                   op->seqnum);
        MessageHelper::AppendInlineData(&response, STRING_AS_SPAN(*log_data));
        FinishLocalOpWithResponse(op, &response);
        return;
    }
    // Cache miss
    if (pending_log_reads_.Put(op->localid, op)) {
        // read request already sent
        return;
    }
    auto request = NewCCReadLogMessage(op);
    const View::Engine* engine_node =
        txn_engine_->view_->GetEngineNode(my_node_id());
    bool success = SendStorageCCReadRequest(request, engine_node);
    if (!success) {
        HLOG_F(FATAL,
               "Failed to send log read request for seqnum {:#x} localid {:#x}",
               op->seqnum,
               op->localid);
        // FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
    }
}

void
Engine::HandleLocalSetAuxData(LocalOp* op)
{
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
Engine::OnRecvNewMetaLog(const SharedLogMessage& message,
                         std::span<const char> payload)
{
    if (use_txn_engine_) {
        TxnEngineRecvMetaLog(message, payload);
    } else {
        SLogRecvMetaLog(message, payload);
    }
}

void
Engine::TxnEngineRecvMetaLog(const protocol::SharedLogMessage& message,
                             std::span<const char> payload)
{
    if (!txn_engine_) {
        HLOG(FATAL) << "Txn engine not activated";
        return;
    }
    std::optional<std::vector<MetaLogProto*>> metalogs;
    {
        absl::MutexLock lk(&txn_engine_->metalog_buffer_.buffer_mu_);
        txn_engine_->metalog_buffer_.ProvideRaw(payload);
        metalogs = txn_engine_->metalog_buffer_.PollBuffer();
    }
    if (!metalogs) {
        return;
    }
    // auto* metalog = txn_engine_->metalog_pool_.Get();
    // metalog.ParseFromArray(payload.data(), static_cast<int>(payload.size()));
    TxnEngine::FinishedOpVec finished_ops;
    {
        absl::MutexLock lk(&txn_engine_->append_mu_);
        txn_engine_->ApplyMetaLogs(*metalogs);
        txn_engine_->PollFinishedOps(finished_ops);
    }
    for (LocalOp* op: finished_ops) {
        ProcessFinishedOp(op);
    }
    // txn_engine_->metalog_pool_.Return(metalog);
    txn_engine_->metalog_buffer_.Recycle(*metalogs);
}

void
Engine::SLogRecvMetaLog(const protocol::SharedLogMessage& message,
                        std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOG);
    MetaLogProto metalog_proto = log_utils::MetaLogFromPayload(payload);
    DCHECK_EQ(metalog_proto.logspace_id(), message.logspace_id);
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
            // for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs())
            locked_producer->ProvideMetaLog(metalog_proto);
            locked_producer->PollAppendResults(&append_results);
        }
        if (current_view_->GetEngineNode(my_node_id())
                ->HasIndexFor(message.sequencer_id))
        {
            auto index_ptr =
                index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                // for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs())
                locked_index->ProvideMetaLog(metalog_proto);
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessAppendResults(append_results);
    ProcessIndexQueryResults(query_results);
}

void
Engine::ProcessFinishedOp(LocalOp* op)
{
    // if cond op failed, then seqnum is kInvalidLogSeqNum
    if (op->is_cond_op && op->seqnum == kInvalidLogSeqNum) {
        FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
        return;
    }
    if (op->type == SharedLogOpType::CC_TXN_WRITE) {
        RecycleLocalOp(op);
        return;
    }
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::APPEND_OK,
                                               op->seqnum);
    HVLOG(1) << fmt::format("op type={:#x} localid={:#x} seqnum={:#x} finished",
                            static_cast<uint16_t>(op->type),
                            op->localid,
                            op->seqnum);
    FinishLocalOpWithResponse(op, &response);
}

void
Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                           std::span<const char> payload)
{
    if (use_txn_engine_) {
        TxnEngineRecvNewIndexData(message, payload);
    } else {
        SLogRecvNewIndexData(message, payload);
    }
}

void
Engine::TxnEngineRecvNewIndexData(const protocol::SharedLogMessage& message,
                                  std::span<const char> payload)
{
    if (!txn_engine_) {
        HLOG(FATAL) << "Txn engine not activated";
        return;
    }
    auto storage_index = std::make_unique<PerStorageIndexProto>();
    storage_index->ParseFromArray(payload.data(), static_cast<int>(payload.size()));
    std::optional<std::vector<PerEngineIndexProto*>> engine_indices;
    {
        absl::MutexLock lk(&txn_engine_->index_buffer_.buffer_mu_);
        for (auto& engine_index: *storage_index->mutable_engine_indices()) {
            txn_engine_->index_buffer_.ProvideAllocated(&engine_index);
        }
        engine_indices = txn_engine_->index_buffer_.PollBuffer();
    }
    if (!engine_indices) {
        return;
    }
    // apply and poll cond ops and pending reads
    // NOTE: for append ops, applying index only finishes cond ops, while applying
    // metalog finishes none-cond ops though it is possible that index is ahead of
    // metalog, and non-cond ops can be finished here, we leave that completely to
    // metalog for clarity
    TxnEngine::CondOpResultVec cond_op_results;
    TxnEngine::IndexQueryResultVec pending_query_results;
    {
        absl::MutexLock lk(&txn_engine_->index_mu_);
        txn_engine_->ApplyIndices(*engine_indices);
        txn_engine_->PollCondOpResults(cond_op_results);
        txn_engine_->PollIndexQueryResults(pending_query_results);
    }
    txn_engine_->index_buffer_.Recycle(*engine_indices);
    // finish cond ops
    TxnEngine::FinishedOpVec finished_cond_ops;
    {
        absl::MutexLock lk(&txn_engine_->append_mu_);
        txn_engine_->FinishCondOps(cond_op_results);
        txn_engine_->PollFinishedOps(finished_cond_ops);
    }
    for (LocalOp* op: finished_cond_ops) {
        ProcessFinishedOp(op);
    }
    // process pending reads
    for (auto& [index_info, op]: pending_query_results) {
        ProcessIndexQueryResult(index_info, op);
    }
}

void
Engine::SLogRecvNewIndexData(const protocol::SharedLogMessage& message,
                             std::span<const char> payload)
{
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

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void
Engine::OnRecvResponse(const SharedLogMessage& message,
                       std::span<const char> payload)
{
    if (use_txn_engine_) {
        TxnEngineRecvResponse(message, payload);
    } else {
        SLogRecvResponse(message, payload);
    }
}

void
Engine::TxnEngineRecvResponse(const SharedLogMessage& message,
                              std::span<const char> payload)
{
    // only read responses here
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::CC_READ_KVS:
        ProcessKVSReadResult(message, payload);
        break;
    case SharedLogOpType::CC_READ_LOG:
        ProcessLogReadResult(message, payload);
        break;
    default:
        HLOG(FATAL) << "Unknown response op type: " << message.op_type;
    }
}

void
Engine::ProcessKVSReadResult(const protocol::SharedLogMessage& message,
                             std::span<const char> payload)
{
    auto result = SharedLogMessageHelper::GetResultType(message);
    if (result == SharedLogResultType::DATA_LOST) {
        HLOG(FATAL) << fmt::format("kvs read seqnum={:#x} tag={} data lost",
                                   message.query_seqnum,
                                   message.query_tag);
        return;
    }
    if (result != SharedLogResultType::READ_OK) {
        HLOG(FATAL) << "Unknown result op type: " << message.op_result;
        return;
    }
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK,
                                               message.query_seqnum);
    MessageHelper::AppendInlineData(&response, payload);
    std::vector<LocalOp*> finished_ops;
    pending_kvs_reads_.Poll(std::make_pair(message.query_seqnum, message.query_tag),
                            finished_ops);
    for (LocalOp* op: finished_ops) {
        FinishLocalOpWithResponse(op, &response);
    }
    CCLogCachePut(message.query_seqnum, message.query_tag, payload);
}

void
Engine::ProcessLogReadResult(const protocol::SharedLogMessage& message,
                             std::span<const char> payload)
{
    auto result = SharedLogMessageHelper::GetResultType(message);
    uint64_t query_seqnum =
        bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
    if (result == SharedLogResultType::DATA_LOST) {
        HLOG(FATAL) << fmt::format("log read seqnum={:#x} localid={:#x} data lost",
                                   query_seqnum,
                                   message.localid);
        return;
    }
    if (result != SharedLogResultType::READ_OK) {
        HLOG(FATAL) << "Unknown result op type: " << message.op_result;
        return;
    }
    Message response =
        MessageHelper::NewSharedLogOpSucceeded(SharedLogResultType::READ_OK,
                                               query_seqnum);
    MessageHelper::AppendInlineData(&response, payload);
    std::vector<LocalOp*> finished_ops;
    pending_log_reads_.Poll(message.localid, finished_ops);
    for (LocalOp* op: finished_ops) {
        FinishLocalOpWithResponse(op, &response);
    }
    CCLogCachePut(message.localid, payload);
}

void
Engine::SLogRecvResponse(const SharedLogMessage& message,
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
    } else {
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
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
