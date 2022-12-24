#include "log/sequencer.h"

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/flags/internal/flag.h"
#include "absl/synchronization/mutex.h"
#include "base/logging.h"
#include "base/macro.h"
#include "common/protocol.h"
#include "common/time.h"
#include "fmt/core.h"
#include "log/flags.h"
#include "log/utils.h"
#include "proto/shared_log.pb.h"
#include "server/io_worker.h"
#include "utils/bits.h"
#include "utils/object_pool.h"
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <utility>
#include <vector>

namespace faas { namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

void
ConflictGraph::AbortTxnByOffset(uint64_t offset)
{
    uint64_t txn_id = batch_for_reorder->at(offset)->txn_id();
    uint16_t engine_id = bits::LowHalf32(bits::HighHalf64(txn_id));
    auto engine_aborts = cc_reorder_engine_->pending_txn_aborts_[engine_id].get();
    // per_engine_aborts[engine_id].push_back(txn_id);
    {
        absl::MutexLock lk(&engine_aborts->mu_);
        engine_aborts->txn_ids.push_back(txn_id);
    }
    delete batch_for_reorder->at(offset);
    // txn_commit_proto_pool_->Return(batch_for_reorder[offset]);
}
// state view should be readlocked
void
ConflictGraph::PreValidate(const absl::flat_hash_map<uint64_t, uint64_t>& state_view)
{
    int num_aborts = 0;
    for (size_t i = 0; i < batch_for_reorder->size(); i++) {
        auto& txn_commit = batch_for_reorder->at(i);
        bool abort = false;
        for (uint64_t read_keys: txn_commit->read_set()) {
            if (state_view.contains(read_keys) &&
                state_view.at(read_keys) > txn_commit->start_seqnum())
            {
                abort = true;
                break;
            }
        }
        if (abort) {
            AbortTxnByOffset(i);
            num_aborts++;
        } else {
            active_txn_offsets.push_back(i);
        }
    }
    VLOG(1) << fmt::format("PreValidate: {} txns aborted", num_aborts);
}

void
ConflictGraph::BuildConflictGraph()
{
    for (size_t i = 0; i < batch_for_reorder->size(); i++) {
        active_txn_offsets.push_back(i);
    }
    for (uint64_t offset: active_txn_offsets) {
        auto& txn_commit = batch_for_reorder->at(offset);
        for (auto write_key: txn_commit->write_set()) {
            write_table[write_key].insert(offset);
        }
    }
    for (uint64_t offset: active_txn_offsets) {
        auto& txn_commit = batch_for_reorder->at(offset);
        for (auto read_key: txn_commit->read_set()) {
            if (write_table.contains(read_key)) {
                for (uint64_t write_offset: write_table.at(read_key)) {
                    if (write_offset == offset) {
                        continue;
                    }
                    txn_nodes[offset].out_edges.insert(write_offset);
                    txn_nodes[write_offset].in_edges.insert(offset);
                }
            }
        }
    }
}

void
ConflictGraph::Trim(absl::InlinedVector<uint64_t, 64>* head,
                    absl::InlinedVector<uint64_t, 64>* tail)
{
    // int i = 0, j = static_cast<int>(active_txn_offsets.size()) - 1;
    auto i = active_txn_offsets.begin();
    auto j = active_txn_offsets.end();
    while (i != j) {
        uint64_t offset = *i;
        auto& node = txn_nodes[offset];
        if (node.in_edges.size() > 0 && node.out_edges.size() > 0) {
            i++;
            continue;
        }
        j--;
        if (node.in_edges.size() == 0 && head != nullptr) {
            head->push_back(offset);
        } else if (node.out_edges.size() == 0 && tail != nullptr) {
            tail->push_back(offset);
        }
        std::swap(*i, *j);
    }
    active_txn_offsets.erase(j, active_txn_offsets.end());
}

void
ConflictGraph::RemoveNode(uint64_t offset,
                          absl::InlinedVector<uint64_t, 64>* head,
                          absl::InlinedVector<uint64_t, 64>* tail)
{
    CHECK(txn_nodes.contains(offset));
    auto& node = txn_nodes[offset];
    for (uint64_t out_offset: node.out_edges) {
        txn_nodes[out_offset].in_edges.erase(offset);
        if (txn_nodes[out_offset].in_edges.size() == 0 &&
            txn_nodes[out_offset].out_edges.size() > 0)
        {
            head->push_back(out_offset);
        }
    }
    for (uint64_t in_offset: node.in_edges) {
        txn_nodes[in_offset].out_edges.erase(offset);
        if (txn_nodes[in_offset].out_edges.size() == 0 &&
            txn_nodes[in_offset].in_edges.size() > 0)
        {
            tail->push_back(in_offset);
        }
    }
    txn_nodes.erase(offset);
}

void
ConflictGraph::Acyclic()
{
    // std::vector<uint64_t> head, tail;
    size_t num_aborts = 0, num_entries = active_txn_offsets.size();
    uint64_t head_pos = 0, tail_pos = 0;
    Trim(&head, &tail);
    while (true) {
        while (head_pos < head.size()) {
            RemoveNode(head[head_pos], &head, &tail);
            head_pos++;
        }
        while (tail_pos < tail.size()) {
            RemoveNode(tail[tail_pos], &head, &tail);
            tail_pos++;
        }
        Trim();
        if (active_txn_offsets.empty()) {
            break;
        }
        if (kQuickSelect > active_txn_offsets.size()) {
            kQuickSelect = 1;
        }
        size_t num_remaining = active_txn_offsets.size() - kQuickSelect;
        std::nth_element(
            active_txn_offsets.begin(),
            active_txn_offsets.begin() + num_remaining,
            active_txn_offsets.end(),
            [this](uint64_t a, uint64_t b) {
                return txn_nodes[a].in_edges.size() * txn_nodes[a].out_edges.size() <
                       txn_nodes[b].in_edges.size() * txn_nodes[b].out_edges.size();
            });
        for (size_t i = 0; i < kQuickSelect; i++) {
            uint64_t offset = active_txn_offsets[num_remaining + i];
            RemoveNode(offset, &head, &tail);
            AbortTxnByOffset(offset);
        }
        num_aborts += kQuickSelect;
        active_txn_offsets.erase(active_txn_offsets.begin() + num_remaining,
                                 active_txn_offsets.end());
    }
    CHECK(head.size() + tail.size() + num_aborts == num_entries);
    VLOG(1) << fmt::format("commit {} abort {} total {}",
                           num_entries - num_aborts,
                           num_aborts,
                           num_entries);
}

void
ConflictGraph::FillTxnCommitLocalBatch(TxnCommitLocalBatchProto* batch_proto)
{
    for (size_t i = 0; i < head.size(); i++) {
        // auto& txn_commit = *batch_for_reorder[tail[i]];
        // batch_proto->mutable_commit_order()->Add(std::move(txn_commit));
        auto txn_commit = batch_for_reorder->at(head[i]);
        // batch_proto->mutable_commit_order()->UnsafeArenaAddAllocated(txn_commit);
        batch_proto->mutable_commit_order()->Add(std::move(*txn_commit));
    }
    if (tail.empty()) {
        return;
    }
    for (auto iter = tail.rbegin(); iter != tail.rend(); iter++) {
        auto txn_commit = batch_for_reorder->at(*iter);
        // batch_proto->mutable_commit_order()->UnsafeArenaAddAllocated(txn_commit);
        batch_proto->mutable_commit_order()->Add(std::move(*txn_commit));
    }
    // Clear();
}

CCReorderEngine::CCReorderEngine(const View* view,
                                 Sequencer* sequencer,
                                 size_t num_io_workers)
    : sequencer_(sequencer),
      current_view_(view),
      num_io_workers_(num_io_workers)
{
    // per_worker_log_spaces_.resize(num_io_workers);
    // for (size_t i = 0; i < num_io_workers; i++) {
    //     per_worker_log_spaces_[i].reset(new PerWorkerLogSpace());
    // }
    logspace_id_ = bits::JoinTwo16(view->id(), sequencer->my_node_id());
    kBatchSize = absl::GetFlag(FLAGS_cc_reorder_batch_size);
    max_interval_ = absl::GetFlag(FLAGS_cc_reorder_max_interval_us);
    max_interval_rel_ = absl::GetFlag(FLAGS_cc_reorder_max_interval_rel);
    for (uint16_t engine_id: view->GetEngineNodes()) {
        pending_txn_starts_[engine_id].reset(new TxnStarts);
        // pending_txn_starts_[engine_id]->max_txn_id = 0;
        // last_txn_starts_[engine_id] = 0;
        pending_txn_aborts_[engine_id].reset(new TxnAborts);
    }
    kQuickSelect = absl::GetFlag(FLAGS_cc_reorder_quickselect);
    // for (size_t i = 0; i < num_io_workers; i++) {
    //     per_worker_log_spaces_[i]->txn_commit_pool.SetArena(&arena_);
    //     per_worker_log_spaces_[i]->local_batch_pool.SetArena(&arena_);
    //     // worker_space->global_batch =
    //     //     utils::Arena::CreateMessage<CCGlobalBatchProto>(&arena_);
    //     auto& conflict_graph = per_worker_log_spaces_[i]->conflict_graph;
    //     conflict_graph.kQuickSelect = kQuickSelect;
    //     conflict_graph.abort_mu_ = &abort_mu_;
    // }
    // for (auto& worker_space: per_worker_log_spaces_) {
    //     // worker_space.local_batch =
    //     //     utils::Arena::CreateMessage<TxnCommitLocalBatchProto>(&arena_);
    //     // worker_space->txn_commit_pool.SetArena(&worker_space->arena_);
    //     // worker_space->local_batch_pool.SetArena(&worker_space->arena_);
    //     // worker_space->global_batch =
    //     // utils::Arena::CreateMessage<CCGlobalBatchProto>(&worker_space->arena_);
    //     auto& conflict_graph = worker_space->conflict_graph;
    //     conflict_graph.kQuickSelect = kQuickSelect;
    //     // conflict_graph.abort_mu_ = &abort_mu_;
    //     conflict_graph.cc_reorder_engine_ = this;
    //     // conflict_graph.txn_commit_proto_pool_ = &worker_space->txn_commit_pool;
    //     // worker_space.conflict_graph.per_engine_aborts =
    //     //     worker_space.per_engine_aborts;
    // }
}

void
CCReorderEngine::ProcessLocalBatch(std::vector<TxnCommitProto*>& batch_for_reorder)
{
    std::vector<uint64_t> txn_ids;
    for (auto ptr: batch_for_reorder) {
        txn_ids.push_back(ptr->txn_id());
    }
    VLOG_F(1,
           "worker_id: {} collect local batch of size {}, txn_ids: {::#x}",
           MyWorkerId(),
           batch_for_reorder.size(),
           txn_ids);
    auto local_batch_ptr = DoReorder(&batch_for_reorder);
    // for now, there's a single sequencer, apply immediately
    // otherwise, should wait for global cut of controller(metasequencer)
    PerEngineVec<uint64_t> per_engine_aborts;
    PerEngineVec<CommitRecord> per_engine_commits;
    auto global_batch =
        ApplyLocalBatch(local_batch_ptr, per_engine_aborts, per_engine_commits);
    VLOG_F(1,
           "worker_id: {} propagate global batch {}",
           MyWorkerId(),
           global_batch.global_batch_id());
    std::string serialized_global_batch;
    auto response = SharedLogMessageHelper::NewCCGlobalBatchMessage(logspace_id_);
    response.origin_node_id = sequencer_->my_node_id();
    // a single sequencer sends to all engines
    for (uint16_t engine_id: current_view_->GetEngineNodes()) {
        FillGlobalBatchForEngine(&global_batch,
                                 engine_id,
                                 per_engine_commits[engine_id],
                                 per_engine_aborts[engine_id]);
        global_batch.SerializeToString(&serialized_global_batch);
        // send to engine
        response.payload_size =
            gsl::narrow_cast<uint32_t>(serialized_global_batch.size());
        bool success = sequencer_->SendSharedLogMessage(
            protocol::ConnType::SEQUENCER_TO_ENGINE,
            engine_id,
            response,
            STRING_AS_SPAN(serialized_global_batch));
        if (!success) {
            LOG_F(
                ERROR,
                "cc reorder engine {}-{} failed to send global batch {} to engine {}",
                sequencer_->my_node_id(),
                server::IOWorker::current()->worker_id(),
                global_batch.global_batch_id(),
                engine_id);
        }
    }
}

void
CCReorderEngine::OnReorderRequest(const protocol::SharedLogMessage& message,
                                  std::span<const char> payload)
{
    // auto txn_commit_proto = utils::Arena::CreateMessage<TxnCommitProto>(&arena_);
    // auto& my_space = MyWorkerSpace();
    // auto txn_commit_proto = my_space.txn_commit_pool.Get();
    auto txn_commit_proto = new TxnCommitProto();
    log_utils::TxnCommitProtoFromReorderMsg(message, payload, *txn_commit_proto);
    VLOG_F(1, "recv reorder request for txn {:#x}", txn_commit_proto->txn_id());
    std::vector<TxnCommitProto*> batch_for_reorder;
    bool batch_full = AppendTxnCommitProto(*txn_commit_proto, &batch_for_reorder);
    if (!batch_full) {
        return;
    }
    ProcessLocalBatch(batch_for_reorder);
}

// bool
// CCReorderEngine::AppendTxnCommitProto(TxnCommitProto& txn_commit_proto)
// {
//     auto& my_space = MyWorkerSpace();
//     // bool batch_full = false;
//     {
//         absl::MutexLock lk(&append_mu_);
//         // pending_requests.emplace_back(std::move(txn_commit_proto));
//         pending_requests.push_back(&txn_commit_proto);
//         if (pending_requests.size() >= kBatchSize) {
//             // batch_full = true;
//             CHECK(my_space.conflict_graph.batch_for_reorder.empty());
//             my_space.conflict_graph.batch_for_reorder.swap(pending_requests);
//             // pending_requests.clear();
//             pending_requests.reserve(kBatchSize);
//             // CHECK(pending_requests.capacity() >= 64);
//             return true;
//         }
//     }
//     return false;
// }

void
CCReorderEngine::CollectLocalBatch(std::vector<TxnCommitProto*>* batch_for_reorder,
                                   int64_t now)
{
    last_batch_time_ = now;
    batch_for_reorder->swap(pending_requests);
    pending_requests.reserve(kBatchSize);
}

bool
CCReorderEngine::AppendTxnCommitProto(
    TxnCommitProto& txn_commit_proto,
    std::vector<TxnCommitProto*>* batch_for_reorder)
{
    // auto& my_space = MyWorkerSpace();
    // bool batch_full = false;
    bool abort = false;
    {
        absl::ReaderMutexLock lk(&apply_mu_);
        for (uint64_t read_keys: txn_commit_proto.read_set()) {
            if (state_view.contains(read_keys) &&
                state_view.at(read_keys) > txn_commit_proto.start_seqnum())
            {
                abort = true;
                break;
            }
        }
    }
    if (abort) {
        uint64_t txn_id = txn_commit_proto.txn_id();
        VLOG(1) << fmt::format("pre-abort txn {:#x}", txn_id);
        uint16_t engine_id = bits::LowHalf32(bits::HighHalf64(txn_id));
        auto engine_aborts = pending_txn_aborts_[engine_id].get();
        // per_engine_aborts[engine_id].push_back(txn_id);
        {
            absl::MutexLock lk(&engine_aborts->mu_);
            engine_aborts->txn_ids.push_back(txn_id);
        }
        delete &txn_commit_proto;
        return false;
    }
    {
        absl::MutexLock lk(&append_mu_);
        // pending_requests.emplace_back(std::move(txn_commit_proto));
        pending_requests.push_back(&txn_commit_proto);
        int64_t now = GetMonotonicMicroTimestamp();
        if (pending_requests.size() >=
            kBatchSize /* || now - last_batch_time_ > max_interval_ */)
        {
            CollectLocalBatch(batch_for_reorder, now);
            return true;
        }
    }
    return false;
}

TxnCommitLocalBatchProto*
CCReorderEngine::DoReorder(std::vector<TxnCommitProto*>* batch_for_reorder)
{
    // auto& my_space = MyWorkerSpace();
    auto graph = GetConflictGraph();
    graph->batch_for_reorder = batch_for_reorder;
    // {
    //     absl::ReaderMutexLock lk(&apply_mu_);
    //     // need to validate again
    //     graph->PreValidate(state_view);
    // }
    VLOG(1) << fmt::format("worker_id: {} start reorder", MyWorkerId());
    graph->BuildConflictGraph();
    graph->Acyclic();
    // collect the result into a batch
    uint64_t batch_id = next_local_batch_id_.fetch_add(1, std::memory_order_relaxed);
    auto local_batch = local_batch_pool_.Get();
    // should release txn_commit_protos back to pool
    local_batch->Clear();
    local_batch->set_local_batch_id(batch_id);
    // TODO: fill in aborts
    graph->FillTxnCommitLocalBatch(local_batch);
    conflict_graph_pool_.Return(graph);
    return local_batch;
}

CCGlobalBatchProto
CCReorderEngine::ApplyLocalBatch(TxnCommitLocalBatchProto* batch_proto,
                                 PerEngineVec<uint64_t>& aborts,
                                 PerEngineVec<CommitRecord>& commits)
{
    // auto& my_space = MyWorkerSpace();
    for (auto iter = pending_txn_aborts_.begin(); iter != pending_txn_aborts_.end();
         iter++)
    {
        uint16_t engine_id = iter->first;
        auto& engine_aborts = aborts[engine_id];
        absl::MutexLock lk(&iter->second->mu_);
        engine_aborts.swap(iter->second->txn_ids);
    }
    uint64_t global_batch_id;
    uint64_t start_seqnum;
    absl::flat_hash_map<uint64_t, absl::InlinedVector<int, 4>> flat_index;
    {
        absl::MutexLock lk(&apply_mu_);
        global_batch_id = next_global_batch_id_++;
        start_seqnum = current_seqnum_;
        for (int i = 0; i < batch_proto->commit_order_size(); i++) {
            current_seqnum_++;
            auto& txn_commit_proto = batch_proto->commit_order(i);
            bool abort = false;
            for (uint64_t read_key: txn_commit_proto.read_set()) {
                if (state_view[read_key] > txn_commit_proto.start_seqnum()) {
                    abort = true;
                    break;
                }
            }
            uint64_t txn_id = txn_commit_proto.txn_id();
            uint16_t engine_id = bits::LowHalf32(bits::HighHalf64(txn_id));
            if (abort) {
                // just need a staging area here
                aborts[engine_id].push_back(txn_id);
                // absl::MutexLock lk(&pending_txn_aborts_[engine_id].mu_);
                // pending_txn_aborts_[engine_id].txn_ids.push_back(txn_id);
                // continue;
                // VLOG(1) << fmt::format("global batch {} abort txn {:#x}",
                //                        global_batch_id,
                //                        txn_id);
            } else {
                commits[engine_id].push_back({txn_id, static_cast<uint32_t>(i) + 1});
                // VLOG(1) << fmt::format("global batch {} commit txn {:#x}",
                //                        global_batch_id,
                //                        txn_id);
                for (uint64_t write_key: txn_commit_proto.write_set()) {
                    state_view[write_key] = current_seqnum_;
                    flat_index[write_key].push_back(i);
                }
            }
        }
        // current_seqnum_ +=
        // static_cast<uint64_t>(batch_proto->commit_order_size());
    }
    // for (int i = 0; i < batch_proto->commit_order_size(); i++) {
    //     auto txn_commit_proto =
    //     batch_proto->mutable_commit_order()->ReleaseLast();
    //     // batch_proto->mutable_commit_order()->UnsafeArenaReleaseLast();
    //     my_space.txn_commit_pool.Return(txn_commit_proto);
    // }
    // now per_engine_commits holds all the commits
    // but aborts may still be in other worker's conflict graph
    // acquire exclusive lock on all pending_aborts
    // return global_batch_id;
    // Fill in common part, engine specfic part handled by the following function
    // if (my_space.global_batch == nullptr) {
    //     my_space.global_batch =
    //         utils::Arena::CreateMessage<CCGlobalBatchProto>(&arena_);
    // }
    // auto& global_batch = my_space.global_batch;
    // global_batch.Clear();
    CCGlobalBatchProto global_batch;
    // if (!commits.empty()) {
    global_batch.set_global_batch_id(global_batch_id);
    global_batch.set_start_seqnum(start_seqnum);
    global_batch.set_end_seqnum(current_seqnum_);
    // global_batch.clear_batch_index();
    for (auto iter = flat_index.begin(); iter != flat_index.end(); ++iter) {
        auto idx = global_batch.add_batch_index();
        idx->set_key(iter->first);
        for (int i: flat_index[iter->first]) {
            // CHECK(batch_proto->commit_order(i).txn_id() != 0);
            idx->add_txn_ids(batch_proto->commit_order(i).txn_id());
            idx->add_offsets(static_cast<uint32_t>(i) + 1);
        }
    }
    // }
    pending_.store(false, std::memory_order_relaxed);
    // return local batch to pool, may not be the same worker
    local_batch_pool_.Return(batch_proto);
    return global_batch;
}

void
CCReorderEngine::FillGlobalBatchForEngine(
    CCGlobalBatchProto* batch_proto,
    uint16_t engine_id,
    absl::InlinedVector<CommitRecord, 16> commits,
    absl::InlinedVector<uint64_t, 16> aborts)
{
    // auto& my_space = MyWorkerSpace();
    batch_proto->clear_committed_txn_ids();
    batch_proto->clear_committed_offsets();
    batch_proto->clear_aborted_txn_ids();
    // batch_proto->clear_latest_txn_start();
    // for now, assume all engines are handled by the same single sequencer
    // for (uint16_t engine_id: current_view_->GetEngineNodes()) {
    for (auto& [txn_id, offset]: commits) {
        batch_proto->add_committed_txn_ids(txn_id);
        batch_proto->add_committed_offsets(offset);
    }
    if (!commits.empty()) {
        // uint16_t engine_id = bits::LowHalf32(bits::HighHalf64(commits[0]));
        VLOG(1) << fmt::format(
            "propagate global batch {} engine {} commit txns {::#x}",
            batch_proto->global_batch_id(),
            engine_id,
            batch_proto->committed_txn_ids());
    }
    for (uint64_t txn_id: aborts) {
        batch_proto->add_aborted_txn_ids(txn_id);
    }
    if (!aborts.empty()) {
        // uint16_t engine_id = bits::LowHalf32(bits::HighHalf64(aborts[0]));
        VLOG(1) << fmt::format(
            "propagate global batch {} engine {} abort txns {::#x}",
            batch_proto->global_batch_id(),
            engine_id,
            batch_proto->aborted_txn_ids());
    }
    auto& engine_txn_starts = pending_txn_starts_[engine_id];
    {
        absl::MutexLock lock(&engine_txn_starts->mu_);
        batch_proto->set_latest_txn_start(engine_txn_starts->max_txn_id);
    }
}

Sequencer::Sequencer(uint16_t node_id)
    : SequencerBase(node_id),
      log_header_(fmt::format("Sequencer[{}-N]: ", node_id)),
      current_view_(nullptr)
{}

Sequencer::~Sequencer() {}

void
Sequencer::OnViewCreated(const View* view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_sequencer_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            if (view->is_active_phylog(my_node_id())) {
                primary_collection_.InstallLogSpace(
                    std::make_unique<MetaLogPrimary>(view, my_node_id()));
                uint32_t identifier = bits::JoinTwo16(view->id(), my_node_id());
                cc_reorder_engine_collections_[identifier].reset(
                    new CCReorderEngine(view, this, my_node_id()));
            }
            for (uint16_t id: view->GetSequencerNodes()) {
                if (!view->is_active_phylog(id)) {
                    continue;
                }
                if (view->GetSequencerNode(id)->IsReplicaSequencerNode(my_node_id()))
                {
                    backup_collection_.InstallLogSpace(
                        std::make_unique<MetaLogBackup>(view, id));
                }
            }
        }
        current_primary_ = primary_collection_.GetLogSpace(
            bits::JoinTwo16(view->id(), my_node_id()));
        future_requests_.OnNewView(view,
                                   contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        log_header_ = fmt::format("Sequencer[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr,
            [this, requests = std::move(ready_requests)] {
                ProcessRequests(requests);
            });
    }
}

namespace {
template <class T>
void
FreezeLogSpace(LockablePtr<T> logspace_ptr, MetaLogsProto* tail_metalogs)
{
    auto locked_logspace = logspace_ptr.Lock();
    locked_logspace->Freeze();
    tail_metalogs->set_logspace_id(locked_logspace->identifier());
    uint32_t num_entries = gsl::narrow_cast<uint32_t>(
        absl::GetFlag(FLAGS_slog_num_tail_metalog_entries));
    uint32_t end_pos = locked_logspace->metalog_position();
    uint32_t start_pos = end_pos - std::min(num_entries, end_pos);
    for (uint32_t pos = start_pos; pos < end_pos; pos++) {
        auto metalog = locked_logspace->GetMetaLog(pos);
        CHECK(metalog.has_value());
        tail_metalogs->add_metalogs()->CopyFrom(*metalog);
    }
}
} // namespace

void
Sequencer::OnViewFrozen(const View* view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} frozen", view->id());
    FrozenSequencerProto frozen_proto;
    frozen_proto.set_view_id(view->id());
    frozen_proto.set_sequencer_id(my_node_id());
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(view->id(), current_view_->id());
        if (current_primary_ != nullptr) {
            FreezeLogSpace<MetaLogPrimary>(current_primary_,
                                           frozen_proto.add_tail_metalogs());
        }
        backup_collection_.ForEachActiveLogSpace(
            view,
            [&frozen_proto](uint32_t, LockablePtr<MetaLogBackup> logspace_ptr) {
                FreezeLogSpace<MetaLogBackup>(std::move(logspace_ptr),
                                              frozen_proto.add_tail_metalogs());
            });
    }
    if (frozen_proto.tail_metalogs().empty()) {
        return;
    }
    std::string serialized;
    CHECK(frozen_proto.SerializeToString(&serialized));
    zk_session()->Create(
        fmt::format("freeze/{}-{}", view->id(), my_node_id()),
        STRING_AS_SPAN(serialized),
        zk::ZKCreateMode::kPersistentSequential,
        [this](zk::ZKStatus status, const zk::ZKResult& result, bool*) {
            if (!status.ok()) {
                HLOG(FATAL) << "Failed to publish freeze data: "
                            << status.ToString();
            }
            HLOG_F(INFO, "Frozen at ZK path {}", result.path);
        });
}

void
Sequencer::OnViewFinalized(const FinalizedView* finalized_view)
{
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
    if (current_primary_ != nullptr) {
        log_utils::FinalizedLogSpace<MetaLogPrimary>(current_primary_,
                                                     finalized_view);
    }
    backup_collection_.ForEachActiveLogSpace(
        finalized_view->view(),
        [finalized_view](uint32_t, LockablePtr<MetaLogBackup> logspace_ptr) {
            log_utils::FinalizedLogSpace<MetaLogBackup>(std::move(logspace_ptr),
                                                        finalized_view);
        });
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

#define PANIC_IF_FROM_FUTURE_VIEW(MESSAGE_VAR)             \
    do {                                                   \
        if (current_view_ == nullptr ||                    \
            (MESSAGE_VAR).view_id > current_view_->id()) { \
            HLOG_F(FATAL,                                  \
                   "Receive message from future view {}",  \
                   (MESSAGE_VAR).view_id);                 \
        }                                                  \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)              \
    do {                                                   \
        if (current_view_ != nullptr &&                    \
            (MESSAGE_VAR).view_id < current_view_->id()) { \
            HLOG_F(WARNING,                                \
                   "Receive outdate message from view {}", \
                   (MESSAGE_VAR).view_id);                 \
            return;                                        \
        }                                                  \
    } while (0)

#define RETURN_IF_LOGSPACE_INACTIVE(LOGSPACE_PTR)                                  \
    do {                                                                           \
        if ((LOGSPACE_PTR)->frozen()) {                                            \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier();                   \
            HLOG_F(WARNING, "LogSpace {} is frozen", bits::HexStr0x(logspace_id)); \
            return;                                                                \
        }                                                                          \
        if ((LOGSPACE_PTR)->finalized()) {                                         \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier();                   \
            HLOG_F(WARNING,                                                        \
                   "LogSpace {} is finalized",                                     \
                   bits::HexStr0x(logspace_id));                                   \
            return;                                                                \
        }                                                                          \
    } while (0)

void
Sequencer::HandleTrimRequest(const SharedLogMessage& request)
{
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void
Sequencer::OnRecvMetaLogProgress(const SharedLogMessage& message)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::META_PROG);
    const View* view = nullptr;
    absl::InlinedVector<MetaLogProto, 4> replicated_metalogs;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        PANIC_IF_FROM_FUTURE_VIEW(message); // I believe this will never happen
        IGNORE_IF_FROM_PAST_VIEW(message);
        view = current_view_;
        auto logspace_ptr =
            primary_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_logspace = logspace_ptr.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            uint32_t old_position = locked_logspace->replicated_metalog_position();
            locked_logspace->UpdateReplicaProgress(message.origin_node_id,
                                                   message.metalog_position);
            uint32_t new_position = locked_logspace->replicated_metalog_position();
            for (uint32_t pos = old_position; pos < new_position; pos++) {
                if (auto metalog = locked_logspace->GetMetaLog(pos);
                    metalog.has_value())
                {
                    replicated_metalogs.push_back(std::move(*metalog));
                } else {
                    HLOG_F(FATAL, "Cannot get meta log at position {}", pos);
                }
            }
        }
    }
    for (const MetaLogProto& metalog_proto: replicated_metalogs) {
        PropagateMetaLog(DCHECK_NOTNULL(view), metalog_proto);
    }
}

void
Sequencer::OnRecvShardProgress(const SharedLogMessage& message,
                               std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) ==
           SharedLogOpType::SHARD_PROG);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto logspace_ptr =
            primary_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_logspace = logspace_ptr.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            std::vector<uint32_t> progress(payload.size() / sizeof(uint32_t), 0);
            memcpy(progress.data(), payload.data(), payload.size());
            locked_logspace->UpdateStorageProgress(message.origin_node_id, progress);
        }
    }
}

void
Sequencer::OnRecvTxnStartRequest(const protocol::SharedLogMessage& message)
{
    HVLOG(1) << fmt::format("Recv Txn start from engine {} id {:#x}",
                            message.origin_node_id,
                            message.txn_id);
    DCHECK(SharedLogMessageHelper::GetOpType(message) ==
           SharedLogOpType::CC_TXN_START);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        // ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        // IGNORE_IF_FROM_PAST_VIEW(message);
        auto cc_engine =
            cc_reorder_engine_collections_.at(message.logspace_id).get();
        cc_engine->pending_.store(true, std::memory_order_relaxed);
        uint16_t engine_id = bits::LowHalf32((bits::HighHalf64(message.txn_id)));
        // uint32_t localid_lowhalf = bits::LowHalf64(message.txn_id);
        auto engine_txn_starts = cc_engine->pending_txn_starts_.at(engine_id).get();
        absl::MutexLock lock(&engine_txn_starts->mu_);
        if (engine_txn_starts->max_txn_id < message.txn_id) {
            engine_txn_starts->max_txn_id = message.txn_id;
        }
    }
}

void
Sequencer::OnRecvReorderRequest(const SharedLogMessage& message,
                                std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::REORDER);
    switch (SharedLogMessageHelper::GetCCType(message)) {
    case protocol::SharedLogOpType::CC_TXN_COMMIT:
        {
            CCReorderEngine* cc_engine;
            {
                absl::ReaderMutexLock view_lk(&view_mu_);
                cc_engine =
                    cc_reorder_engine_collections_.at(message.logspace_id).get();
            }
            cc_engine->pending_.store(true, std::memory_order_relaxed);
            cc_engine->OnReorderRequest(message, payload);
        }
        break;
    default:
        NOT_IMPLEMENTED();
    }
}

void
Sequencer::OnRecvNewMetaLogs(const SharedLogMessage& message,
                             std::span<const char> payload)
{
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    uint32_t logspace_id = message.logspace_id;
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), logspace_id);
    uint32_t old_metalog_position;
    uint32_t new_metalog_position;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto logspace_ptr = backup_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_logspace = logspace_ptr.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            old_metalog_position = locked_logspace->metalog_position();
            for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs()) {
                locked_logspace->ProvideMetaLog(metalog_proto);
            }
            new_metalog_position = locked_logspace->metalog_position();
        }
    }
    if (new_metalog_position > old_metalog_position) {
        SharedLogMessage response =
            SharedLogMessageHelper::NewMetaLogProgressMessage(logspace_id,
                                                              new_metalog_position);
        SendSequencerMessage(message.sequencer_id, &response);
    }
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef PANIC_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void
Sequencer::ProcessRequests(const std::vector<SharedLogRequest>& requests)
{
    for (const SharedLogRequest& request: requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

void
Sequencer::GrantTxnStartIds()
{
    CCReorderEngine* cc_engine;
    // const View* view = nullptr;
    uint32_t logspace_id;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_primary_ == nullptr || current_view_ == nullptr) {
            return;
        }
        // view = current_view_;
        auto locked_logspace = current_primary_.ReaderLock();
        RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
        logspace_id = locked_logspace->identifier();
        cc_engine = cc_reorder_engine_collections_.at(logspace_id).get();
    }

    int timer_count =
        cc_engine->timer_count_.fetch_add(1, std::memory_order_relaxed);
    if ((timer_count + 1) % cc_engine->max_interval_rel_ == 0) {
        std::vector<TxnCommitProto*> batch_for_reorder;
        bool pending = cc_engine->pending_.load(std::memory_order_relaxed);
        {
            absl::MutexLock lk(&cc_engine->append_mu_);
            int64_t now = GetMonotonicMicroTimestamp();
            if (now - cc_engine->last_batch_time_ > cc_engine->max_interval_) {
                cc_engine->CollectLocalBatch(&batch_for_reorder, now);
            }
        }
        if (!batch_for_reorder.empty() || pending) {
            cc_engine->ProcessLocalBatch(batch_for_reorder);
        }
    }

    // bool pending = false;
    // absl::flat_hash_map<uint16_t, absl::InlinedVector<uint64_t, 16>>
    //     per_engine_aborts_;
    // absl::flat_hash_map<uint16_t, uint64_t> latest_txn_starts_;
    // for (uint16_t engine_id: cc_engine->current_view_->GetEngineNodes()) {
    //     auto engine_txn_starts =
    //     cc_engine->pending_txn_starts_.at(engine_id).get();
    //     {
    //         absl::MutexLock lock(&engine_txn_starts->mu_);
    //         if (engine_txn_starts->max_txn_id >
    //             cc_engine->last_txn_starts_[engine_id])
    //         {
    //             cc_engine->last_txn_starts_[engine_id] =
    //                 engine_txn_starts->max_txn_id;
    //             // engine_ids.push_back(engine_id);
    //             latest_txn_starts_[engine_id] = engine_txn_starts->max_txn_id;
    //             pending = true;
    //         }
    //     }
    //     auto engine_aborts = cc_engine->pending_txn_aborts_[engine_id].get();
    //     // per_engine_aborts[engine_id].push_back(txn_id);

    //     // cc_engine->per_engine_aborts_[engine_id].clear();
    //     {
    //         absl::MutexLock lk(&engine_aborts->mu_);
    //         // global_batch.mutable_txn_ids()->Add(engine_aborts->txn_ids.begin(),
    //         //                                     engine_aborts->txn_ids.end());
    //         // engine_aborts->txn_ids.clear();
    //         if (!engine_aborts->txn_ids.empty()) {
    //             pending = true;
    //             per_engine_aborts_[engine_id].swap(engine_aborts->txn_ids);
    //         }
    //     }
    // }
    // // if (engine_ids.empty()) {
    // //     return;
    // // }
    // if (!pending) {
    //     return;
    // }
    // // uint64_t global_batch_id =
    // //     cc_engine->next_global_batch_id_.fetch_add(1,
    // std::memory_order_relaxed); uint64_t global_batch_id;
    // {
    //     absl::MutexLock lk(&cc_engine->apply_mu_);
    //     global_batch_id = cc_engine->next_global_batch_id_++;
    // }
    // HVLOG(1) << fmt::format("issue global batch {}", global_batch_id);
    // CCGlobalBatchProto global_batch;
    // // auto& my_space = cc_engine->MyWorkerSpace();
    // // auto& global_batch = my_space.global_batch;
    // // global_batch.Clear();
    // global_batch.set_global_batch_id(global_batch_id);
    // std::string serialized_global_batch;
    // auto response = SharedLogMessageHelper::NewCCGlobalBatchMessage(logspace_id);
    // response.origin_node_id = my_node_id();
    // for (uint16_t engine_id: cc_engine->current_view_->GetEngineNodes()) {
    //     // forward aborts
    //     global_batch.clear_txn_ids();
    //     global_batch.clear_results();
    //     auto& engine_aborts = per_engine_aborts_[engine_id];
    //     global_batch.mutable_txn_ids()->Add(engine_aborts.begin(),
    //                                         engine_aborts.end());
    //     int num_results = global_batch.txn_ids_size();
    //     global_batch.mutable_results()->Reserve(num_results);
    //     for (int i = 0; i < num_results; i++) {
    //         global_batch.add_results(CCGlobalBatchProto::ABORT);
    //     }
    //     // forward txn starts
    //     // uint64_t last_txn_start = cc_engine->last_txn_starts_.at(engine_id);
    //     global_batch.set_latest_txn_start(latest_txn_starts_[engine_id]);
    //     global_batch.SerializeToString(&serialized_global_batch);

    //     // HVLOG(1) << fmt::format(
    //     //     "engine {}: sending {} txn aborts, latest start {:#x}",
    //     //     engine_id,
    //     //     num_results,
    //     //     last_txn_start);
    //     // send to engine
    //     response.payload_size =
    //         gsl::narrow_cast<uint32_t>(serialized_global_batch.size());
    //     bool success =
    //     SendSharedLogMessage(protocol::ConnType::SEQUENCER_TO_ENGINE,
    //                                         engine_id,
    //                                         response,
    //                                         STRING_AS_SPAN(serialized_global_batch));
    //     if (!success) {
    //         LOG_F(ERROR,
    //               "cc reorder engine {}-{} failed to grant txn start to engine
    //               {}", my_node_id(), server::IOWorker::current()->worker_id(),
    //               engine_id);
    //     }
    // }
}

void
Sequencer::MarkNextCutIfDoable()
{
    const View* view = nullptr;
    std::optional<MetaLogProto> meta_log_proto;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_primary_ == nullptr || current_view_ == nullptr) {
            return;
        }
        view = current_view_;
        {
            auto locked_logspace = current_primary_.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            if (!locked_logspace->all_metalog_replicated()) {
                HLOG(INFO) << "Not all meta log replicated, will not mark new cut";
                return;
            }
            meta_log_proto = locked_logspace->MarkNextCut();
        }
    }
    if (meta_log_proto.has_value()) {
        ReplicateMetaLog(view, *meta_log_proto);
    }
}

#undef RETURN_IF_LOGSPACE_INACTIVE

}} // namespace faas::log
