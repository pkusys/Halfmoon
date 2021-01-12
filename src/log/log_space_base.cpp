#include "log/log_space_base.h"

#include "utils/bits.h"

namespace faas {
namespace log {

LogSpaceBase::LogSpaceBase(Mode mode, const View* view, uint16_t sequencer_id)
    : mode_(mode),
      state_(kCreated),
      view_(view),
      sequencer_node_(view->GetSequencerNode(sequencer_id)),
      metalog_position_(0),
      seqnum_position_(0),
      log_header_(fmt::format("LogSpace[{}-{}]: ", view->id(), sequencer_id)),
      shard_progrsses_(view->num_engine_nodes(), 0) {}

LogSpaceBase::~LogSpaceBase() {}

void LogSpaceBase::AddInterestedShard(uint16_t engine_id) {
    DCHECK(state_ == kCreated);
    const View::NodeIdVec& engine_node_ids = view_->GetEngineNodes();
    size_t idx = absl::c_find(engine_node_ids, engine_id) - engine_node_ids.begin();
    DCHECK_LT(idx, engine_node_ids.size());
    interested_shards_.insert(idx);
}

bool LogSpaceBase::ProvideMetaLog(const MetaLogProto& meta_log) {
    DCHECK(state_ == kNormal);
    if (mode_ == kLiteMode && meta_log.type() == MetaLogProto::TRIM) {
        HLOG(WARNING) << fmt::format("Trim log (seqnum={}) is simply ignore in lite mode",
                                     meta_log.metalog_seqnum());
        return false;
    }
    uint32_t seqnum = meta_log.metalog_seqnum();
    MetaLogProto* meta_log_copy = metalog_pool_.Get();
    meta_log_copy->CopyFrom(meta_log);
    pending_metalogs_[seqnum] = meta_log_copy;
    uint32_t prev_metalog_position = metalog_position_;
    AdvanceMetaLogProgress();
    return metalog_position_ > prev_metalog_position;
}

void LogSpaceBase::Freeze() {
    DCHECK(state_ == kNormal);
    state_ = kFrozen;
}

bool LogSpaceBase::Finalize(uint32_t final_metalog_position,
                            const std::vector<MetaLogProto>& tail_metalogs) {
    DCHECK(state_ == kNormal || state_ == kFrozen);
    for (const MetaLogProto& meta_log : tail_metalogs) {
        ProvideMetaLog(meta_log);
    }
    if (metalog_position_ == final_metalog_position) {
        state_ = kFinalized;
        OnFinalized();
        return true;
    } else {
        return false;
    }
}

void LogSpaceBase::SerializeToProto(MetaLogsProto* meta_logs_proto) {
    DCHECK(state_ == kFinalized && mode_ == kFullMode);
    meta_logs_proto->Clear();
    for (const MetaLogProto* metalog : applied_metalogs_) {
        meta_logs_proto->add_metalogs()->CopyFrom(*metalog);
    }
}

void LogSpaceBase::AdvanceMetaLogProgress() {
    auto iter = pending_metalogs_.begin();
    while (iter != pending_metalogs_.end()) {
        if (iter->first < metalog_position_) {
            iter = pending_metalogs_.erase(iter);
            continue;
        }
        MetaLogProto* meta_log = iter->second;
        if (!CanApplyMetaLog(*meta_log)) {
            break;
        }
        ApplyMetaLog(*meta_log);
        switch (mode_) {
        case kLiteMode:
            metalog_pool_.Return(meta_log);
            break;
        case kFullMode:
            DCHECK_EQ(size_t{metalog_position_}, applied_metalogs_.size());
            applied_metalogs_.push_back(meta_log);
            break;
        default:
            UNREACHABLE();
        }
        metalog_position_ = meta_log->metalog_seqnum() + 1;
        iter = pending_metalogs_.erase(iter);
    }
}

bool LogSpaceBase::CanApplyMetaLog(const MetaLogProto& meta_log) {
    switch (mode_) {
    case kLiteMode:
        switch (meta_log.type()) {
        case MetaLogProto::NEW_LOGS:
            for (size_t shard_idx : interested_shards_) {
                uint32_t shard_start = meta_log.new_logs_proto().shard_starts(shard_idx);
                DCHECK_GE(shard_start, shard_progrsses_[shard_idx]);
                if (shard_start > shard_progrsses_[shard_idx]) {
                    return false;
                }
            }
            return true;
        default:
            break;
        }
        break;
    case kFullMode:
        return meta_log.metalog_seqnum() == metalog_position_;
    default:
        break;
    }
    UNREACHABLE();
}

void LogSpaceBase::ApplyMetaLog(const MetaLogProto& meta_log) {
    switch (meta_log.type()) {
    case MetaLogProto::NEW_LOGS:
        {
            const auto& new_logs = meta_log.new_logs_proto();
            const View::NodeIdVec& engine_node_ids = view_->GetEngineNodes();
            uint32_t start_seqnum = new_logs.start_seqnum();
            for (size_t i = 0; i < engine_node_ids.size(); i++) {
                uint64_t start_localid = bits::JoinTwo32(
                    engine_node_ids[i], new_logs.shard_starts(i));
                uint32_t delta = new_logs.shard_deltas(i);
                if (mode_ == kFullMode || interested_shards_.contains(i)) {
                    OnNewLogs(start_seqnum, start_localid, delta);
                }
                shard_progrsses_[i] = new_logs.shard_starts(i) + delta;
                start_seqnum += delta;
            }
            DCHECK_GT(start_seqnum, seqnum_position_);
            seqnum_position_ = start_seqnum;
        }
        break;
    case MetaLogProto::TRIM:
        DCHECK(mode_ == kFullMode);
        {
            const auto& trim = meta_log.trim_proto();
            OnTrim(trim.user_logspace(), trim.user_tag(), trim.trim_seqnum());
        }
        break;
    default:
        UNREACHABLE();
    }
}

}  // namespace log
}  // namespace faas