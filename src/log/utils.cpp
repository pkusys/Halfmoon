#include "log/utils.h"

#include "base/logging.h"
#include "base/std_span.h"
#include "common/protocol.h"
#include "log/common.h"
#include "proto/shared_log.pb.h"
#include "utils/bits.h"
#include <cstdint>
#include <string>

namespace faas { namespace log_utils {

using log::View;
using log::SharedLogRequest;
using log::LogMetaData;
using log::LogEntryProto;
using log::MetaLogProto;
using log::MetaLogsProto;
using protocol::SharedLogMessage;

using protocol::SharedLogOpType;

uint16_t
GetViewId(uint64_t value)
{
    return bits::HighHalf32(bits::HighHalf64(value));
}

FutureRequests::FutureRequests()
    : next_view_id_(0)
{}

FutureRequests::~FutureRequests() {}

void
FutureRequests::OnNewView(const View* view,
                          std::vector<SharedLogRequest>* ready_requests)
{
    absl::MutexLock lk(&mu_);
    if (view->id() != next_view_id_) {
        LOG_F(FATAL,
              "Views are not consecutive: have={}, expect={}",
              view->id(),
              next_view_id_);
    }
    if (onhold_requests_.contains(view->id())) {
        if (ready_requests == nullptr) {
            LOG_F(FATAL, "Not expect on-hold requests for view {}", view->id());
        }
        *ready_requests = std::move(onhold_requests_[view->id()]);
        onhold_requests_.erase(view->id());
    }
    next_view_id_++;
}

void
FutureRequests::OnHoldRequest(uint16_t view_id, SharedLogRequest request)
{
    absl::MutexLock lk(&mu_);
    if (view_id < next_view_id_) {
        LOG_F(FATAL,
              "Receive request from view not in the future: "
              "request_view_id={}, next_view_id={}",
              view_id,
              next_view_id_);
    }
    onhold_requests_[view_id].push_back(std::move(request));
}

MetaLogsProto
MetaLogsFromPayload(std::span<const char> payload)
{
    MetaLogsProto metalogs_proto;
    if (!metalogs_proto.ParseFromArray(payload.data(),
                                       static_cast<int>(payload.size())))
    {
        LOG(FATAL) << "Failed to parse MetaLogsProto";
    }
    if (metalogs_proto.metalogs_size() == 0) {
        LOG(FATAL) << "Empty MetaLogsProto";
    }
    uint32_t logspace_id = metalogs_proto.logspace_id();
    for (const MetaLogProto& metalog_proto: metalogs_proto.metalogs()) {
        if (metalog_proto.logspace_id() != logspace_id) {
            LOG(FATAL)
                << "Meta logs in on MetaLogsProto must have the same logspace_id";
        }
    }
    return metalogs_proto;
}

LogMetaData
GetMetaDataFromMessage(const SharedLogMessage& message)
{
    size_t total_size = message.payload_size;
    size_t num_tags = message.num_tags;
    size_t aux_data_size = message.aux_data_size;
    DCHECK_LT(num_tags * sizeof(uint64_t) + aux_data_size, total_size);
    size_t log_data_size = total_size - num_tags * sizeof(uint64_t) - aux_data_size;
    return LogMetaData{
        .user_logspace = message.user_logspace,
        .seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf),
        .localid = message.localid,
        .num_tags = num_tags,
        .data_size = log_data_size};
}

void
SplitPayloadForMessage(const protocol::SharedLogMessage& message,
                       std::span<const char> payload,
                       std::span<const uint64_t>* user_tags,
                       std::span<const char>* log_data,
                       std::span<const char>* aux_data)
{
    size_t total_size = message.payload_size;
    DCHECK_EQ(payload.size(), total_size);
    size_t num_tags = message.num_tags;
    size_t aux_data_size = message.aux_data_size;
    DCHECK_LT(num_tags * sizeof(uint64_t) + aux_data_size, total_size);
    size_t log_data_size = total_size - num_tags * sizeof(uint64_t) - aux_data_size;
    const char* ptr = payload.data();
    if (user_tags != nullptr) {
        if (num_tags > 0) {
            *user_tags =
                std::span<const uint64_t>(reinterpret_cast<const uint64_t*>(ptr),
                                          num_tags);
        } else {
            *user_tags = std::span<const uint64_t>();
        }
    }
    ptr += num_tags * sizeof(uint64_t);
    if (log_data != nullptr) {
        *log_data = std::span<const char>(ptr, log_data_size);
    }
    ptr += log_data_size;
    if (aux_data != nullptr) {
        if (aux_data_size > 0) {
            *aux_data = std::span<const char>(ptr, aux_data_size);
        } else {
            *aux_data = std::span<const char>();
        }
    }
}

void
PopulateMetaDataToMessage(const LogMetaData& metadata, SharedLogMessage* message)
{
    message->logspace_id = bits::HighHalf64(metadata.seqnum);
    message->user_logspace = metadata.user_logspace;
    message->seqnum_lowhalf = bits::LowHalf64(metadata.seqnum);
    message->num_tags = gsl::narrow_cast<uint16_t>(metadata.num_tags);
    message->localid = metadata.localid;
}

void
PopulateMetaDataToMessage(const LogEntryProto& log_entry, SharedLogMessage* message)
{
    message->logspace_id = bits::HighHalf64(log_entry.seqnum());
    message->user_logspace = log_entry.user_logspace();
    message->seqnum_lowhalf = bits::LowHalf64(log_entry.seqnum());
    message->num_tags = gsl::narrow_cast<uint16_t>(log_entry.user_tags_size());
    message->localid = log_entry.localid();
}

void
FillReplicateMsgWithOp(protocol::SharedLogMessage* message, log::LocalOp* op)
{
    message->op_type = static_cast<uint16_t>(SharedLogOpType::REPLICATE);
    // cc_type is of no use in replicate, the storage needs not to understand it.
    // instead, set up flag to notify storage that this is cc op, following cc path
    message->flags |= protocol::kSLogIsCCOpFlag;
    message->user_logspace = op->user_logspace;
    message->logspace_id = op->logspace_id;
    // cc specific
    // message->cc_type = static_cast<uint16_t>(op->type);
    // make sure this is the same as txn_id in commitheader
    message->txn_localid = op->txn_localid;
    message->payload_size = gsl::narrow_cast<uint32_t>(op->data.length());
}

void
ReorderMsgFromReplicateMsg(protocol::SharedLogMessage* message, log::LocalOp* op)
{
    message->op_type = static_cast<uint16_t>(SharedLogOpType::REORDER);
    message->cc_type = static_cast<uint16_t>(op->type);
    switch (op->type) {
    case SharedLogOpType::CC_TXN_COMMIT: // cc info is in the payload
        {
            CHECK(is_aligned<protocol::TxnCommitHeader>(op->data.data()));
            auto commit_header =
                reinterpret_cast<protocol::TxnCommitHeader*>(op->data.data());
            message->payload_size = gsl::narrow_cast<uint32_t>(
                sizeof(protocol::TxnCommitHeader) +
                commit_header->read_set_size * sizeof(uint64_t) +
                commit_header->write_set_size * sizeof(uint64_t));
            break;
        }
    case SharedLogOpType::CC_READ_LOCK:
    case SharedLogOpType::CC_WRITE_LOCK:
    case SharedLogOpType::CC_READ_UNLOCK:
    case SharedLogOpType::CC_WRITE_UNLOCK:
        {
            CHECK(is_aligned<protocol::TxnLockHeader>(op->data.data()));
            auto lock_header =
                reinterpret_cast<protocol::TxnLockHeader*>(op->data.data());
            message->txn_id = lock_header->txn_id;
            message->query_tag = lock_header->key;
            message->payload_size = 0;
            // TODO: add hint
            break;
        }
    default:
        LOG(FATAL) << "cc op not implemented";
    }
}

// std::span<const char>
// TxnCommitProtoFromOp(const log::LocalOp* op)
// {
//     DCHECK(op->type == protocol::SharedLogOpType::CC_TXN_COMMIT);
//     DCHECK_GE(op->data.length(), sizeof(protocol::TxnCommitHeader));
//     const char* ptr = op->data.data();
//     auto header_ptr = reinterpret_cast<const protocol::TxnCommitHeader*>(ptr);
//     DCHECK_EQ(op->data.length(),
//               sizeof(protocol::TxnCommitHeader) +
//                   header_ptr->read_set_size * sizeof(uint64_t) +
//                   header_ptr->write_set_size * sizeof(uint64_t));
//     ptr += sizeof(protocol::TxnCommitHeader);
//     auto read_set_ptr = reinterpret_cast<const uint64_t*>(ptr);
//     auto write_set_ptr = read_set_ptr + header_ptr->read_set_size;

//     SLogCCEntry cc_entry;
//     // The following fields are set in sharedlogmessage header, no need to set
//     here
//     // cc_entry.set_engine_id(bits::HighHalf64(op->txn_id));
//     // cc_entry.set_txn_id(bits::LowHalf64(op->txn_id));
//     cc_entry.set_type(SLogCCEntry::TXN_COMMIT);
//     auto commit_msg = cc_entry.mutable_commit_msg();
//     commit_msg->set_start_seqnum(bits::LowHalf64(op->seqnum));
//     commit_msg->mutable_read_set()->Add(read_set_ptr, write_set_ptr);
//     commit_msg->mutable_write_set()->Add(write_set_ptr,
//                                          write_set_ptr +
//                                          header_ptr->write_set_size);
//     std::string proto_string;
//     cc_entry.SerializeToString(&proto_string);
//     return STRING_AS_SPAN(proto_string);
// }

}} // namespace faas::log_utils
