#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include <cstdint>

__BEGIN_THIRD_PARTY_HEADERS
#include "proto/shared_log.pb.h"
__END_THIRD_PARTY_HEADERS

namespace faas { namespace log {

constexpr uint64_t kEmptyLogTag = 0;
constexpr uint64_t kMaxLogSeqNum = 0xffff000000000000ULL;
constexpr uint64_t kInvalidLogSeqNum = protocol::kInvalidLogSeqNum;
constexpr uint64_t kInvalidLogTag = protocol::kInvalidLogTag;
constexpr uint32_t kInvalidCondPos = std::numeric_limits<uint32_t>::max();

struct SharedLogRequest {
    protocol::SharedLogMessage message;
    std::string payload;
    void* local_op;

    explicit SharedLogRequest(void* local_op)
        : local_op(local_op)
    {}

    explicit SharedLogRequest(const protocol::SharedLogMessage& message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN)
        : message(message),
          payload(),
          local_op(nullptr)
    {
        if (payload.size() > 0) {
            this->payload.assign(payload.data(), payload.size());
        }
    }
};

using UserTagVec = absl::InlinedVector<uint64_t, 8>;

struct LogMetaData {
    uint32_t user_logspace;
    uint64_t seqnum;
    uint64_t localid;
    size_t num_tags;
    size_t data_size;
};

struct LogEntry {
    LogMetaData metadata;
    UserTagVec user_tags;
    std::string data;
};

struct CCLogMetaData {
    // uint64_t localid;
    // uint64_t seqnum;
    uint16_t op_type;
    uint16_t num_tags;
    uint32_t cond_pos;
    uint64_t cond_tag; // if not kInvalidLogTag, then this is cond op
    // union {
    //     uint64_t cond_tag;  // if not kInvalidLogTag, then this is cond op
    //     uint64_t write_tag; // for async_write(cc_txn_write)
    // };
    // log_data size is implicit
};

static_assert(sizeof(CCLogMetaData) % sizeof(uint64_t) == 0,
              "CC log metadata must be 8bytes-aligned");

struct CCLogEntry {
    CCLogMetaData metadata;
    UserTagVec user_tags;
    std::string data;
};

}} // namespace faas::log
