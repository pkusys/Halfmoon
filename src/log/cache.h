#pragma once

#include "log/common.h"

// Forward declarations
namespace tkrzw {
class CacheDBM;
}

namespace faas { namespace log {

class LRUCache {
public:
    explicit LRUCache(int mem_cap_mb);
    ~LRUCache();

    void Put(const LogMetaData& log_metadata,
             std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    std::optional<LogEntry> Get(uint64_t seqnum);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t seqnum);

    void CCPut(uint64_t localid, std::span<const char> log_data);
    void CCPut(uint64_t seqnum, uint64_t key, std::span<const char> log_data);
    std::optional<std::string> CCGet(uint64_t localid);
    std::optional<std::string> CCGet(uint64_t seqnum, uint64_t key);

    void CCPutAuxData(uint64_t seqnum,
                      //   uint64_t key,
                      std::span<const char> aux_data);
    std::optional<std::string> CCGetAuxData(uint64_t seqnum /* uint64_t key */);

private:
    std::unique_ptr<tkrzw::CacheDBM> dbm_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

}} // namespace faas::log
