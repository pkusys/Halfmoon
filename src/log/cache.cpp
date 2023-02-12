#include "log/cache.h"
#include "log/utils.h"
#include <string>

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
__END_THIRD_PARTY_HEADERS

namespace faas { namespace log {

LRUCache::LRUCache(int mem_cap_mb)
{
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    dbm_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
}

LRUCache::~LRUCache() {}

namespace {
static inline std::string
EncodeLogEntry(const LogMetaData& log_metadata,
               std::span<const uint64_t> user_tags,
               std::span<const char> log_data)
{
    DCHECK_EQ(log_metadata.num_tags, user_tags.size());
    DCHECK_EQ(log_metadata.data_size, log_data.size());
    size_t total_size =
        log_data.size() + user_tags.size() * sizeof(uint64_t) + sizeof(LogMetaData);
    std::string encoded;
    encoded.resize(total_size);
    char* ptr = encoded.data();
    DCHECK_GT(log_data.size(), 0U);
    memcpy(ptr, log_data.data(), log_data.size());
    ptr += log_data.size();
    if (user_tags.size() > 0) {
        memcpy(ptr, user_tags.data(), user_tags.size() * sizeof(uint64_t));
        ptr += user_tags.size() * sizeof(uint64_t);
    }
    memcpy(ptr, &log_metadata, sizeof(LogMetaData));
    return encoded;
}

static inline void
DecodeLogEntry(std::string encoded, LogEntry* log_entry)
{
    DCHECK_GT(encoded.size(), sizeof(LogMetaData));
    LogMetaData& metadata = log_entry->metadata;
    memcpy(&metadata,
           encoded.data() + encoded.size() - sizeof(LogMetaData),
           sizeof(LogMetaData));
    size_t total_size = metadata.data_size + metadata.num_tags * sizeof(uint64_t) +
                        sizeof(LogMetaData);
    DCHECK_EQ(total_size, encoded.size());
    if (metadata.num_tags > 0) {
        std::span<const uint64_t> user_tags(
            reinterpret_cast<const uint64_t*>(encoded.data() + metadata.data_size),
            metadata.num_tags);
        log_entry->user_tags.assign(user_tags.begin(), user_tags.end());
    } else {
        log_entry->user_tags.clear();
    }
    encoded.resize(metadata.data_size);
    log_entry->data = std::move(encoded);
}
} // namespace

// 1. engines cache(only data) upon op finished
// 2. storage side, support full mode and lite mode(only data) read/write, but always
// store in full mode.
// 3. storage and engine cache all use localid as index, and the mapping between
// localid and seqnum is defined by metalog
// 4. aux data is indexed by seqnum

void
LRUCache::CCPut(uint64_t localid, std::span<const char> log_data)
{
    std::string key = fmt::format("l{:016x}", localid);
    dbm_->Set(key,
              std::string_view(log_data.data(), log_data.size()),
              /* overwrite= */ true);
}

void
LRUCache::CCPut(uint64_t seqnum, uint64_t key, std::span<const char> value)
{
    std::string key_str = fmt::format("{:016x}-{:016x}", seqnum, key);
    dbm_->Set(key_str,
              std::string_view(value.data(), value.size()),
              /* overwrite= */ true);
}

std::optional<std::string>
LRUCache::CCGet(uint64_t localid)
{
    std::string key = fmt::format("l{:016x}", localid);
    std::string log_data;
    if (dbm_->Get(key, &log_data) != tkrzw::Status::SUCCESS) {
        return std::nullopt;
    }
    return log_data;
}

std::optional<std::string>
LRUCache::CCGet(uint64_t seqnum, uint64_t key)
{
    std::string key_str = fmt::format("{:016x}-{:016x}", seqnum, key);
    std::string value;
    if (dbm_->Get(key_str, &value) != tkrzw::Status::SUCCESS) {
        return std::nullopt;
    }
    return value;
}

void
LRUCache::CCPutAuxData(uint64_t seqnum,
                       //    uint64_t key,
                       std::span<const char> aux_data)
{
    std::string key = fmt::format("s{:016x}", seqnum);
    dbm_->Set(key,
              std::string_view(aux_data.data(), aux_data.size()),
              /* overwrite= */ true);
}

std::optional<std::string>
LRUCache::CCGetAuxData(uint64_t seqnum /* uint64_t key */)
{
    std::string key = fmt::format("s{:016x}", seqnum);
    // VLOG(1) << fmt::format("get aux data with key={}", key_str);
    std::string aux_data;
    if (dbm_->Get(key, &aux_data) != tkrzw::Status::SUCCESS) {
        return std::nullopt;
    }
    return aux_data;
}

void
LRUCache::Put(const LogMetaData& log_metadata,
              std::span<const uint64_t> user_tags,
              std::span<const char> log_data)
{
    std::string key_str = fmt::format("0_{:016x}", log_metadata.seqnum);
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    dbm_->Set(key_str, data, /* overwrite= */ false);
}

std::optional<LogEntry>
LRUCache::Get(uint64_t seqnum)
{
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        LogEntry log_entry;
        DecodeLogEntry(std::move(data), &log_entry);
        DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
        return log_entry;
    } else {
        return std::nullopt;
    }
}

void
LRUCache::PutAuxData(uint64_t seqnum, std::span<const char> data)
{
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    dbm_->Set(key_str,
              std::string_view(data.data(), data.size()),
              /* overwrite= */ true);
}

std::optional<std::string>
LRUCache::GetAuxData(uint64_t seqnum)
{
    std::string key_str = fmt::format("1_{:016x}", seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        return data;
    } else {
        return std::nullopt;
    }
}

}} // namespace faas::log
