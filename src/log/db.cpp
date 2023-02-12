#include "log/db.h"

#include "utils/bits.h"

__BEGIN_THIRD_PARTY_HEADERS

#include <rocksdb/db.h>

#include <tkrzw_dbm.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_dbm_tree.h>
#include <tkrzw_dbm_skip.h>

__END_THIRD_PARTY_HEADERS

ABSL_FLAG(int, rocksdb_max_background_jobs, 2, "");
ABSL_FLAG(size_t, rocksdb_block_cache_size_mb, 1024, "");
ABSL_FLAG(bool, rocksdb_enable_compression, false, "");

#define ROCKSDB_CHECK_OK(STATUS_VAR, OP_NAME)              \
    do {                                                   \
        if (!(STATUS_VAR).ok()) {                          \
            LOG(FATAL) << "RocksDB::" #OP_NAME " failed: " \
                       << (STATUS_VAR).ToString();         \
        }                                                  \
    } while (0)

#define TKRZW_CHECK_OK(STATUS_VAR, OP_NAME)              \
    do {                                                 \
        if (!(STATUS_VAR).IsOK()) {                      \
            LOG(FATAL) << "Tkrzw::" #OP_NAME " failed: " \
                       << tkrzw::ToString(STATUS_VAR);   \
        }                                                \
    } while (0)

#define log_header_ "LogDB: "

namespace faas { namespace log {

RocksDBBackend::RocksDBBackend(std::string_view db_path)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.max_background_jobs = absl::GetFlag(FLAGS_rocksdb_max_background_jobs);
    rocksdb::DB* db;
    HLOG_F(INFO, "Open RocksDB at path {}", db_path);
    auto status = rocksdb::DB::Open(options, std::string(db_path), &db);
    ROCKSDB_CHECK_OK(status, Open);
    db_.reset(db);
}

RocksDBBackend::~RocksDBBackend() {}

void
RocksDBBackend::InstallLogSpace(uint32_t logspace_id)
{
    HLOG_F(INFO, "Install log space {}", bits::HexStr0x(logspace_id));
    rocksdb::ColumnFamilyOptions options;
    if (absl::GetFlag(FLAGS_rocksdb_enable_compression)) {
        options.compression = rocksdb::kZSTD;
    } else {
        options.compression = rocksdb::kNoCompression;
    }
    options.OptimizeForPointLookup(absl::GetFlag(FLAGS_rocksdb_block_cache_size_mb));
    rocksdb::ColumnFamilyHandle* cf_handle = nullptr;
    auto status =
        db_->CreateColumnFamily(options, bits::HexStr(logspace_id), &cf_handle);
    ROCKSDB_CHECK_OK(status, CreateColumnFamily);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!column_families_.contains(logspace_id));
        column_families_[logspace_id].reset(DCHECK_NOTNULL(cf_handle));
    }
}

std::optional<std::string>
RocksDBBackend::Get(uint32_t logspace_id, uint64_t key)
{
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return std::nullopt;
    }
    std::string key_str = bits::HexStr(key);
    std::string data;
    auto status = db_->Get(rocksdb::ReadOptions(), cf_handle, key_str, &data);
    if (status.IsNotFound()) {
        return std::nullopt;
    }
    ROCKSDB_CHECK_OK(status, Get);
    return data;
}

void
RocksDBBackend::Put(uint32_t logspace_id, uint64_t key, std::span<const char> data)
{
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(ERROR, "Log space {} not created", bits::HexStr0x(logspace_id));
        return;
    }
    std::string key_str = bits::HexStr(key);
    auto status = db_->Put(rocksdb::WriteOptions(),
                           cf_handle,
                           key_str,
                           rocksdb::Slice(data.data(), data.size()));
    ROCKSDB_CHECK_OK(status, Put);
}

std::optional<std::string>
RocksDBBackend::GetKV(uint64_t seqnum, uint64_t key)
{
    uint32_t logspace_id = bits::HighHalf64(seqnum);
    uint32_t seqnum_lowhalf = bits::LowHalf64(seqnum);
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return std::nullopt;
    }
    std::string key_str = fmt::format("{:08x}-{:016x}", seqnum_lowhalf, key);
    std::string data;
    auto status = db_->Get(rocksdb::ReadOptions(), cf_handle, key_str, &data);
    if (status.IsNotFound()) {
        return std::nullopt;
    }
    ROCKSDB_CHECK_OK(status, Get);
    return data;
}

void
RocksDBBackend::PutKV(uint64_t seqnum, uint64_t key, std::span<const char> value)
{
    uint32_t logspace_id = bits::HighHalf64(seqnum);
    uint32_t seqnum_lowhalf = bits::LowHalf64(seqnum);
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(ERROR, "Log space {} not created", bits::HexStr0x(logspace_id));
        return;
    }
    std::string key_str = fmt::format("{:08x}-{:016x}", seqnum_lowhalf, key);
    auto status = db_->Put(rocksdb::WriteOptions(),
                           cf_handle,
                           key_str,
                           rocksdb::Slice(value.data(), value.size()));
    ROCKSDB_CHECK_OK(status, Put);
}

rocksdb::ColumnFamilyHandle*
RocksDBBackend::GetCFHandle(uint32_t logspace_id)
{
    absl::ReaderMutexLock lk(&mu_);
    if (!column_families_.contains(logspace_id)) {
        return nullptr;
    }
    return column_families_.at(logspace_id).get();
}

TkrzwDBMBackend::TkrzwDBMBackend(Type type, std::string_view db_path)
    : type_(type),
      db_path_(db_path)
{}

TkrzwDBMBackend::~TkrzwDBMBackend()
{
    for (const auto& [logspace_id, dbm]: dbs_) {
        auto status = dbm->Close();
        TKRZW_CHECK_OK(status, Close);
    }
}

void
TkrzwDBMBackend::InstallLogSpace(uint32_t logspace_id)
{
    HLOG_F(INFO, "Install log space {}", bits::HexStr0x(logspace_id));
    tkrzw::DBM* db_ptr = nullptr;
    if (type_ == kHashDBM) {
        tkrzw::HashDBM* db = new tkrzw::HashDBM();
        tkrzw::HashDBM::TuningParameters params;
        auto status = db->OpenAdvanced(
            /* path= */ fmt::format("{}/{}.tkh",
                                    db_path_,
                                    bits::HexStr(logspace_id)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ params);
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else if (type_ == kTreeDBM) {
        tkrzw::TreeDBM* db = new tkrzw::TreeDBM();
        tkrzw::TreeDBM::TuningParameters params;
        auto status = db->OpenAdvanced(
            /* path= */ fmt::format("{}/{}.tkt",
                                    db_path_,
                                    bits::HexStr(logspace_id)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ params);
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else if (type_ == kSkipDBM) {
        tkrzw::SkipDBM* db = new tkrzw::SkipDBM();
        tkrzw::SkipDBM::TuningParameters params;
        auto status = db->OpenAdvanced(
            /* path= */ fmt::format("{}/{}.tks",
                                    db_path_,
                                    bits::HexStr(logspace_id)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ params);
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else {
        UNREACHABLE();
    }

    {
        absl::MutexLock lk(&mu_);
        DCHECK(!dbs_.contains(logspace_id));
        dbs_[logspace_id].reset(DCHECK_NOTNULL(db_ptr));
    }
}

std::optional<std::string>
TkrzwDBMBackend::Get(uint32_t logspace_id, uint64_t key)
{
    tkrzw::DBM* dbm = GetDBM(logspace_id);
    if (dbm == nullptr) {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return std::nullopt;
    }
    std::string key_str = bits::HexStr(key);
    std::string data;
    auto status = dbm->Get(key_str, &data);
    if (status.IsOK()) {
        return data;
    } else {
        return std::nullopt;
    }
}

void
TkrzwDBMBackend::Put(uint32_t logspace_id, uint64_t key, std::span<const char> data)
{
    tkrzw::DBM* dbm = GetDBM(logspace_id);
    if (dbm == nullptr) {
        HLOG_F(FATAL, "Log space {} not created", bits::HexStr0x(logspace_id));
    }
    std::string key_str = bits::HexStr(key);
    auto status = dbm->Set(key_str, std::string_view(data.data(), data.size()));
    TKRZW_CHECK_OK(status, Set);
}

tkrzw::DBM*
TkrzwDBMBackend::GetDBM(uint32_t logspace_id)
{
    absl::ReaderMutexLock lk(&mu_);
    if (!dbs_.contains(logspace_id)) {
        return nullptr;
    }
    return dbs_.at(logspace_id).get();
}

}} // namespace faas::log
