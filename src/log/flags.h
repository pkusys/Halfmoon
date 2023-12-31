#pragma once

#include "common/flags.h"
#include <cstdint>

ABSL_DECLARE_FLAG(int, slog_local_cut_interval_us);
ABSL_DECLARE_FLAG(int, slog_global_cut_interval_us);
ABSL_DECLARE_FLAG(size_t, slog_log_space_hash_tokens);
ABSL_DECLARE_FLAG(size_t, slog_num_tail_metalog_entries);

ABSL_DECLARE_FLAG(bool, slog_enable_statecheck);
ABSL_DECLARE_FLAG(int, slog_statecheck_interval_sec);

ABSL_DECLARE_FLAG(bool, slog_engine_force_remote_index);
ABSL_DECLARE_FLAG(float, slog_engine_prob_remote_index);
ABSL_DECLARE_FLAG(bool, slog_engine_enable_cache);
ABSL_DECLARE_FLAG(int, slog_engine_cache_cap_mb);
ABSL_DECLARE_FLAG(bool, slog_engine_propagate_auxdata);

ABSL_DECLARE_FLAG(int, slog_storage_cache_cap_mb);
ABSL_DECLARE_FLAG(std::string, slog_storage_backend);
ABSL_DECLARE_FLAG(int, slog_storage_bgthread_interval_ms);
ABSL_DECLARE_FLAG(size_t, slog_storage_max_live_entries);

// ABSL_DECLARE_FLAG(size_t, cc_reorder_batch_size);
// ABSL_DECLARE_FLAG(size_t, cc_reorder_quickselect);
// ABSL_DECLARE_FLAG(int64_t, cc_reorder_max_interval_us);
// ABSL_DECLARE_FLAG(int, cc_reorder_max_interval_rel);
ABSL_DECLARE_FLAG(bool, use_txn_engine);