#pragma once

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

ABSL_DECLARE_FLAG(bool, tcp_enable_nodelay);
ABSL_DECLARE_FLAG(bool, tcp_enable_keepalive);

ABSL_DECLARE_FLAG(int, io_uring_entries);
ABSL_DECLARE_FLAG(int, io_uring_fd_slots);
ABSL_DECLARE_FLAG(bool, io_uring_sqpoll);
ABSL_DECLARE_FLAG(int, io_uring_sq_thread_idle_ms);
ABSL_DECLARE_FLAG(int, io_uring_cq_nr_wait);
ABSL_DECLARE_FLAG(int, io_uring_cq_wait_timeout_us);

ABSL_DECLARE_FLAG(bool, enable_monitor);
ABSL_DECLARE_FLAG(bool, func_worker_use_engine_socket);
ABSL_DECLARE_FLAG(bool, use_fifo_for_nested_call);

ABSL_DECLARE_FLAG(bool, enable_shared_log);
ABSL_DECLARE_FLAG(int, shared_log_num_replicas);
ABSL_DECLARE_FLAG(int, shared_log_local_cut_interval_us);
ABSL_DECLARE_FLAG(int, shared_log_global_cut_interval_us);