// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata-owned metrics emitted through the shared recorder.

use crate::error::MetadataError;
use beryl_common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RpcErrorDetail, WorkerErrorKind,
};

pub(crate) const METADATA_UP: &str = "metadata_up";
pub(crate) const METADATA_BUILD_INFO: &str = "metadata_build_info";
pub(crate) const METADATA_ROOT_READY: &str = "metadata_root_ready";
pub(crate) const METADATA_RAFT_ROLE: &str = "metadata_raft_role";
pub(crate) const METADATA_RAFT_TERM: &str = "metadata_raft_term";
pub(crate) const METADATA_RAFT_LAST_APPLIED_INDEX: &str = "metadata_raft_last_applied_index";
pub(crate) const METADATA_RAFT_COMMITTED_INDEX: &str = "metadata_raft_committed_index";
pub(crate) const METADATA_RAFT_PROPOSALS_TOTAL: &str = "metadata_raft_proposals_total";
pub(crate) const METADATA_RAFT_PROPOSE_DURATION_SECONDS: &str = "metadata_raft_propose_duration_seconds";
pub(crate) const METADATA_RAFT_COMMAND_BYTES: &str = "metadata_raft_command_bytes";
pub(crate) const METADATA_RAFT_APPLY_TOTAL: &str = "metadata_raft_apply_total";
pub(crate) const METADATA_RAFT_APPLY_DURATION_SECONDS: &str = "metadata_raft_apply_duration_seconds";
pub(crate) const METADATA_RAFT_LOG_DURABLE_WRITE_BYTES_TOTAL: &str = "metadata_raft_log_durable_write_bytes_total";
pub(crate) const METADATA_RAFT_LOG_DURABLE_WRITE_DURATION_SECONDS: &str =
    "metadata_raft_log_durable_write_duration_seconds";
pub(crate) const METADATA_RAFT_SNAPSHOT_BYTES_TOTAL: &str = "metadata_raft_snapshot_bytes_total";
pub(crate) const METADATA_RAFT_SNAPSHOT_DURATION_SECONDS: &str = "metadata_raft_snapshot_duration_seconds";
pub(crate) const METADATA_RAFT_STORAGE_CLEANUP_TOTAL: &str = "metadata_raft_storage_cleanup_total";
pub(crate) const METADATA_RAFT_ACTIVE_GENERATION: &str = "metadata_raft_active_generation";
pub(crate) const METADATA_RAFT_AUTHORITY_COMMIT_DURATION_SECONDS: &str =
    "metadata_raft_authority_commit_duration_seconds";
pub(crate) const METADATA_RPC_REQUESTS_TOTAL: &str = "metadata_rpc_requests_total";
pub(crate) const METADATA_RPC_REQUEST_DURATION_SECONDS: &str = "metadata_rpc_request_duration_seconds";
pub(crate) const METADATA_WRITE_SESSIONS: &str = "metadata_write_sessions";
pub(crate) const METADATA_WRITE_SESSION_REJECTED_TOTAL: &str = "metadata_write_session_rejected_total";
pub(crate) const METADATA_WRITE_SESSION_EXPIRED_TOTAL: &str = "metadata_write_session_expired_total";
pub(crate) const METADATA_WRITE_TARGETS: &str = "metadata_write_targets";
pub(crate) const METADATA_WRITE_TARGET_REJECTED_TOTAL: &str = "metadata_write_target_rejected_total";
pub(crate) const METADATA_FS_OPS_TOTAL: &str = "metadata_fs_ops_total";
pub(crate) const METADATA_FS_OP_DURATION_SECONDS: &str = "metadata_fs_op_duration_seconds";
pub(crate) const METADATA_ROCKSDB_READS_TOTAL: &str = "metadata_rocksdb_reads_total";
pub(crate) const METADATA_WORKER_LIVE: &str = "metadata_worker_live";
pub(crate) const METADATA_WORKER_REGISTERED_TOTAL: &str = "metadata_worker_registered_total";
pub(crate) const METADATA_WORKER_REGISTRATION_DURATION_SECONDS: &str = "metadata_worker_registration_duration_seconds";
pub(crate) const METADATA_WORKER_HEARTBEAT_TOTAL: &str = "metadata_worker_heartbeat_total";
pub(crate) const METADATA_WORKER_HEARTBEAT_DURATION_SECONDS: &str = "metadata_worker_heartbeat_duration_seconds";
pub(crate) const METADATA_WORKER_HEARTBEAT_LAG_SECONDS: &str = "metadata_worker_heartbeat_lag_seconds";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_TOTAL: &str = "metadata_worker_block_report_total";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_DURATION_SECONDS: &str = "metadata_worker_block_report_duration_seconds";
pub(crate) const METADATA_WORKER_BLOCK_REPORT_BLOCKS_TOTAL: &str = "metadata_worker_block_report_blocks_total";
pub(crate) const METADATA_CLEANUP_SCANS_TOTAL: &str = "metadata_cleanup_scans_total";
pub(crate) const METADATA_CLEANUP_DECISIONS_TOTAL: &str = "metadata_cleanup_decisions_total";
pub(crate) const METADATA_CLEANUP_CANDIDATES: &str = "metadata_cleanup_candidates";
pub(crate) const METADATA_CLEANUP_READY_CANDIDATES: &str = "metadata_cleanup_ready_candidates";
pub(crate) const METADATA_CLEANUP_OLDEST_CANDIDATE_AGE_SECONDS: &str = "metadata_cleanup_oldest_candidate_age_seconds";
pub(crate) const METADATA_CLEANUP_ANOMALIES_TOTAL: &str = "metadata_cleanup_anomalies_total";
pub(crate) const METADATA_CLEANUP_COMMANDS_TOTAL: &str = "metadata_cleanup_commands_total";
pub(crate) const METADATA_CLEANUP_RETRIES_TOTAL: &str = "metadata_cleanup_retries_total";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_PASSES_TOTAL: &str = "metadata_detached_root_reclaim_passes_total";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_CANDIDATES: &str = "metadata_detached_root_reclaim_candidates";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_BACKLOG_TRUNCATED: &str =
    "metadata_detached_root_reclaim_backlog_truncated";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_OLDEST_AGE_SECONDS: &str =
    "metadata_detached_root_reclaim_oldest_age_seconds";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_ENTRIES_TOTAL: &str = "metadata_detached_root_reclaim_entries_total";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_LOGICAL_BYTES_TOTAL: &str =
    "metadata_detached_root_reclaim_logical_bytes_total";
pub(crate) const METADATA_DETACHED_ROOT_RECLAIM_DURATION_SECONDS: &str =
    "metadata_detached_root_reclaim_duration_seconds";

pub(crate) fn record_metadata_started(service: &str, version: &str) {
    metrics::gauge!(METADATA_UP).set(1.0);
    metrics::gauge!(
        METADATA_BUILD_INFO,
        "service" => service.to_string(),
        "version" => version.to_string()
    )
    .set(1.0);
}

pub(crate) fn record_root_ready(ready: bool) {
    metrics::gauge!(METADATA_ROOT_READY).set(if ready { 1.0 } else { 0.0 });
}

pub(crate) fn record_raft_role(role: &str) {
    for known_role in ["leader", "follower", "candidate", "learner", "shutdown", "unknown"] {
        metrics::gauge!(METADATA_RAFT_ROLE, "role" => known_role).set(if known_role == role { 1.0 } else { 0.0 });
    }
}

pub(crate) fn record_raft_term(term: u64) {
    metrics::gauge!(METADATA_RAFT_TERM).set(term as f64);
}

pub(crate) fn record_raft_indexes(last_applied: Option<u64>, committed: Option<u64>) {
    if let Some(last_applied) = last_applied {
        metrics::gauge!(METADATA_RAFT_LAST_APPLIED_INDEX).set(last_applied as f64);
    }
    if let Some(committed) = committed {
        metrics::gauge!(METADATA_RAFT_COMMITTED_INDEX).set(committed as f64);
    }
}

pub(crate) fn record_raft_proposal(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_RAFT_PROPOSALS_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_RAFT_PROPOSE_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

/// Publish the exact opening and active write-session counts.
pub(crate) fn set_write_sessions(opening: usize, active: usize) {
    metrics::gauge!(METADATA_WRITE_SESSIONS, "state" => "opening").set(opening as f64);
    metrics::gauge!(METADATA_WRITE_SESSIONS, "state" => "active").set(active as f64);
}

/// Count immediate write-session capacity rejection by stable limit scope.
pub(crate) fn record_write_session_rejected(limit: &'static str) {
    metrics::counter!(METADATA_WRITE_SESSION_REJECTED_TOTAL, "limit" => limit).increment(1);
}

/// Count one leader-local opening or active session retired after expiry.
pub(crate) fn record_write_session_expired() {
    metrics::counter!(METADATA_WRITE_SESSION_EXPIRED_TOTAL).increment(1);
}

/// Publish the exact pending and issued write-target occupancy.
pub(crate) fn set_write_targets(pending: usize, issued: usize) {
    metrics::gauge!(METADATA_WRITE_TARGETS, "state" => "pending").set(pending as f64);
    metrics::gauge!(METADATA_WRITE_TARGETS, "state" => "issued").set(issued as f64);
}

/// Count one write-target rejection by stable limit scope.
pub(crate) fn record_write_target_rejected(limit: &'static str) {
    metrics::counter!(METADATA_WRITE_TARGET_REJECTED_TOTAL, "limit" => limit).increment(1);
}

/// Records serialized command size with one stable operation label.
pub(crate) fn record_raft_command_bytes(operation: &'static str, bytes: usize) {
    metrics::histogram!(METADATA_RAFT_COMMAND_BYTES, "operation" => operation).record(bytes as f64);
}

pub(crate) fn record_raft_apply(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_RAFT_APPLY_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_RAFT_APPLY_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_raft_log_durable_write(status: &'static str, bytes: usize, duration_seconds: f64) {
    metrics::counter!(METADATA_RAFT_LOG_DURABLE_WRITE_BYTES_TOTAL, "status" => status).increment(bytes as u64);
    metrics::histogram!(METADATA_RAFT_LOG_DURABLE_WRITE_DURATION_SECONDS, "status" => status).record(duration_seconds);
}

pub(crate) fn record_raft_snapshot(
    operation: &'static str,
    stage: &'static str,
    status: &'static str,
    bytes: u64,
    duration_seconds: f64,
) {
    metrics::counter!(
        METADATA_RAFT_SNAPSHOT_BYTES_TOTAL,
        "operation" => operation,
        "stage" => stage,
        "status" => status
    )
    .increment(bytes);
    metrics::histogram!(
        METADATA_RAFT_SNAPSHOT_DURATION_SECONDS,
        "operation" => operation,
        "stage" => stage,
        "status" => status
    )
    .record(duration_seconds);
}

pub(crate) fn record_raft_storage_cleanup(kind: &'static str, count: usize) {
    metrics::counter!(METADATA_RAFT_STORAGE_CLEANUP_TOTAL, "kind" => kind).increment(count as u64);
}

pub(crate) fn record_raft_active_generation(generation: u64) {
    metrics::gauge!(METADATA_RAFT_ACTIVE_GENERATION).set(generation as f64);
}

pub(crate) fn record_raft_authority_commit(status: &'static str, duration_seconds: f64) {
    metrics::histogram!(METADATA_RAFT_AUTHORITY_COMMIT_DURATION_SECONDS, "status" => status).record(duration_seconds);
}

pub(crate) fn record_rpc_request(service: &str, method: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_RPC_REQUESTS_TOTAL,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_RPC_REQUEST_DURATION_SECONDS,
        "service" => service.to_string(),
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_fs_op(operation: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_FS_OPS_TOTAL,
        "operation" => operation.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_FS_OP_DURATION_SECONDS,
        "operation" => operation.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_rocksdb_read(kind: &'static str) {
    metrics::counter!(METADATA_ROCKSDB_READS_TOTAL, "kind" => kind).increment(1);
}

pub(crate) fn set_worker_live(count: usize) {
    metrics::gauge!(METADATA_WORKER_LIVE).set(count as f64);
}

pub(crate) fn record_worker_registration(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_WORKER_REGISTERED_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_WORKER_REGISTRATION_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_worker_heartbeat(status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_WORKER_HEARTBEAT_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_WORKER_HEARTBEAT_DURATION_SECONDS,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_worker_heartbeat_lag(lag_seconds: f64) {
    metrics::histogram!(METADATA_WORKER_HEARTBEAT_LAG_SECONDS).record(lag_seconds);
}

pub(crate) fn record_worker_block_report(kind: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        METADATA_WORKER_BLOCK_REPORT_TOTAL,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        METADATA_WORKER_BLOCK_REPORT_DURATION_SECONDS,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_worker_block_report_blocks(change: &str, count: usize) {
    if count == 0 {
        return;
    }
    metrics::counter!(METADATA_WORKER_BLOCK_REPORT_BLOCKS_TOTAL, "change" => change.to_string())
        .increment(count as u64);
}

pub(crate) fn record_cleanup_scan(result: &'static str) {
    metrics::counter!(METADATA_CLEANUP_SCANS_TOTAL, "result" => result).increment(1);
}

pub(crate) fn record_cleanup_decision(decision: &'static str) {
    metrics::counter!(METADATA_CLEANUP_DECISIONS_TOTAL, "decision" => decision).increment(1);
}

pub(crate) fn set_cleanup_candidates(total: usize, ready: usize, oldest_age_seconds: f64) {
    metrics::gauge!(METADATA_CLEANUP_CANDIDATES).set(total as f64);
    metrics::gauge!(METADATA_CLEANUP_READY_CANDIDATES).set(ready as f64);
    metrics::gauge!(METADATA_CLEANUP_OLDEST_CANDIDATE_AGE_SECONDS).set(oldest_age_seconds);
}

pub(crate) fn record_cleanup_anomaly(kind: &'static str) {
    metrics::counter!(METADATA_CLEANUP_ANOMALIES_TOTAL, "kind" => kind).increment(1);
}

pub(crate) fn record_cleanup_command() {
    metrics::counter!(METADATA_CLEANUP_COMMANDS_TOTAL).increment(1);
}

pub(crate) fn record_cleanup_retry() {
    metrics::counter!(METADATA_CLEANUP_RETRIES_TOTAL).increment(1);
}

pub(crate) fn set_detached_root_reclaim_candidates(selected: usize, backlog_truncated: bool, oldest_age_seconds: f64) {
    metrics::gauge!(METADATA_DETACHED_ROOT_RECLAIM_CANDIDATES).set(selected as f64);
    metrics::gauge!(METADATA_DETACHED_ROOT_RECLAIM_BACKLOG_TRUNCATED).set(if backlog_truncated { 1.0 } else { 0.0 });
    metrics::gauge!(METADATA_DETACHED_ROOT_RECLAIM_OLDEST_AGE_SECONDS).set(oldest_age_seconds);
}

pub(crate) fn record_detached_root_reclaim_pass(
    result: &'static str,
    processed_entries: u32,
    logical_batch_bytes: u32,
    duration_seconds: f64,
) {
    metrics::counter!(METADATA_DETACHED_ROOT_RECLAIM_PASSES_TOTAL, "result" => result).increment(1);
    metrics::histogram!(METADATA_DETACHED_ROOT_RECLAIM_DURATION_SECONDS, "result" => result).record(duration_seconds);
    if processed_entries > 0 {
        metrics::counter!(METADATA_DETACHED_ROOT_RECLAIM_ENTRIES_TOTAL).increment(u64::from(processed_entries));
    }
    if logical_batch_bytes > 0 {
        metrics::counter!(METADATA_DETACHED_ROOT_RECLAIM_LOGICAL_BYTES_TOTAL).increment(u64::from(logical_batch_bytes));
    }
}

pub(crate) fn metadata_error_kind(error: &MetadataError) -> &'static str {
    match error {
        MetadataError::NotFound(_) => "not_found",
        MetadataError::AlreadyExists(_) => "already_exists",
        MetadataError::InvalidArgument(_) => "invalid_argument",
        MetadataError::NotDir(_) => "not_dir",
        MetadataError::IsDir(_) => "is_dir",
        MetadataError::DirectoryNotEmpty(_) => "directory_not_empty",
        MetadataError::CrossMountRename(_) => "cross_mount_rename",
        MetadataError::PermissionDenied(_) => "permission_denied",
        MetadataError::NotSupported(_) => "not_supported",
        MetadataError::Busy(_) => "busy",
        MetadataError::ActiveWorkerConflict(_) => "active_worker_conflict",
        MetadataError::Again(_) => "again",
        MetadataError::ResourceExhausted(_) => "resource_exhausted",
        MetadataError::WriteSessionLimitExceeded(_) => "write_session_limit_exceeded",
        MetadataError::GlobalWriteTargetLimitExceeded(_) => "global_write_target_limit_exceeded",
        MetadataError::LeaseFenced { .. } => "lease_fenced",
        MetadataError::LeaderChanged(_) => "not_leader",
        MetadataError::EpochMismatch { .. } => "epoch_mismatch",
        MetadataError::MountEpochMismatch { .. } => "mount_epoch_mismatch",
        MetadataError::RoutingStale(_) => "route_epoch_mismatch",
        MetadataError::StaleState(_) => "stale_state",
        MetadataError::FullReportRequired(_) => "full_report_required",
        MetadataError::Internal(_) => "internal",
        MetadataError::ServiceUnavailable(_) => "unavailable",
    }
}

pub(crate) fn rpc_error_kind(error: &RpcErrorDetail) -> &'static str {
    error_kind_label(error.kind)
}

fn error_kind_label(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader) => "invalid_header",
        ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument) => "invalid_argument",
        ErrorKind::Protocol(ProtocolErrorKind::PermissionDenied) => "permission_denied",
        ErrorKind::Protocol(ProtocolErrorKind::Unsupported) => "unsupported",
        ErrorKind::Metadata(MetadataErrorKind::NotFound) => "not_found",
        ErrorKind::Metadata(MetadataErrorKind::AlreadyExists) => "already_exists",
        ErrorKind::Metadata(MetadataErrorKind::NotDirectory) => "not_directory",
        ErrorKind::Metadata(MetadataErrorKind::IsDirectory) => "is_directory",
        ErrorKind::Metadata(MetadataErrorKind::DirectoryNotEmpty) => "directory_not_empty",
        ErrorKind::Metadata(MetadataErrorKind::CrossMountRename) => "cross_mount_rename",
        ErrorKind::Metadata(MetadataErrorKind::Busy) => "busy",
        ErrorKind::Metadata(MetadataErrorKind::Conflict) => "conflict",
        ErrorKind::Metadata(MetadataErrorKind::NotLeader) => "not_leader",
        ErrorKind::Metadata(MetadataErrorKind::StaleState) => "stale_state",
        ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch) => "mount_epoch_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch) => "route_epoch_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch) => "owner_group_mismatch",
        ErrorKind::Metadata(MetadataErrorKind::GroupMismatch) => "group_mismatch",
        ErrorKind::Worker(WorkerErrorKind::NotRegistered) => "worker_not_registered",
        ErrorKind::Worker(WorkerErrorKind::RunMismatch) => "worker_run_mismatch",
        ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch) => "worker_descriptor_mismatch",
        ErrorKind::Worker(WorkerErrorKind::FullReportRequired) => "full_report_required",
        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable) => "block_location_unavailable",
        ErrorKind::Metadata(MetadataErrorKind::Fencing) => "fencing",
        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid) => "session_invalid",
        ErrorKind::Metadata(MetadataErrorKind::SessionExpired) => "session_expired",
        ErrorKind::Metadata(MetadataErrorKind::EpochMismatch) => "epoch_mismatch",
        ErrorKind::Internal(InternalErrorKind::NodeUnavailable) => "node_unavailable",
        ErrorKind::Internal(InternalErrorKind::Timeout) => "timeout",
        ErrorKind::Internal(InternalErrorKind::ResourceExhausted) => "resource_exhausted",
        ErrorKind::Internal(InternalErrorKind::Cancelled) => "cancelled",
        ErrorKind::Internal(InternalErrorKind::Corrupt) => "corrupt",
        ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted) => "resource_exhausted",
        ErrorKind::Worker(kind) => worker_error_kind(kind),
        ErrorKind::Protocol(kind) => protocol_error_kind(kind),
        ErrorKind::Internal(_) => "internal",
    }
}

fn worker_error_kind(kind: WorkerErrorKind) -> &'static str {
    match kind {
        WorkerErrorKind::NotRegistered => "worker_not_registered",
        WorkerErrorKind::RunMismatch => "worker_run_mismatch",
        WorkerErrorKind::DescriptorMismatch => "worker_descriptor_mismatch",
        WorkerErrorKind::FullReportRequired => "full_report_required",
        WorkerErrorKind::BlockLocationUnavailable => "block_location_unavailable",
        WorkerErrorKind::NodeUnavailable => "worker_node_unavailable",
        WorkerErrorKind::Timeout => "worker_timeout",
        WorkerErrorKind::ResourceExhausted => "worker_resource_exhausted",
        WorkerErrorKind::Conflict => "worker_conflict",
        WorkerErrorKind::Corrupt => "worker_corrupt",
        WorkerErrorKind::Fencing => "worker_fencing",
        WorkerErrorKind::Cancelled => "worker_cancelled",
        WorkerErrorKind::Io => "worker_io",
        WorkerErrorKind::NotFound => "worker_not_found",
    }
}

fn protocol_error_kind(kind: ProtocolErrorKind) -> &'static str {
    match kind {
        ProtocolErrorKind::InvalidHeader => "invalid_header",
        ProtocolErrorKind::InvalidArgument => "invalid_argument",
        ProtocolErrorKind::PermissionDenied => "permission_denied",
        ProtocolErrorKind::Unsupported => "unsupported",
        ProtocolErrorKind::Cancelled => "cancelled",
        ProtocolErrorKind::Corrupt => "corrupt",
    }
}
