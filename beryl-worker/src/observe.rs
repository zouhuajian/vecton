// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-owned metrics emitted through the shared recorder.

use crate::error::WorkerError;
use crate::store::dirs::StoreReport;
use beryl_common::error::rpc::{ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, WorkerErrorKind};

pub(crate) const WORKER_UP: &str = "worker_up";
pub(crate) const WORKER_BUILD_INFO: &str = "worker_build_info";
pub(crate) const WORKER_REGISTERED: &str = "worker_registered";
pub(crate) const WORKER_METADATA_RPC_TOTAL: &str = "worker_metadata_rpc_total";
pub(crate) const WORKER_METADATA_RPC_DURATION_SECONDS: &str = "worker_metadata_rpc_duration_seconds";
pub(crate) const WORKER_HEARTBEAT_SENT_TOTAL: &str = "worker_heartbeat_sent_total";
pub(crate) const WORKER_BLOCK_REPORT_SENT_TOTAL: &str = "worker_block_report_sent_total";
pub(crate) const WORKER_BLOCK_REPORT_DURATION_SECONDS: &str = "worker_block_report_duration_seconds";
pub(crate) const WORKER_STORE_CAPACITY_BYTES: &str = "worker_store_capacity_bytes";
pub(crate) const WORKER_STORE_WRITABLE: &str = "worker_store_writable";
pub(crate) const WORKER_STORE_BLOCKS: &str = "worker_store_blocks";
pub(crate) const WORKER_STORE_IO_BYTES: &str = "worker_store_io_bytes";
pub(crate) const WORKER_STORE_IO_DURATION_SECONDS: &str = "worker_store_io_duration_seconds";
pub(crate) const WORKER_DATA_RPC_TOTAL: &str = "worker_data_rpc_total";
pub(crate) const WORKER_DATA_RPC_DURATION_SECONDS: &str = "worker_data_rpc_duration_seconds";
pub(crate) const WORKER_STREAM_OPEN_TOTAL: &str = "worker_stream_open_total";
pub(crate) const WORKER_STREAM_INFLIGHT: &str = "worker_stream_inflight";
pub(crate) const WORKER_DATA_RPC_CAPACITY_REJECTIONS_TOTAL: &str = "worker_data_rpc_capacity_rejections_total";
pub(crate) const WORKER_STREAM_FRAME_BYTES: &str = "worker_stream_frame_bytes";
pub(crate) const WORKER_STREAM_FRAMES_TOTAL: &str = "worker_stream_frames_total";
pub(crate) const WORKER_CLEANUP_QUEUE_DEPTH: &str = "worker_cleanup_queue_depth";
pub(crate) const WORKER_CLEANUP_RECLAIMING_COUNT: &str = "worker_cleanup_reclaiming_count";
pub(crate) const WORKER_CLEANUP_ENQUEUE_TOTAL: &str = "worker_cleanup_enqueue_total";
pub(crate) const WORKER_CLEANUP_RESULT_TOTAL: &str = "worker_cleanup_result_total";

pub fn record_worker_started(service: &str, version: &str) {
    metrics::gauge!(WORKER_UP).set(1.0);
    metrics::gauge!(
        WORKER_BUILD_INFO,
        "service" => service.to_string(),
        "version" => version.to_string()
    )
    .set(1.0);
}

pub fn set_worker_registered(registered: bool) {
    metrics::gauge!(WORKER_REGISTERED).set(if registered { 1.0 } else { 0.0 });
}

pub(crate) fn record_metadata_rpc(method: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        WORKER_METADATA_RPC_TOTAL,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        WORKER_METADATA_RPC_DURATION_SECONDS,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_heartbeat_sent(status: &str, error_kind: &str) {
    metrics::counter!(
        WORKER_HEARTBEAT_SENT_TOTAL,
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn record_block_report_sent(kind: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        WORKER_BLOCK_REPORT_SENT_TOTAL,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        WORKER_BLOCK_REPORT_DURATION_SECONDS,
        "kind" => kind.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn set_cleanup_queue_depth(count: usize) {
    metrics::gauge!(WORKER_CLEANUP_QUEUE_DEPTH).set(count as f64);
}

pub(crate) fn set_cleanup_reclaiming(count: usize) {
    metrics::gauge!(WORKER_CLEANUP_RECLAIMING_COUNT).set(count as f64);
}

pub(crate) fn record_cleanup_enqueue(result: &str) {
    metrics::counter!(WORKER_CLEANUP_ENQUEUE_TOTAL, "result" => result.to_string()).increment(1);
}

pub(crate) fn record_cleanup_result(result: &str) {
    metrics::counter!(WORKER_CLEANUP_RESULT_TOTAL, "result" => result.to_string()).increment(1);
}

pub(crate) fn record_store_capacity(dir_id: &str, kind: &str, bytes: u64) {
    metrics::gauge!(
        WORKER_STORE_CAPACITY_BYTES,
        "dir_id" => dir_id.to_string(),
        "kind" => kind.to_string()
    )
    .set(bytes as f64);
}

pub(crate) fn record_store_writable(dir_id: &str, writable: bool) {
    metrics::gauge!(WORKER_STORE_WRITABLE, "dir_id" => dir_id.to_string()).set(if writable { 1.0 } else { 0.0 });
}

pub(crate) fn record_store_blocks(dir_id: &str, count: u64) {
    metrics::gauge!(WORKER_STORE_BLOCKS, "dir_id" => dir_id.to_string()).set(count as f64);
}

pub(crate) fn record_store_report(report: &StoreReport) {
    for dir in &report.dirs {
        record_store_capacity(&dir.id, "total", dir.capacity_bytes);
        record_store_capacity(&dir.id, "used", dir.used_bytes);
        record_store_capacity(&dir.id, "available", dir.free_bytes);
        record_store_writable(&dir.id, dir.writable);
        record_store_blocks(&dir.id, dir.block_count);
    }
}

pub(crate) fn record_store_io(operation: &str, status: &str, error_kind: &str, bytes: u64, duration_seconds: f64) {
    if bytes > 0 {
        metrics::counter!(WORKER_STORE_IO_BYTES, "operation" => operation.to_string()).increment(bytes);
    }
    metrics::histogram!(
        WORKER_STORE_IO_DURATION_SECONDS,
        "operation" => operation.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_data_rpc(method: &str, status: &str, error_kind: &str, duration_seconds: f64) {
    metrics::counter!(
        WORKER_DATA_RPC_TOTAL,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
    metrics::histogram!(
        WORKER_DATA_RPC_DURATION_SECONDS,
        "method" => method.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .record(duration_seconds);
}

pub(crate) fn record_stream_open(mode: &str, status: &str, error_kind: &str) {
    metrics::counter!(
        WORKER_STREAM_OPEN_TOTAL,
        "mode" => mode.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn increment_stream_inflight(mode: &str) {
    metrics::gauge!(WORKER_STREAM_INFLIGHT, "mode" => mode.to_string()).increment(1.0);
}

pub(crate) fn decrement_stream_inflight(mode: &str) {
    metrics::gauge!(WORKER_STREAM_INFLIGHT, "mode" => mode.to_string()).decrement(1.0);
}

pub(crate) fn record_data_rpc_capacity_rejection(mode: &str) {
    metrics::counter!(WORKER_DATA_RPC_CAPACITY_REJECTIONS_TOTAL, "mode" => mode.to_string()).increment(1);
}

pub(crate) fn record_stream_frame(mode: &str, status: &str, error_kind: &str, bytes: u64) {
    if bytes > 0 {
        metrics::counter!(WORKER_STREAM_FRAME_BYTES, "mode" => mode.to_string()).increment(bytes);
    }
    metrics::counter!(
        WORKER_STREAM_FRAMES_TOTAL,
        "mode" => mode.to_string(),
        "status" => status.to_string(),
        "error_kind" => error_kind.to_string()
    )
    .increment(1);
}

pub(crate) fn worker_error_kind(error: &WorkerError) -> &'static str {
    match error {
        WorkerError::Timeout(_) => "timeout",
        WorkerError::Unavailable(_) => "unavailable",
        WorkerError::DiskError(_) => "disk_error",
        WorkerError::Cancelled(_) => "cancelled",
        WorkerError::InvalidArgument(_) => "invalid_argument",
        WorkerError::NotFound(_) => "not_found",
        WorkerError::Corrupt(_) => "corrupt",
        WorkerError::RefreshMetadata { kind, .. } => error_kind_label(*kind),
        WorkerError::PermissionDenied(_) => "permission_denied",
        WorkerError::Internal(_) => "internal",
        WorkerError::ResourceExhausted(_) => "resource_exhausted",
    }
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
        ErrorKind::Worker(kind) => rpc_worker_error_kind(kind),
        ErrorKind::Protocol(kind) => protocol_error_kind(kind),
        ErrorKind::Internal(_) => "internal",
    }
}

fn rpc_worker_error_kind(kind: WorkerErrorKind) -> &'static str {
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
