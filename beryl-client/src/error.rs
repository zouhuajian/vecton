// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stable public client errors and internal failure evidence.

use crate::runtime::OperationContext;
use beryl_common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail,
    WorkerEndpointHint, WorkerErrorKind,
};
use beryl_common::header::{
    HEADER_PRE_HANDLER_REJECTION, HEADER_WORKER_DATA_REJECTION, PRE_HANDLER_REJECTION_RPC_CONCURRENCY,
    WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT,
};
use beryl_common::{CommonError, CommonErrorKind};
use beryl_proto::common::WorkerEndpointInfoProto;
use beryl_types::{CallId, ContentGeneration, GroupName};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::time::Duration;
use tonic::{Code, Status};

/// Stable, caller-visible category for a client failure.
///
/// Retry authorization is intentionally not part of this enum. Whether a
/// request may be replayed also depends on the operation and side-effect
/// evidence retained internally by the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientErrorKind {
    /// A caller-provided argument is invalid.
    InvalidArgument,
    /// Client configuration is invalid.
    InvalidConfiguration,
    /// The requested namespace or storage object does not exist.
    NotFound,
    /// The requested namespace object already exists.
    AlreadyExists,
    /// A path component expected to be a directory is not one.
    NotDirectory,
    /// A file operation targeted a directory.
    IsDirectory,
    /// A non-recursive directory operation targeted a nonempty directory.
    DirectoryNotEmpty,
    /// The operation crosses an unsupported mount boundary.
    CrossMount,
    /// The target is busy with another authoritative operation.
    Busy,
    /// The request conflicts with current authoritative state.
    Conflict,
    /// The caller is not authorized.
    PermissionDenied,
    /// A bounded server or client resource is exhausted.
    ResourceExhausted,
    /// The requested operation or protocol is unsupported.
    Unsupported,
    /// An opened file or write session no longer matches current authority.
    StaleHandle,
    /// A write session is no longer current.
    SessionInvalid,
    /// A write session lease expired.
    SessionExpired,
    /// A fencing token or epoch was rejected.
    Fenced,
    /// The operation exceeded its deadline.
    Timeout,
    /// The operation was cancelled.
    Cancelled,
    /// The required Metadata or Worker endpoint is unavailable.
    Unavailable,
    /// Worker or local storage IO failed.
    Io,
    /// An exact read reached the opened file's end too early.
    UnexpectedEof,
    /// Returned data is corrupt.
    CorruptData,
    /// A response violated the validated wire or domain contract.
    InvalidResponse,
    /// An internal invariant or unexpected implementation failure occurred.
    Internal,
}

impl ClientErrorKind {
    /// Returns the low-cardinality label used by client metrics.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::InvalidConfiguration => "invalid_configuration",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::NotDirectory => "not_directory",
            Self::IsDirectory => "is_directory",
            Self::DirectoryNotEmpty => "directory_not_empty",
            Self::CrossMount => "cross_mount",
            Self::Busy => "busy",
            Self::Conflict => "conflict",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Unsupported => "unsupported",
            Self::StaleHandle => "stale_handle",
            Self::SessionInvalid => "session_invalid",
            Self::SessionExpired => "session_expired",
            Self::Fenced => "fencing",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Unavailable => "unavailable",
            Self::Io => "io",
            Self::UnexpectedEof => "unexpected_eof",
            Self::CorruptData => "corrupt_data",
            Self::InvalidResponse => "invalid_response",
            Self::Internal => "internal",
        }
    }
}

/// Endpoint hint preserved from a validated structured RPC failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EndpointHint {
    pub(crate) worker_id: u64,
    pub(crate) endpoint: String,
}

impl From<WorkerEndpointHint> for EndpointHint {
    fn from(value: WorkerEndpointHint) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
        }
    }
}

impl From<WorkerEndpointInfoProto> for EndpointHint {
    fn from(value: WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
        }
    }
}

/// Authority hints preserved from a validated structured RPC failure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RefreshHint {
    pub(crate) leader_endpoint: Option<String>,
    pub(crate) group_name: Option<GroupName>,
    pub(crate) mount_prefix: Option<String>,
    pub(crate) route_epoch: Option<u64>,
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) endpoint_hint: Option<EndpointHint>,
    pub(crate) worker_endpoints: Vec<EndpointHint>,
    pub(crate) worker_resolve_required: bool,
}

#[derive(Clone, Debug)]
enum FailureDetail {
    Local,
    Remote {
        rpc_error: Box<RpcErrorDetail>,
        hint: Box<RefreshHint>,
    },
    Transport {
        code: Code,
        definitely_before_side_effect: bool,
    },
}

/// Error returned by the Rust native Beryl client.
///
/// `kind` describes what failed while `is_outcome_unknown` independently
/// reports whether a side effect may have completed. Callers must not infer
/// replay safety from the category alone.
#[derive(Clone, Debug)]
pub struct ClientError {
    kind: ClientErrorKind,
    operation: Option<&'static str>,
    call_id: Option<CallId>,
    outcome_unknown: bool,
    invalid_success_response: bool,
    retry_after: Option<Duration>,
    message: String,
    detail: FailureDetail,
}

impl ClientError {
    fn local(kind: ClientErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            operation: None,
            call_id: None,
            outcome_unknown: false,
            invalid_success_response: false,
            retry_after: None,
            message: message.into(),
            detail: FailureDetail::Local,
        }
    }

    /// Returns the stable caller-visible failure category.
    pub const fn kind(&self) -> ClientErrorKind {
        self.kind
    }

    /// Returns the exact RPC operation name when the failure reached that boundary.
    pub const fn operation(&self) -> Option<&'static str> {
        self.operation
    }

    /// Returns the stable call identity associated with the failing RPC intent.
    pub const fn call_id(&self) -> Option<CallId> {
        self.call_id
    }

    /// Returns whether a side effect may have completed without a validated result.
    pub const fn is_outcome_unknown(&self) -> bool {
        self.outcome_unknown
    }

    /// Returns a validated server retry delay hint, when one was supplied.
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Returns the diagnostic message without requiring callers to parse Display output.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::InvalidArgument, message)
    }

    pub(crate) fn unexpected_eof(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::UnexpectedEof, message)
    }

    pub(crate) fn invalid_configuration(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::InvalidConfiguration, message)
    }

    pub(crate) fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::ResourceExhausted, message)
    }

    pub(crate) fn invalid_layout(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::InvalidResponse, message)
    }

    pub(crate) fn metadata(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::Internal, message)
    }

    pub(crate) fn worker(message: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::Io, message)
    }

    pub(crate) fn stale_handle(reason: impl Into<String>) -> Self {
        Self::local(ClientErrorKind::StaleHandle, reason)
    }

    /// Report stale content authority without implying historical reads are supported.
    pub(crate) fn generation_mismatch(expected: ContentGeneration, actual: ContentGeneration) -> Self {
        Self::local(
            ClientErrorKind::StaleHandle,
            format!("generation mismatch: expected {expected}, got {actual}"),
        )
    }

    pub(crate) fn invalid_response(operation: &'static str, reason: impl Into<String>) -> Self {
        let mut error = Self::local(ClientErrorKind::InvalidResponse, reason);
        error.operation = Some(operation);
        error.invalid_success_response = true;
        error
    }

    pub(crate) fn malformed_response(reason: impl Into<String>) -> Self {
        let mut error = Self::local(ClientErrorKind::InvalidResponse, reason);
        error.invalid_success_response = true;
        error
    }

    pub(crate) fn unknown_outcome(message: impl Into<String>) -> Self {
        let mut error = Self::local(ClientErrorKind::Internal, message);
        error.outcome_unknown = true;
        error
    }

    pub(crate) fn session_expired_unknown(message: impl Into<String>) -> Self {
        let mut error = Self::local(ClientErrorKind::SessionExpired, message);
        error.outcome_unknown = true;
        error
    }

    /// Attaches the immutable operation identity without replacing an earlier one.
    pub(crate) fn with_operation_context(mut self, operation: &OperationContext) -> Self {
        self.operation.get_or_insert(operation.operation_name());
        self.call_id.get_or_insert(operation.call_id());
        self
    }

    /// Marks an ambiguous side-effect result while preserving the original failure fact.
    pub(crate) fn with_unknown_outcome(mut self, operation: &OperationContext, message: impl Into<String>) -> Self {
        self.operation = Some(operation.operation_name());
        self.call_id = Some(operation.call_id());
        self.outcome_unknown = true;
        self.message = message.into();
        self
    }

    /// Marks ambiguity when only the operation name, not its full context, is retained.
    pub(crate) fn with_unknown_outcome_name(mut self, operation: &'static str, message: impl Into<String>) -> Self {
        self.operation.get_or_insert(operation);
        self.outcome_unknown = true;
        self.message = message.into();
        self
    }

    /// Preserves validated server recovery evidence for internal retry decisions.
    pub(crate) fn from_remote(rpc_error: RpcErrorDetail, hint: RefreshHint) -> Self {
        let retry_after = match &rpc_error.recovery {
            RecoveryAction::Retry { after_ms } => after_ms.map(Duration::from_millis),
            _ => None,
        };
        Self {
            kind: client_kind_from_rpc(rpc_error.kind),
            operation: None,
            call_id: None,
            outcome_unknown: false,
            invalid_success_response: false,
            retry_after,
            message: rpc_error.message.clone(),
            detail: FailureDetail::Remote {
                rpc_error: Box::new(rpc_error),
                hint: Box::new(hint),
            },
        }
    }

    pub(crate) fn remote_error(&self) -> Option<&RpcErrorDetail> {
        match &self.detail {
            FailureDetail::Remote { rpc_error, .. } => Some(rpc_error),
            _ => None,
        }
    }

    pub(crate) fn refresh_hint(&self) -> Option<&RefreshHint> {
        match &self.detail {
            FailureDetail::Remote { hint, .. } => Some(hint),
            _ => None,
        }
    }

    pub(crate) fn transport_code(&self) -> Option<Code> {
        match &self.detail {
            FailureDetail::Transport { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Returns whether the failure came from an unstructured transport status.
    pub(crate) fn is_transport_failure(&self) -> bool {
        self.transport_code().is_some()
    }

    pub(crate) fn is_retryable_transport(&self) -> bool {
        matches!(
            self.transport_code(),
            Some(Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted)
        )
    }

    pub(crate) const fn is_invalid_success_response(&self) -> bool {
        self.invalid_success_response
    }

    pub(crate) fn is_definitely_before_side_effect(&self) -> bool {
        matches!(
            &self.detail,
            FailureDetail::Transport {
                definitely_before_side_effect: true,
                ..
            }
        )
    }

    pub(crate) fn classification_label(&self) -> &'static str {
        if self.outcome_unknown {
            return "unknown_outcome";
        }
        if self.is_definitely_before_side_effect() {
            return "server_retry";
        }
        if self.is_retryable_transport() {
            return "retryable_transport";
        }
        match self.remote_error().map(|error| &error.recovery) {
            Some(RecoveryAction::Retry { .. }) => "server_retry",
            Some(RecoveryAction::RefreshMetadata { .. }) => "refresh_metadata",
            _ => self.kind.label(),
        }
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match (self.operation, self.call_id) {
            (Some(operation), Some(call_id)) => write!(f, "{operation} [{call_id}]: {}", self.message),
            (Some(operation), None) => write!(f, "{operation}: {}", self.message),
            _ => f.write_str(&self.message),
        }
    }
}

impl Error for ClientError {}

/// Result type alias for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

pub(crate) fn side_effect_response_body_mismatch(operation: &'static str, detail: impl Display) -> ClientError {
    let mut error =
        ClientError::invalid_response(operation, format!("response body mismatch after OK header: {detail}"));
    error.outcome_unknown = true;
    error
}

pub(crate) fn invalid_response(operation: &'static str, reason: impl Into<String>) -> ClientError {
    ClientError::invalid_response(operation, reason)
}

impl From<CommonError> for ClientError {
    fn from(error: CommonError) -> Self {
        let kind = match error.kind {
            CommonErrorKind::Timeout => ClientErrorKind::Timeout,
            CommonErrorKind::Overloaded => ClientErrorKind::ResourceExhausted,
            CommonErrorKind::NotFound => ClientErrorKind::NotFound,
            CommonErrorKind::PermissionDenied => ClientErrorKind::PermissionDenied,
            CommonErrorKind::InvalidArgument => ClientErrorKind::InvalidArgument,
            CommonErrorKind::Io => ClientErrorKind::Io,
            CommonErrorKind::Internal => ClientErrorKind::Internal,
        };
        Self::local(kind, error.to_string())
    }
}

impl From<Status> for ClientError {
    fn from(status: Status) -> Self {
        let definitely_before_side_effect = status
            .metadata()
            .get(HEADER_PRE_HANDLER_REJECTION)
            .and_then(|value| value.to_str().ok())
            == Some(PRE_HANDLER_REJECTION_RPC_CONCURRENCY)
            || status
                .metadata()
                .get(HEADER_WORKER_DATA_REJECTION)
                .and_then(|value| value.to_str().ok())
                == Some(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT);
        let kind = match status.code() {
            Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => ClientErrorKind::InvalidArgument,
            Code::NotFound => ClientErrorKind::NotFound,
            Code::AlreadyExists => ClientErrorKind::AlreadyExists,
            Code::PermissionDenied | Code::Unauthenticated => ClientErrorKind::PermissionDenied,
            Code::ResourceExhausted => ClientErrorKind::ResourceExhausted,
            Code::Unimplemented => ClientErrorKind::Unsupported,
            Code::DeadlineExceeded => ClientErrorKind::Timeout,
            Code::Cancelled => ClientErrorKind::Cancelled,
            Code::Unavailable => ClientErrorKind::Unavailable,
            Code::DataLoss => ClientErrorKind::CorruptData,
            _ => ClientErrorKind::Internal,
        };
        Self {
            kind,
            operation: None,
            call_id: None,
            outcome_unknown: false,
            invalid_success_response: false,
            retry_after: None,
            message: format!("transport status {:?}: {}", status.code(), status.message()),
            detail: FailureDetail::Transport {
                code: status.code(),
                definitely_before_side_effect,
            },
        }
    }
}

fn client_kind_from_rpc(kind: ErrorKind) -> ClientErrorKind {
    match kind {
        ErrorKind::Metadata(kind) => match kind {
            MetadataErrorKind::NotFound => ClientErrorKind::NotFound,
            MetadataErrorKind::AlreadyExists => ClientErrorKind::AlreadyExists,
            MetadataErrorKind::NotDirectory => ClientErrorKind::NotDirectory,
            MetadataErrorKind::IsDirectory => ClientErrorKind::IsDirectory,
            MetadataErrorKind::DirectoryNotEmpty => ClientErrorKind::DirectoryNotEmpty,
            MetadataErrorKind::CrossMountRename => ClientErrorKind::CrossMount,
            MetadataErrorKind::Busy => ClientErrorKind::Busy,
            MetadataErrorKind::Conflict => ClientErrorKind::Conflict,
            MetadataErrorKind::Fencing | MetadataErrorKind::EpochMismatch => ClientErrorKind::Fenced,
            MetadataErrorKind::SessionInvalid => ClientErrorKind::SessionInvalid,
            MetadataErrorKind::SessionExpired => ClientErrorKind::SessionExpired,
            MetadataErrorKind::ResourceExhausted => ClientErrorKind::ResourceExhausted,
            MetadataErrorKind::NotLeader
            | MetadataErrorKind::StaleState
            | MetadataErrorKind::MountEpochMismatch
            | MetadataErrorKind::RouteEpochMismatch
            | MetadataErrorKind::OwnerGroupMismatch
            | MetadataErrorKind::GroupMismatch => ClientErrorKind::Unavailable,
        },
        ErrorKind::Worker(kind) => match kind {
            WorkerErrorKind::Timeout => ClientErrorKind::Timeout,
            WorkerErrorKind::ResourceExhausted => ClientErrorKind::ResourceExhausted,
            WorkerErrorKind::Cancelled => ClientErrorKind::Cancelled,
            WorkerErrorKind::Io => ClientErrorKind::Io,
            WorkerErrorKind::Corrupt => ClientErrorKind::CorruptData,
            WorkerErrorKind::Fencing => ClientErrorKind::Fenced,
            WorkerErrorKind::Conflict => ClientErrorKind::Conflict,
            WorkerErrorKind::NotFound => ClientErrorKind::NotFound,
            WorkerErrorKind::NotRegistered
            | WorkerErrorKind::RunMismatch
            | WorkerErrorKind::DescriptorMismatch
            | WorkerErrorKind::FullReportRequired
            | WorkerErrorKind::BlockLocationUnavailable
            | WorkerErrorKind::NodeUnavailable => ClientErrorKind::Unavailable,
        },
        ErrorKind::Protocol(kind) => match kind {
            ProtocolErrorKind::InvalidHeader => ClientErrorKind::InvalidResponse,
            ProtocolErrorKind::InvalidArgument => ClientErrorKind::InvalidArgument,
            ProtocolErrorKind::PermissionDenied => ClientErrorKind::PermissionDenied,
            ProtocolErrorKind::Unsupported => ClientErrorKind::Unsupported,
            ProtocolErrorKind::Cancelled => ClientErrorKind::Cancelled,
            ProtocolErrorKind::Corrupt => ClientErrorKind::CorruptData,
        },
        ErrorKind::Internal(kind) => match kind {
            InternalErrorKind::NodeUnavailable => ClientErrorKind::Unavailable,
            InternalErrorKind::Timeout => ClientErrorKind::Timeout,
            InternalErrorKind::ResourceExhausted => ClientErrorKind::ResourceExhausted,
            InternalErrorKind::Cancelled => ClientErrorKind::Cancelled,
            InternalErrorKind::Corrupt => ClientErrorKind::CorruptData,
            InternalErrorKind::Internal => ClientErrorKind::Internal,
        },
    }
}
