// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Common error types and utilities.

use std::error::Error as StdError;
use std::fmt;

pub mod rpc {
    //! RPC header error model for Beryl.
    //!
    //! The model has two independent axes:
    //! - `ErrorKind`: the fact that failed.
    //! - `RecoveryAction`: what a caller should do next.
    //!
    //! Human-readable `message` is diagnostic only. Machine control flow must
    //! branch on `kind` and `recovery`.

    use serde::{Deserialize, Serialize};

    /// Stable, machine-readable failure fact classified by the boundary that owns it.
    ///
    /// Success is represented by the absence of an `RpcErrorDetail`; every value
    /// of this enum therefore denotes a real failure.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum ErrorKind {
        /// Metadata namespace, authority, freshness, or session failure.
        Metadata(MetadataErrorKind),
        /// Worker storage or execution failure.
        Worker(WorkerErrorKind),
        /// Malformed, unauthorized, or unsupported request/protocol failure.
        Protocol(ProtocolErrorKind),
        /// Infrastructure or invariant failure not owned by a service domain.
        Internal(InternalErrorKind),
    }

    /// Metadata-domain failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum MetadataErrorKind {
        NotFound,
        AlreadyExists,
        NotDirectory,
        IsDirectory,
        DirectoryNotEmpty,
        CrossMountRename,
        Busy,
        Conflict,
        NotLeader,
        StaleState,
        MountEpochMismatch,
        RouteEpochMismatch,
        OwnerGroupMismatch,
        GroupMismatch,
        Fencing,
        SessionInvalid,
        SessionExpired,
        EpochMismatch,
        ResourceExhausted,
    }

    /// Worker-domain failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum WorkerErrorKind {
        NotRegistered,
        RunMismatch,
        DescriptorMismatch,
        FullReportRequired,
        BlockLocationUnavailable,
        NodeUnavailable,
        Timeout,
        ResourceExhausted,
        Conflict,
        Corrupt,
        Fencing,
        Cancelled,
        Io,
        /// A worker-owned local block or storage resource is absent.
        NotFound,
    }

    /// Protocol and request-shape failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum ProtocolErrorKind {
        InvalidHeader,
        InvalidArgument,
        PermissionDenied,
        Unsupported,
        Cancelled,
        Corrupt,
    }

    /// Internal or infrastructure failure fact.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum InternalErrorKind {
        NodeUnavailable,
        Timeout,
        ResourceExhausted,
        Cancelled,
        Corrupt,
        Internal,
    }

    /// Caller recovery strategy. This is deliberately smaller than `ErrorKind`.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum RecoveryAction {
        Fail,
        Retry { after_ms: Option<u64> },
        RefreshMetadata { hint: RefreshHint },
        ReopenWriteSession { hint: RefreshHint },
        RegisterWorker,
        SendFullBlockReport,
    }

    /// Worker endpoint hint used in RPC refresh hints.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WorkerEndpointHint {
        pub worker_id: u64,
        pub endpoint: String,
    }

    /// Structured refresh hints attached to RPC errors.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RefreshHint {
        pub leader_endpoint: Option<String>,
        pub group_name: Option<String>,
        pub mount_epoch: Option<u64>,
        pub mount_prefix: Option<String>,
        pub route_epoch: Option<u64>,
        pub worker_endpoints: Vec<WorkerEndpointHint>,
        pub worker_resolve_required: bool,
    }

    /// RPC error model for Beryl.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct RpcErrorDetail {
        pub kind: ErrorKind,
        pub recovery: RecoveryAction,
        pub message: String,
    }

    impl RpcErrorDetail {
        pub fn new(kind: ErrorKind, recovery: RecoveryAction, message: impl Into<String>) -> Self {
            Self {
                kind,
                recovery,
                message: message.into(),
            }
        }

        pub fn fail(kind: ErrorKind, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::Fail, message)
        }

        pub fn retry(kind: ErrorKind, after_ms: Option<u64>, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::Retry { after_ms }, message)
        }

        pub fn refresh_metadata(kind: ErrorKind, hint: RefreshHint, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::RefreshMetadata { hint }, message)
        }

        pub fn reopen_write_session(kind: ErrorKind, hint: RefreshHint, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::ReopenWriteSession { hint }, message)
        }

        pub fn register_worker(kind: ErrorKind, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::RegisterWorker, message)
        }

        pub fn send_full_block_report(kind: ErrorKind, message: impl Into<String>) -> Self {
            Self::new(kind, RecoveryAction::SendFullBlockReport, message)
        }
    }
}

/// Error kinds for common utility-layer failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommonErrorKind {
    /// Operation timed out.
    Timeout,
    /// Service is overloaded (too many concurrent requests).
    Overloaded,
    /// Resource not found.
    NotFound,
    /// Permission denied.
    PermissionDenied,
    /// Invalid argument.
    InvalidArgument,
    /// I/O error.
    Io,
    /// Internal error.
    Internal,
}

impl CommonErrorKind {
    /// Check if this error kind is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, CommonErrorKind::Timeout | CommonErrorKind::Overloaded)
    }
}

impl fmt::Display for CommonErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommonErrorKind::Timeout => write!(f, "Timeout"),
            CommonErrorKind::Overloaded => write!(f, "Overloaded"),
            CommonErrorKind::NotFound => write!(f, "NotFound"),
            CommonErrorKind::PermissionDenied => write!(f, "PermissionDenied"),
            CommonErrorKind::InvalidArgument => write!(f, "InvalidArgument"),
            CommonErrorKind::Io => write!(f, "Io"),
            CommonErrorKind::Internal => write!(f, "Internal"),
        }
    }
}

/// Common error type used across all modules.
#[derive(Clone, Debug)]
pub struct CommonError {
    /// Error kind.
    pub kind: CommonErrorKind,
    /// Human-readable error message.
    pub message: String,
}

impl CommonError {
    /// Create a new CommonError.
    pub fn new(kind: CommonErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl StdError for CommonError {}

impl From<CommonErrorKind> for CommonError {
    fn from(kind: CommonErrorKind) -> Self {
        CommonError::new(kind, kind.to_string())
    }
}
