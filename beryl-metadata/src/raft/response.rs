// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Responses produced by committed metadata Raft commands.

use crate::error::MetadataError;
use crate::inode::InodeAttrs;
use crate::mount::MountEntry;

use beryl_types::ids::{BlockId, InodeId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::{ContentGeneration, LeaseEpoch};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Result produced for one durably applied metadata Raft entry.
///
/// `Err(ApplyRejection)` is a deterministic application rejection whose log
/// entry and applied index have committed. Infrastructure and invariant
/// failures are returned separately as [`FatalApplyError`] and never enter
/// this response.
pub(crate) type RaftApplyResult = Result<ApplySuccess, ApplyRejection>;

/// Successful outcome of applying one metadata Raft entry.
///
/// Application commands use operation-specific variants so required response
/// fields cannot be omitted or combined with fields from another operation.
/// `RaftEntryApplied` is reserved for OpenRaft blank and membership entries,
/// which require one response value but have no metadata command payload.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ApplySuccess {
    /// Root mount created or confirmed by namespace bootstrap.
    MountUpserted(MountEntry),
    /// Requested directory path exists with these persisted attributes.
    DirectoryEnsured { inode_id: InodeId, attrs: InodeAttrs },
    /// File inode, initial write lease, and replay record committed atomically.
    FileCreated {
        inode_id: InodeId,
        layout: FileLayout,
        lease_epoch: LeaseEpoch,
        expires_at_ms: u64,
        generation: ContentGeneration,
    },
    /// The exact namespace delete mutation committed.
    DeleteApplied,
    /// The exact namespace rename mutation committed.
    RenameApplied,
    /// Durable write-lease authority advanced to this epoch.
    WriteLeaseAcquired { inode_id: InodeId, lease_epoch: LeaseEpoch },
    /// Durable block ordinal allocated for one active write lease.
    BlockAllocated(BlockId),
    /// Durable write-lease authority ended at this fencing epoch.
    WriteLeaseEnded { inode_id: InodeId, lease_epoch: LeaseEpoch },
    /// File visibility committed at this content generation.
    FilePublished {
        inode_id: InodeId,
        generation: ContentGeneration,
    },
    /// Content, ended writer epoch, and exact completion evidence committed together.
    FileCommitted {
        inode_id: InodeId,
        generation: ContentGeneration,
        lease_epoch: LeaseEpoch,
    },
    /// Durable worker descriptor accepted by the authority state.
    WorkerUpserted(WorkerId),
    /// Bounded progress made by one internal detached-root mutation.
    DetachedRootsReclaimed(DetachedRootReclaimResult),
    /// OpenRaft blank or membership entry durably applied without an application command.
    RaftEntryApplied,
}

/// Deterministic progress counters returned to the leader-only reclaimer.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DetachedRootReclaimResult {
    pub(crate) processed_entries: u32,
    pub(crate) completed_roots: u32,
    pub(crate) created_roots: u32,
    pub(crate) logical_batch_bytes: u32,
}

/// Recoverable error kinds that may be committed as deterministic apply results.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ApplyRejectionKind {
    NotFound,
    AlreadyExists,
    InvalidArgument,
    NotDir,
    IsDir,
    DirectoryNotEmpty,
    CrossMountRename,
    PermissionDenied,
    NotSupported,
    Busy,
    ActiveWorkerConflict,
    Again,
    ResourceExhausted,
    LeaseFenced { expected: LeaseEpoch, got: LeaseEpoch },
}

/// Recoverable application failure returned through Raft.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ApplyRejection {
    pub kind: ApplyRejectionKind,
    pub message: String,
}

impl ApplyRejection {
    /// Convert a deterministic metadata-domain failure into a committed result.
    ///
    /// Infrastructure, leadership, freshness, and invariant failures remain
    /// fatal because replaying them as business rejections would hide an apply
    /// failure or make it dependent on leader-local state.
    pub(crate) fn from_metadata_error(error: MetadataError) -> Result<Self, FatalApplyError> {
        let rejection = match error {
            MetadataError::NotFound(message) => Self {
                kind: ApplyRejectionKind::NotFound,
                message,
            },
            MetadataError::AlreadyExists(message) => Self {
                kind: ApplyRejectionKind::AlreadyExists,
                message,
            },
            MetadataError::InvalidArgument(message) => Self {
                kind: ApplyRejectionKind::InvalidArgument,
                message,
            },
            MetadataError::NotDir(message) => Self {
                kind: ApplyRejectionKind::NotDir,
                message,
            },
            MetadataError::IsDir(message) => Self {
                kind: ApplyRejectionKind::IsDir,
                message,
            },
            MetadataError::DirectoryNotEmpty(message) => Self {
                kind: ApplyRejectionKind::DirectoryNotEmpty,
                message,
            },
            MetadataError::CrossMountRename(message) => Self {
                kind: ApplyRejectionKind::CrossMountRename,
                message,
            },
            MetadataError::PermissionDenied(message) => Self {
                kind: ApplyRejectionKind::PermissionDenied,
                message,
            },
            MetadataError::NotSupported(message) => Self {
                kind: ApplyRejectionKind::NotSupported,
                message,
            },
            MetadataError::Busy(message) => Self {
                kind: ApplyRejectionKind::Busy,
                message,
            },
            MetadataError::ActiveWorkerConflict(message) => Self {
                kind: ApplyRejectionKind::ActiveWorkerConflict,
                message,
            },
            MetadataError::Again(message) => Self {
                kind: ApplyRejectionKind::Again,
                message,
            },
            MetadataError::ResourceExhausted(message) => Self {
                kind: ApplyRejectionKind::ResourceExhausted,
                message,
            },
            MetadataError::LeaseFenced { expected, got } => Self {
                kind: ApplyRejectionKind::LeaseFenced { expected, got },
                message: format!("lease fenced: expected epoch >= {expected}, got {got}"),
            },
            fatal @ (MetadataError::LeaderChanged(_)
            | MetadataError::EpochMismatch { .. }
            | MetadataError::MountEpochMismatch { .. }
            | MetadataError::RoutingStale(_)
            | MetadataError::StaleState(_)
            | MetadataError::FullReportRequired(_)
            | MetadataError::WriteSessionLimitExceeded(_)
            | MetadataError::GlobalWriteTargetLimitExceeded(_)
            | MetadataError::Internal(_)
            | MetadataError::ServiceUnavailable(_)) => return Err(FatalApplyError(fatal)),
        };
        Ok(rejection)
    }

    /// Restore the metadata-domain failure after a committed result reaches the proposer.
    pub fn into_metadata_error(self) -> MetadataError {
        match self.kind {
            ApplyRejectionKind::NotFound => MetadataError::NotFound(self.message),
            ApplyRejectionKind::AlreadyExists => MetadataError::AlreadyExists(self.message),
            ApplyRejectionKind::InvalidArgument => MetadataError::InvalidArgument(self.message),
            ApplyRejectionKind::NotDir => MetadataError::NotDir(self.message),
            ApplyRejectionKind::IsDir => MetadataError::IsDir(self.message),
            ApplyRejectionKind::DirectoryNotEmpty => MetadataError::DirectoryNotEmpty(self.message),
            ApplyRejectionKind::CrossMountRename => MetadataError::CrossMountRename(self.message),
            ApplyRejectionKind::PermissionDenied => MetadataError::PermissionDenied(self.message),
            ApplyRejectionKind::NotSupported => MetadataError::NotSupported(self.message),
            ApplyRejectionKind::Busy => MetadataError::Busy(self.message),
            ApplyRejectionKind::ActiveWorkerConflict => MetadataError::ActiveWorkerConflict(self.message),
            ApplyRejectionKind::Again => MetadataError::Again(self.message),
            ApplyRejectionKind::ResourceExhausted => MetadataError::ResourceExhausted(self.message),
            ApplyRejectionKind::LeaseFenced { expected, got } => MetadataError::LeaseFenced { expected, got },
        }
    }
}

/// Infrastructure or invariant failure that must fail committed apply closed.
#[derive(Debug, Error)]
#[error("fatal metadata Raft apply error: {0}")]
pub(crate) struct FatalApplyError(MetadataError);

impl FatalApplyError {
    pub(crate) fn new(error: MetadataError) -> Self {
        Self(error)
    }

    pub(crate) fn as_inner(&self) -> &MetadataError {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MetadataError;

    #[test]
    fn apply_rejections_preserve_domain_errors_and_keep_fatal_errors_separate() {
        let rejection = ApplyRejection::from_metadata_error(MetadataError::NotFound("inode 7".to_string())).unwrap();

        assert!(matches!(
            rejection.into_metadata_error(),
            MetadataError::NotFound(message) if message == "inode 7"
        ));

        let rejection =
            ApplyRejection::from_metadata_error(MetadataError::ResourceExhausted("extent limit".to_string())).unwrap();
        assert!(matches!(
            rejection.into_metadata_error(),
            MetadataError::ResourceExhausted(message) if message == "extent limit"
        ));

        let rejection = ApplyRejection::from_metadata_error(MetadataError::LeaseFenced {
            expected: LeaseEpoch::new(11),
            got: LeaseEpoch::new(9),
        })
        .unwrap();
        assert!(matches!(
            rejection.into_metadata_error(),
            MetadataError::LeaseFenced { expected, got } if expected == LeaseEpoch::new(11) && got == LeaseEpoch::new(9)
        ));

        let fatal =
            ApplyRejection::from_metadata_error(MetadataError::Internal("decode failed".to_string())).unwrap_err();
        assert!(matches!(
            fatal.as_inner(),
            MetadataError::Internal(message) if message == "decode failed"
        ));
    }
}
