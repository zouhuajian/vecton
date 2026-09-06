// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata authority commands replicated through Raft.

use crate::inode::FilePublication;
use crate::inode::InodeAttrs;
pub(crate) use crate::inode::PublishMode;
use crate::session_registry::CreateFileOperationId;
use beryl_types::ids::{InodeId, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::{CallId, ClientId, GroupName, LeaseEpoch};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const MAX_RECLAIM_DETACHED_ROOT_CANDIDATES: u32 = 64;
pub(crate) const MAX_RECLAIM_DETACHED_ROOT_ENTRIES: u32 = 256;
pub(crate) const MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES: u32 = 4 * 1024;
pub(crate) const MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES: u32 = 1024 * 1024;

/// Largest serialized application command admitted to Raft.
///
/// This limits the command payload before OpenRaft constructs or persists a
/// log entry. Semantic apply limits remain responsible for bounding state
/// machine work after replay.
pub(crate) const MAX_COMMAND_BYTES: usize = 4 * 1024 * 1024;

/// One durable metadata authority operation.
///
/// CreateFile and CommitFile carry stable client/call identities for durable
/// response-loss replay. Other mutations retain their own state preconditions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Command {
    BootstrapNamespace {
        proposed_at_ms: u64,
        group_name: GroupName,
    },
    CreateDirectory {
        proposed_at_ms: u64,
        root_inode_id: InodeId,
        components: Vec<String>,
        attrs: InodeAttrs,
        recursive: bool,
    },
    CreateFile {
        proposed_at_ms: u64,
        operation_id: CreateFileOperationId,
        request_deadline_ms: u64,
        session_expires_at_ms: u64,
        normalized_path: String,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        attrs: InodeAttrs,
        layout: FileLayout,
    },
    /// Delete one exact mount-relative target after revalidating its path.
    ///
    /// Recursive directories are detached with a constant-size namespace
    /// mutation; descendants are reclaimed later by `ReclaimDetachedRoots`.
    Delete {
        proposed_at_ms: u64,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        expected_file_lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
    },
    Rename {
        proposed_at_ms: u64,
        src_parent_inode_id: InodeId,
        src_name: String,
        expected_src_inode_id: InodeId,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        expected_dst_inode_id: Option<InodeId>,
        expected_dst_lease_epoch: Option<LeaseEpoch>,
        flags: u32,
    },
    AcquireWriteLease {
        proposed_at_ms: u64,
        inode_id: InodeId,
        expected_lease_epoch: LeaseEpoch,
    },
    AllocateBlock {
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    },
    EndWriteLease {
        proposed_at_ms: u64,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    },
    PublishFile {
        proposed_at_ms: u64,
        inode_id: InodeId,
        publication: FilePublication,
    },
    /// Publish content, end the exact writer epoch, and record completion atomically.
    CommitFile {
        proposed_at_ms: u64,
        inode_id: InodeId,
        client_id: ClientId,
        call_id: CallId,
        publication: FilePublication,
    },
    RegisterWorkerDescriptor {
        proposed_at_ms: u64,
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    },
    /// Reclaim a bounded amount of namespace authority from detached roots.
    ///
    /// Every budget is part of the replicated command. Apply also enforces
    /// fixed protocol maxima so local configuration cannot make replicas
    /// execute different state transitions.
    ReclaimDetachedRoots {
        candidate_root_inode_ids: Vec<InodeId>,
        max_entries: u32,
        max_batch_bytes: u32,
    },
}

impl Command {
    /// Stable low-cardinality operation name for logs and metrics.
    pub(crate) fn operation_name(&self) -> &'static str {
        match self {
            Self::BootstrapNamespace { .. } => "bootstrap_namespace",
            Self::CreateDirectory { .. } => "create_directory",
            Self::CreateFile { .. } => "create_file",
            Self::Delete { .. } => "delete",
            Self::Rename { .. } => "rename",
            Self::AcquireWriteLease { .. } => "acquire_write_lease",
            Self::AllocateBlock { .. } => "allocate_block",
            Self::EndWriteLease { .. } => "end_write_lease",
            Self::PublishFile { .. } => "publish_file",
            Self::CommitFile { .. } => "commit_file",
            Self::RegisterWorkerDescriptor { .. } => "register_worker_descriptor",
            Self::ReclaimDetachedRoots { .. } => "reclaim_detached_roots",
        }
    }
}

/// Capture the server proposal timestamp immediately before Raft submission.
pub(crate) fn proposal_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockId, BlockIndex, CommittedBlock, ContentGeneration, MAX_FILE_BLOCKS};

    #[test]
    fn maximum_commit_file_command_fits_command_limit() {
        let inode_id = InodeId::new(u64::MAX);
        let blocks = (0..MAX_FILE_BLOCKS)
            .map(|index| CommittedBlock {
                block_id: BlockId::new(inode_id, BlockIndex::new(index as u32)),

                len: u64::MAX,
            })
            .collect();
        let command = Command::CommitFile {
            proposed_at_ms: u64::MAX,
            inode_id,
            client_id: ClientId::new(u128::MAX),
            call_id: CallId::new(),
            publication: FilePublication {
                blocks,
                target_size: u64::MAX,
                expected_generation: ContentGeneration::new(u64::MAX),
                expected_file_size: u64::MAX,
                lease_epoch: LeaseEpoch::new(u64::MAX),
                mode: PublishMode::ReplaceIfUnchanged,
            },
        };

        let encoded = serde_json::to_vec(&command).expect("maximum legal command must serialize");
        assert!(
            encoded.len() <= MAX_COMMAND_BYTES,
            "maximum CommitFile command is {} bytes, exceeding {}",
            encoded.len(),
            MAX_COMMAND_BYTES
        );
    }
}
