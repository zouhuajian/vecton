// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB authority storage and OpenRaft storage-v2 backend.
//!
//! Keyspace schema:
//! - mounts/{mount_id} -> MountEntry (serialized)
//! - route_epoch -> u64
//! - mount_epoch -> u64
//!
//! FS schema:
//! - inodes/{inode_id_be_fixed_width} -> Inode (serialized)
//!   - key: "inode/" + 8 bytes BE (u64)
//!   - value: Inode (bincode)
//! - dentries/{parent_inode_id_be_fixed_width}/{name_bytes} -> child_inode_id_be_fixed_width
//!   - key: "dentry/" + 8 bytes BE (parent_inode_id) + name_bytes (UTF-8, no null terminator)
//!   - value: 8 bytes BE (child_inode_id)
//!   - Note: Fixed-width encoding enables efficient iteration and comparison

mod generation;
mod log_store;
mod query;
mod schema;
mod snapshot;
mod state_machine_store;
mod transaction;

use crate::error::{MetadataError, MetadataResult};
use crate::inode::Inode;
use crate::mount::MountEntry;
use crate::raft::AppMetadataRaftState;
use crate::session_registry::CreateFileOperationId;
use crate::state::RouteEpoch;
use crate::worker::WorkerInfo;
use beryl_types::ids::{InodeId, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::{CallId, ClientId, ContentGeneration, GroupName, LeaseEpoch};
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
pub(crate) use generation::{GenerationHandle, GenerationWriteGuard, PinnedGeneration, StagedGeneration};
pub(crate) use log_store::AppLogStorage;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
pub(crate) use snapshot::{SnapshotFile, SnapshotInstallTracker};
pub(crate) use state_machine_store::StateMachineStorage;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

type DentryPage = (Vec<(String, InodeId)>, Option<Vec<u8>>, bool);

/// Column family names for RocksDB.
const CF_MOUNTS: &str = "mounts";
const CF_WORKERS: &str = "workers";
/// Raft column families
const CF_META: &str = "meta"; // route_epoch, mount_epoch, file layouts, etc.
const CF_RAFT_LOG: &str = "raft_log"; // Raft log entries
const CF_RAFT_STATE: &str = "raft_state"; // Raft state (hard_state, membership)
const CF_RAFT_SNAPSHOT: &str = "raft_snapshot"; // Raft snapshots

const ROCKSDB_SCHEMA_VERSION_KEY: &[u8] = b"rocksdb_schema_version";
const STORAGE_IDENTITY_KEY: &[u8] = b"storage_identity";
const RAFT_STATE_KEY: &[u8] = b"raft_state";
/// Guards database and snapshot decoding against incompatible persisted metadata encodings.
pub(crate) const ROCKSDB_SCHEMA_VERSION: u64 = 4;
const NEXT_INODE_ID_KEY: &[u8] = b"next_inode_id";
const CREATE_FILE_REPLAY_COUNT_KEY: &[u8] = b"create_file_replay_count";
const CREATE_FILE_REPLAY_PREFIX: &[u8] = b"create_file_replay/";
const CREATE_FILE_REPLAY_EXPIRY_PREFIX: &[u8] = b"create_file_replay_expiry/";
const CREATE_FILE_REPLAY_INODE_PREFIX: &[u8] = b"create_file_replay_inode/";
/// Hard bound on durable CreateFile results retained for response-loss replay.
const MAX_CREATE_FILE_REPLAY_RECORDS: u64 = 65_536;

fn durable_raft_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    options
}

fn worker_key(group_name: &GroupName, worker_id: WorkerId) -> String {
    format!("{}/{}", group_name.as_str(), worker_id.as_raw())
}

// FS column families
const CF_INODES: &str = "inodes"; // inode/{inode_id_be} -> Inode
const CF_DENTRIES: &str = "dentries"; // dentry/{parent_inode_id_be}/{name} -> child_inode_id_be
const CF_DETACHED_ROOTS: &str = "detached_roots"; // root_inode_id_be -> DetachedRoot

const CURRENT_CFS: &[&str] = &[
    CF_MOUNTS,
    CF_WORKERS,
    CF_META,
    CF_RAFT_LOG,
    CF_RAFT_STATE,
    CF_RAFT_SNAPSHOT,
    CF_INODES,
    CF_DENTRIES,
    CF_DETACHED_ROOTS,
];

/// Column families that hold replicated state to be snapshotted/restored.
pub const STATE_CFS: &[&str] = &[
    CF_MOUNTS,
    CF_WORKERS,
    CF_META,
    CF_INODES,
    CF_DENTRIES,
    CF_DETACHED_ROOTS,
];

/// Durable identity binding between the lifecycle marker and its RocksDB state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StorageIdentity {
    pub storage_uuid: String,
    pub cluster_id: String,
    pub group_name: GroupName,
    pub node_id: u64,
    pub bootstrap_client_id: String,
    pub bootstrap_call_id: String,
    pub bootstrap_proposed_at_ms: u64,
}

/// Durable authority proving that a directory root is no longer reachable.
///
/// Descendants remain namespace authority until bounded reclamation removes
/// them. Child directories inherit `detached_at_ms` when they become separate
/// detached roots, preserving the original deletion age across restarts.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DetachedRoot {
    pub(crate) mount_id: MountId,
    pub(crate) detached_at_ms: u64,
}

/// One authoritative state-machine commit assembled before RocksDB publication.
#[derive(Default)]
pub(crate) struct AuthorityBatch(WriteBatch);

impl Deref for AuthorityBatch {
    type Target = WriteBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AuthorityBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<WriteBatch> for AuthorityBatch {
    fn from(batch: WriteBatch) -> Self {
        Self(batch)
    }
}

/// Inode identity reserved by a read-only allocator preparation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InodeAllocation {
    pub(crate) inode_id: InodeId,
    pub(crate) next_inode_id: InodeId,
}

/// Durable result and request identity for one replayable atomic CreateFile.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CreateFileReplayRecord {
    pub(crate) operation_id: CreateFileOperationId,
    pub(crate) request_deadline_ms: u64,
    pub(crate) normalized_path: String,
    pub(crate) parent_inode_id: InodeId,
    pub(crate) name: String,
    pub(crate) inode_id: InodeId,
    pub(crate) mount_id: MountId,
    pub(crate) expected_mount_epoch: u64,
    pub(crate) mount_root_inode_id: InodeId,
    pub(crate) relative_components: Vec<String>,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) layout: FileLayout,
    pub(crate) generation: ContentGeneration,
    pub(crate) expires_at_ms: u64,
}

/// One directory insertion in an atomic recursive mkdir mutation.
pub(crate) struct RecursiveMkdirEntry {
    pub(crate) parent_inode_id: InodeId,
    pub(crate) name: String,
    pub(crate) inode: Inode,
    pub(crate) updated_parent: Inode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BootstrapNamespaceState {
    Empty,
    Matching,
    Conflicting,
}

/// Overwritten rename target state that must be removed with the namespace move.
pub(crate) struct RenameOverwriteCleanup {
    pub inode_id: InodeId,
}

/// Namespace rename writes that must commit as one RocksDB batch.
pub(crate) struct RenameAtomicUpdate<'a> {
    pub src_parent_inode_id: InodeId,
    pub src_name: &'a str,
    pub dst_parent_inode_id: InodeId,
    pub dst_name: &'a str,
    pub src_inode_id: InodeId,
    pub overwritten_target: Option<RenameOverwriteCleanup>,
    pub updated_src_parent: Option<&'a Inode>,
    pub updated_dst_parent: Option<&'a Inode>,
}

/// One namespace child removed from a detached directory in a bounded apply.
///
/// Directories carry a child marker and retain their inode. File entries mark
/// that their layout authority must be removed with the inode.
pub(crate) struct DetachedRootReclaimEntry {
    pub(crate) parent_inode_id: InodeId,
    pub(crate) name: String,
    pub(crate) inode_id: InodeId,

    pub(crate) child_detached_root: Option<DetachedRoot>,
}

impl DetachedRootReclaimEntry {
    /// Deterministic key/value bytes contributed by this namespace mutation.
    pub(crate) fn logical_bytes(&self) -> MetadataResult<usize> {
        let mut bytes = RocksDBStorage::encode_dentry_key(self.parent_inode_id, &self.name).len();
        if let Some(detached_root) = self.child_detached_root {
            let encoded = RocksDBStorage::encode_detached_root(&detached_root)?;
            bytes = bytes
                .checked_add(RocksDBStorage::encode_detached_root_key(self.inode_id).len())
                .and_then(|value| value.checked_add(encoded.len()))
                .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
        } else {
            bytes = bytes
                .checked_add(RocksDBStorage::encode_inode_key(self.inode_id).len())
                .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
        }
        Ok(bytes)
    }
}

/// Complete RocksDB mutation prepared for one bounded detached-root apply.
///
/// The state machine validates every referenced inode and owner before this
/// update is committed, so storage never publishes a partially checked batch.
#[derive(Default)]
pub(crate) struct DetachedRootReclaimUpdate {
    pub(crate) entries: Vec<DetachedRootReclaimEntry>,
    pub(crate) completed_root_inode_ids: Vec<InodeId>,
}

impl DetachedRootReclaimUpdate {
    pub(crate) fn completed_root_logical_bytes(inode_id: InodeId) -> MetadataResult<usize> {
        RocksDBStorage::encode_inode_key(inode_id)
            .len()
            .checked_add(RocksDBStorage::encode_detached_root_key(inode_id).len())
            .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))
    }

    /// Return the replicated batch's deterministic logical key/value byte size.
    ///
    /// RocksDB implementation overhead is deliberately excluded because it is
    /// not a stable protocol value. The Raft apply-state write is included.
    pub(crate) fn logical_batch_bytes(&self, raft_state: &AppMetadataRaftState) -> MetadataResult<usize> {
        let encoded_state = serde_json::to_vec(raft_state)
            .map_err(|error| MetadataError::Internal(format!("Failed to serialize Raft state: {error}")))?;
        let mut bytes = RAFT_STATE_KEY
            .len()
            .checked_add(encoded_state.len())
            .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
        for entry in &self.entries {
            bytes = bytes
                .checked_add(entry.logical_bytes()?)
                .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
        }
        for inode_id in &self.completed_root_inode_ids {
            bytes = bytes
                .checked_add(Self::completed_root_logical_bytes(*inode_id)?)
                .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
        }
        Ok(bytes)
    }
}

/// RocksDB storage backend.
pub(crate) struct RocksDBStorage {
    generations: GenerationHandle,
}

impl RocksDBStorage {
    /// Encode inode key: "inode/" + 8 bytes BE (inode_id)
    fn encode_inode_key(inode_id: InodeId) -> Vec<u8> {
        let mut key = b"inode/".to_vec();
        key.extend_from_slice(&inode_id.to_be_bytes());
        key
    }

    /// Encode dentry key: "dentry/" + 8 bytes BE (parent_inode_id) + name_bytes
    fn encode_dentry_key(parent_inode_id: InodeId, name: &str) -> Vec<u8> {
        let mut key = b"dentry/".to_vec();
        key.extend_from_slice(&parent_inode_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        key
    }

    fn encode_detached_root_key(inode_id: InodeId) -> [u8; 8] {
        inode_id.to_be_bytes()
    }

    fn decode_detached_root_key(key: &[u8]) -> MetadataResult<InodeId> {
        let raw: [u8; 8] = key
            .try_into()
            .map_err(|_| MetadataError::Internal(format!("Invalid detached-root key length: {}", key.len())))?;
        let inode_id = InodeId::from_be_bytes(raw);
        if inode_id.as_raw() == 0 {
            return Err(MetadataError::Internal(
                "Detached-root inode ID must be non-zero".to_string(),
            ));
        }
        Ok(inode_id)
    }

    fn encode_detached_root(detached_root: &DetachedRoot) -> MetadataResult<Vec<u8>> {
        encode_to_vec(detached_root, standard())
            .map_err(|error| MetadataError::Internal(format!("Failed to serialize DetachedRoot: {error}")))
    }

    fn decode_detached_root(inode_id: InodeId, value: &[u8]) -> MetadataResult<DetachedRoot> {
        let (detached_root, consumed): (DetachedRoot, usize) =
            decode_from_slice(value, standard()).map_err(|error| {
                MetadataError::Internal(format!(
                    "Failed to deserialize DetachedRoot for inode {inode_id}: {error}"
                ))
            })?;
        if consumed != value.len() {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot for inode {inode_id} has {} trailing bytes",
                value.len() - consumed
            )));
        }
        if detached_root.mount_id.as_raw() == 0 {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot for inode {inode_id} has zero mount ID"
            )));
        }
        Ok(detached_root)
    }

    fn encode_create_file_operation_bytes(operation_id: CreateFileOperationId) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(&operation_id.client_id.as_raw().to_be_bytes());
        bytes[16..].copy_from_slice(operation_id.call_id.as_uuid().as_bytes());
        bytes
    }

    fn encode_create_file_replay_key(operation_id: CreateFileOperationId) -> Vec<u8> {
        let mut key = Vec::with_capacity(CREATE_FILE_REPLAY_PREFIX.len() + 32);
        key.extend_from_slice(CREATE_FILE_REPLAY_PREFIX);
        key.extend_from_slice(&Self::encode_create_file_operation_bytes(operation_id));
        key
    }

    fn decode_create_file_operation_bytes(bytes: &[u8]) -> MetadataResult<CreateFileOperationId> {
        if bytes.len() != 32 {
            return Err(MetadataError::Internal(
                "invalid CreateFile operation identity length".to_string(),
            ));
        }
        let client_id = ClientId::new(u128::from_be_bytes(
            bytes[..16].try_into().expect("checked client identity length"),
        ));
        let call_id = CallId::from_uuid(Uuid::from_bytes(
            bytes[16..].try_into().expect("checked call identity length"),
        ));
        Ok(CreateFileOperationId { client_id, call_id })
    }

    fn encode_create_file_replay_inode_key(inode_id: InodeId) -> Vec<u8> {
        let mut key = Vec::with_capacity(CREATE_FILE_REPLAY_INODE_PREFIX.len() + 8);
        key.extend_from_slice(CREATE_FILE_REPLAY_INODE_PREFIX);
        key.extend_from_slice(&inode_id.to_be_bytes());
        key
    }

    fn encode_create_file_replay_expiry_key(record: &CreateFileReplayRecord) -> Vec<u8> {
        let mut key = Vec::with_capacity(CREATE_FILE_REPLAY_EXPIRY_PREFIX.len() + 8 + 32);
        key.extend_from_slice(CREATE_FILE_REPLAY_EXPIRY_PREFIX);
        key.extend_from_slice(&record.expires_at_ms.to_be_bytes());
        key.extend_from_slice(&Self::encode_create_file_operation_bytes(record.operation_id));
        key
    }

    fn decode_create_file_replay_expiry_key(key: &[u8]) -> MetadataResult<(u64, CreateFileOperationId)> {
        let expected_len = CREATE_FILE_REPLAY_EXPIRY_PREFIX.len() + 8 + 32;
        if key.len() != expected_len || !key.starts_with(CREATE_FILE_REPLAY_EXPIRY_PREFIX) {
            return Err(MetadataError::Internal(
                "invalid CreateFile replay expiry key".to_string(),
            ));
        }
        let payload = &key[CREATE_FILE_REPLAY_EXPIRY_PREFIX.len()..];
        let expires_at_ms = u64::from_be_bytes(payload[..8].try_into().expect("checked expiry key length"));
        Ok((expires_at_ms, Self::decode_create_file_operation_bytes(&payload[8..])?))
    }

    fn cf<'a>(db: &'a DB, name: &str) -> MetadataResult<&'a ColumnFamily> {
        db.cf_handle(name)
            .ok_or_else(|| MetadataError::Internal(format!("Column family {} not found", name)))
    }
}
