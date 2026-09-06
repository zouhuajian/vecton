// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{
    decode_from_slice, encode_to_vec, standard, worker_key, AppMetadataRaftState, ColumnFamily, CreateFileReplayRecord,
    DetachedRoot, DetachedRootReclaimUpdate, Inode, InodeAllocation, InodeId, Instant, MetadataError, MetadataResult,
    MountEntry, RecursiveMkdirEntry, RenameAtomicUpdate, RocksDBStorage, RouteEpoch, WorkerInfo, WriteBatch,
    CF_DENTRIES, CF_DETACHED_ROOTS, CF_INODES, CF_META, CF_MOUNTS, CF_RAFT_STATE, CF_WORKERS,
    CREATE_FILE_REPLAY_COUNT_KEY, CREATE_FILE_REPLAY_EXPIRY_PREFIX, DB, MAX_CREATE_FILE_REPLAY_RECORDS,
    NEXT_INODE_ID_KEY, RAFT_STATE_KEY,
};
use rocksdb::{Direction, IteratorMode};

impl RocksDBStorage {
    pub(super) fn commit_authority_batch(
        &self,
        mut batch: WriteBatch,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_raft_state = Self::cf(db, CF_RAFT_STATE)?;
        let state_data = serde_json::to_vec(raft_state)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Raft state: {e}")))?;
        batch.put_cf(cf_raft_state, RAFT_STATE_KEY, state_data);
        let started = Instant::now();
        let result = db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("Failed to commit authority batch: {e}")));
        crate::observe::record_raft_authority_commit(
            if result.is_ok() { "ok" } else { "error" },
            started.elapsed().as_secs_f64(),
        );
        result
    }

    fn batch_put_mount(batch: &mut WriteBatch, cf: &ColumnFamily, entry: &MountEntry) -> MetadataResult<()> {
        let key = format!("{}", entry.mount_id.as_raw());
        let value = encode_to_vec(entry, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize MountEntry: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    fn batch_put_route_epoch(batch: &mut WriteBatch, cf: &ColumnFamily, epoch: RouteEpoch) -> MetadataResult<()> {
        let value = encode_to_vec(epoch.as_u64(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize route_epoch: {}", e)))?;
        batch.put_cf(cf, b"route_epoch", value);
        Ok(())
    }

    fn batch_put_mount_epoch(batch: &mut WriteBatch, cf: &ColumnFamily, epoch: u64) -> MetadataResult<()> {
        let value = encode_to_vec(epoch, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize mount_epoch: {}", e)))?;
        batch.put_cf(cf, b"mount_epoch", value);
        Ok(())
    }

    fn batch_put_inode_allocation(
        batch: &mut WriteBatch,
        cf_meta: &ColumnFamily,
        allocation: InodeAllocation,
    ) -> MetadataResult<()> {
        let value = encode_to_vec(allocation.next_inode_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;
        batch.put_cf(cf_meta, NEXT_INODE_ID_KEY, value);
        Ok(())
    }

    fn validate_inode_allocation_targets(
        db: &DB,
        allocation: InodeAllocation,
        target_inode_ids: &[InodeId],
    ) -> MetadataResult<()> {
        if target_inode_ids.is_empty() || allocation.inode_id.as_raw() < 2 {
            return Err(MetadataError::Internal(
                "inode allocation has no valid target".to_string(),
            ));
        }
        let cf_meta = Self::cf(db, CF_META)?;
        let persisted = db
            .get_cf(cf_meta, NEXT_INODE_ID_KEY)
            .map_err(|error| MetadataError::Internal(format!("Failed to read next_inode_id: {error}")))?
            .ok_or_else(|| MetadataError::Internal("next_inode_id allocator authority is missing".to_string()))?;
        let persisted: u64 = decode_from_slice(&persisted, standard())
            .map_err(|error| MetadataError::Internal(format!("Failed to deserialize next_inode_id: {error}")))?
            .0;
        if persisted != allocation.inode_id.as_raw() {
            return Err(MetadataError::Internal(format!(
                "inode allocation {} does not match durable next_inode_id {persisted}",
                allocation.inode_id
            )));
        }

        let cf_inodes = Self::cf(db, CF_INODES)?;
        let mut expected_raw = allocation.inode_id.as_raw();
        for inode_id in target_inode_ids {
            if inode_id.as_raw() != expected_raw {
                return Err(MetadataError::Internal(format!(
                    "inode allocation target {inode_id} is not the expected inode {expected_raw}"
                )));
            }
            if db
                .get_cf(cf_inodes, Self::encode_inode_key(*inode_id))
                .map_err(|error| MetadataError::Internal(format!("Failed to read inode {inode_id}: {error}")))?
                .is_some()
            {
                return Err(MetadataError::Internal(format!(
                    "inode allocation target already exists: {inode_id}"
                )));
            }
            expected_raw = expected_raw
                .checked_add(1)
                .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
        }
        if allocation.next_inode_id.as_raw() != expected_raw {
            return Err(MetadataError::Internal(format!(
                "inode allocation next value {} does not match expected {expected_raw}",
                allocation.next_inode_id
            )));
        }
        Ok(())
    }

    fn batch_put_worker(batch: &mut WriteBatch, cf: &ColumnFamily, info: &WorkerInfo) -> MetadataResult<()> {
        let key = worker_key(&info.group_name, info.worker_id);
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    pub fn register_worker_atomic(&self, info: &WorkerInfo, raft_state: &AppMetadataRaftState) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_workers = Self::cf(db, CF_WORKERS)?;
        if info.worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }

        let mut batch = WriteBatch::default();
        Self::batch_put_worker(&mut batch, cf_workers, info)?;
        self.commit_authority_batch(batch, raft_state)
    }

    fn batch_put_inode(batch: &mut WriteBatch, cf: &ColumnFamily, inode: &Inode) -> MetadataResult<()> {
        let key = Self::encode_inode_key(inode.inode_id);
        let value = serde_json::to_vec(inode)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Inode: {}", e)))?;
        batch.put_cf(cf, key, value);
        Ok(())
    }

    /// Atomically persist a single inode update with apply tracking.
    pub fn put_inode_atomic(&self, inode: &Inode, raft_state: &AppMetadataRaftState) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        self.commit_authority_batch(batch, raft_state)
    }

    pub(crate) fn bootstrap_namespace_atomic(
        &self,
        root_inode: &Inode,
        root_mount: &MountEntry,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_mounts = Self::cf(db, CF_MOUNTS)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, root_inode)?;
        Self::batch_put_mount(&mut batch, cf_mounts, root_mount)?;
        Self::batch_put_route_epoch(&mut batch, cf_meta, RouteEpoch::new(1))?;
        Self::batch_put_mount_epoch(&mut batch, cf_meta, 1)?;
        batch.put_cf(
            cf_meta,
            NEXT_INODE_ID_KEY,
            encode_to_vec(2u64, standard())
                .map_err(|error| MetadataError::Internal(format!("Failed to serialize next_inode_id: {error}")))?,
        );
        self.commit_authority_batch(batch, raft_state)
    }

    fn create_file_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;

        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        batch.put_cf(
            cf_dentries,
            Self::encode_dentry_key(parent_inode_id, name),
            inode.inode_id.to_be_bytes(),
        );

        Ok(batch)
    }

    /// Add one bounded replay record, evicting only an already expired oldest entry.
    fn batch_put_create_file_replay(
        db: &DB,
        cf_meta: &ColumnFamily,
        batch: &mut WriteBatch,
        record: &CreateFileReplayRecord,
        proposed_at_ms: u64,
    ) -> MetadataResult<()> {
        let replay_key = Self::encode_create_file_replay_key(record.operation_id);
        let replay_inode_key = Self::encode_create_file_replay_inode_key(record.inode_id);
        if db
            .get_cf(cf_meta, &replay_key)
            .map_err(|error| MetadataError::Internal(format!("Failed to check CreateFile replay identity: {error}")))?
            .is_some()
        {
            return Err(MetadataError::Internal(
                "new CreateFile mutation attempted to overwrite a replay record".to_string(),
            ));
        }
        if db
            .get_cf(cf_meta, &replay_inode_key)
            .map_err(|error| {
                MetadataError::Internal(format!("Failed to check CreateFile inode replay index: {error}"))
            })?
            .is_some()
        {
            return Err(MetadataError::Internal(
                "new CreateFile mutation attempted to overwrite an inode replay index".to_string(),
            ));
        }
        let mut replay_count = match db
            .get_cf(cf_meta, CREATE_FILE_REPLAY_COUNT_KEY)
            .map_err(|error| MetadataError::Internal(format!("Failed to read CreateFile replay count: {error}")))?
        {
            Some(value) => {
                decode_from_slice::<u64, _>(&value, standard())
                    .map_err(|error| {
                        MetadataError::Internal(format!("Failed to decode CreateFile replay count: {error}"))
                    })?
                    .0
            }
            None => 0,
        };
        if replay_count > MAX_CREATE_FILE_REPLAY_RECORDS {
            return Err(MetadataError::Internal(format!(
                "CreateFile replay count {replay_count} exceeds its compiled bound"
            )));
        }
        if replay_count == MAX_CREATE_FILE_REPLAY_RECORDS {
            let mut iter = db.iterator_cf(
                cf_meta,
                IteratorMode::From(CREATE_FILE_REPLAY_EXPIRY_PREFIX, Direction::Forward),
            );
            let Some(item) = iter.next() else {
                return Err(MetadataError::Internal(
                    "CreateFile replay count has no expiry index".to_string(),
                ));
            };
            let (expiry_key, _) = item.map_err(|error| {
                MetadataError::Internal(format!("Failed to scan CreateFile replay expiry: {error}"))
            })?;
            if !expiry_key.starts_with(CREATE_FILE_REPLAY_EXPIRY_PREFIX) {
                return Err(MetadataError::Internal(
                    "CreateFile replay count has no matching expiry index".to_string(),
                ));
            }
            let (expires_at_ms, expired_operation_id) = Self::decode_create_file_replay_expiry_key(&expiry_key)?;
            if expires_at_ms > proposed_at_ms {
                return Err(MetadataError::ResourceExhausted(format!(
                    "CreateFile replay capacity {MAX_CREATE_FILE_REPLAY_RECORDS} is exhausted"
                )));
            }
            let expired_replay_key = Self::encode_create_file_replay_key(expired_operation_id);
            let expired_replay_value = db
                .get_cf(cf_meta, &expired_replay_key)
                .map_err(|error| {
                    MetadataError::Internal(format!("Failed to verify expired CreateFile replay record: {error}"))
                })?
                .ok_or_else(|| MetadataError::Internal("CreateFile replay expiry index has no record".to_string()))?;
            let (expired_record, consumed): (CreateFileReplayRecord, usize) =
                decode_from_slice(&expired_replay_value, standard()).map_err(|error| {
                    MetadataError::Internal(format!("Failed to decode expired CreateFile replay record: {error}"))
                })?;
            if consumed != expired_replay_value.len() || expired_record.operation_id != expired_operation_id {
                return Err(MetadataError::Internal(
                    "CreateFile replay expiry index has a corrupt record".to_string(),
                ));
            }
            let expired_inode_key = Self::encode_create_file_replay_inode_key(expired_record.inode_id);
            let indexed_operation = db
                .get_cf(cf_meta, &expired_inode_key)
                .map_err(|error| {
                    MetadataError::Internal(format!("Failed to verify expired CreateFile inode index: {error}"))
                })?
                .ok_or_else(|| MetadataError::Internal("expired CreateFile replay has no inode index".to_string()))?;
            if Self::decode_create_file_operation_bytes(&indexed_operation)? != expired_operation_id {
                return Err(MetadataError::Internal(
                    "expired CreateFile inode replay index names another operation".to_string(),
                ));
            }
            batch.delete_cf(cf_meta, expiry_key);
            batch.delete_cf(cf_meta, expired_replay_key);
            batch.delete_cf(cf_meta, expired_inode_key);
            replay_count -= 1;
        }

        let replay_value = encode_to_vec(record, standard())
            .map_err(|error| MetadataError::Internal(format!("Failed to encode CreateFile replay record: {error}")))?;
        batch.put_cf(cf_meta, replay_key, replay_value);
        batch.put_cf(
            cf_meta,
            replay_inode_key,
            Self::encode_create_file_operation_bytes(record.operation_id),
        );
        batch.put_cf(cf_meta, Self::encode_create_file_replay_expiry_key(record), []);
        replay_count += 1;
        batch.put_cf(
            cf_meta,
            CREATE_FILE_REPLAY_COUNT_KEY,
            encode_to_vec(replay_count, standard()).map_err(|error| {
                MetadataError::Internal(format!("Failed to encode CreateFile replay count: {error}"))
            })?,
        );
        Ok(())
    }

    /// Atomically persist create-file mutation with apply tracking.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_file_atomic(
        &self,
        allocation: InodeAllocation,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        replay_record: &CreateFileReplayRecord,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        Self::validate_inode_allocation_targets(db, allocation, std::slice::from_ref(&inode.inode_id))?;
        let mut batch = self.create_file_batch(parent_inode_id, name, inode, updated_parent)?;
        let cf_meta = Self::cf(db, CF_META)?;
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        Self::batch_put_create_file_replay(db, cf_meta, &mut batch, replay_record, proposed_at_ms)?;
        self.commit_authority_batch(batch, raft_state)
    }

    fn create_dir_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;

        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        batch.put_cf(
            cf_dentries,
            Self::encode_dentry_key(parent_inode_id, name),
            inode.inode_id.to_be_bytes(),
        );

        Ok(batch)
    }

    /// Atomically persist mkdir mutation with apply tracking.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_dir_atomic(
        &self,
        allocation: InodeAllocation,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        Self::validate_inode_allocation_targets(db, allocation, std::slice::from_ref(&inode.inode_id))?;
        let mut batch = self.create_dir_batch(parent_inode_id, name, inode, updated_parent)?;
        let cf_meta = Self::cf(db, CF_META)?;
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_authority_batch(batch, raft_state)
    }

    /// Atomically persist all missing components of one recursive mkdir command.
    pub(crate) fn create_directories_atomic(
        &self,
        allocation: InodeAllocation,
        entries: &[RecursiveMkdirEntry],
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let target_inode_ids: Vec<_> = entries.iter().map(|entry| entry.inode.inode_id).collect();
        Self::validate_inode_allocation_targets(db, allocation, &target_inode_ids)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            Self::batch_put_inode(&mut batch, cf_inodes, &entry.inode)?;
            Self::batch_put_inode(&mut batch, cf_inodes, &entry.updated_parent)?;
            batch.put_cf(
                cf_dentries,
                Self::encode_dentry_key(entry.parent_inode_id, &entry.name),
                entry.inode.inode_id.to_be_bytes(),
            );
        }
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_authority_batch(batch, raft_state)
    }

    fn delete_dentry_inode_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;

        let mut batch = WriteBatch::default();
        batch.delete_cf(cf_dentries, Self::encode_dentry_key(parent_inode_id, name));
        batch.delete_cf(cf_inodes, Self::encode_inode_key(inode_id));
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        Ok(batch)
    }

    /// Atomically persist empty-directory deletion with apply tracking.
    #[allow(clippy::too_many_arguments)]
    pub fn unlink_inode_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        let batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        self.commit_authority_batch(batch, raft_state)
    }

    /// Atomically hide a recursive-delete root and publish its durable marker.
    ///
    /// The root inode and all descendants remain intact for bounded background
    /// reclamation. Namespace visibility and reclaim authority therefore
    /// change in the same RocksDB commit as Raft apply tracking.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn detach_directory_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        root_inode_id: InodeId,
        updated_parent: &Inode,
        detached_root: DetachedRoot,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_detached_roots = Self::cf(db, CF_DETACHED_ROOTS)?;
        let mut batch = WriteBatch::default();

        batch.delete_cf(cf_dentries, Self::encode_dentry_key(parent_inode_id, name));
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        batch.put_cf(
            cf_detached_roots,
            Self::encode_detached_root_key(root_inode_id),
            Self::encode_detached_root(&detached_root)?,
        );

        self.commit_authority_batch(batch, raft_state)
    }

    /// Atomically publish one validated, bounded detached-root reclamation.
    pub(crate) fn reclaim_detached_roots_atomic(
        &self,
        update: DetachedRootReclaimUpdate,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_detached_roots = Self::cf(db, CF_DETACHED_ROOTS)?;
        let mut batch = WriteBatch::default();

        for entry in update.entries {
            batch.delete_cf(cf_dentries, Self::encode_dentry_key(entry.parent_inode_id, &entry.name));
            if let Some(detached_root) = entry.child_detached_root {
                batch.put_cf(
                    cf_detached_roots,
                    Self::encode_detached_root_key(entry.inode_id),
                    Self::encode_detached_root(&detached_root)?,
                );
                continue;
            }

            batch.delete_cf(cf_inodes, Self::encode_inode_key(entry.inode_id));
        }
        for root_inode_id in update.completed_root_inode_ids {
            batch.delete_cf(cf_inodes, Self::encode_inode_key(root_inode_id));
            batch.delete_cf(cf_detached_roots, Self::encode_detached_root_key(root_inode_id));
        }

        self.commit_authority_batch(batch, raft_state)
    }

    fn rename_batch(&self, update: RenameAtomicUpdate<'_>) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;

        let mut batch = WriteBatch::default();

        if let Some(cleanup) = update.overwritten_target {
            batch.delete_cf(cf_inodes, Self::encode_inode_key(cleanup.inode_id));
            batch.delete_cf(
                cf_dentries,
                Self::encode_dentry_key(update.dst_parent_inode_id, update.dst_name),
            );
        }

        batch.delete_cf(
            cf_dentries,
            Self::encode_dentry_key(update.src_parent_inode_id, update.src_name),
        );
        batch.put_cf(
            cf_dentries,
            Self::encode_dentry_key(update.dst_parent_inode_id, update.dst_name),
            update.src_inode_id.to_be_bytes(),
        );

        if let Some(parent) = update.updated_src_parent {
            Self::batch_put_inode(&mut batch, cf_inodes, parent)?;
        }
        if let Some(parent) = update.updated_dst_parent {
            Self::batch_put_inode(&mut batch, cf_inodes, parent)?;
        }

        Ok(batch)
    }

    /// Atomically persist rename mutation with apply tracking.
    pub fn rename_atomic(
        &self,
        update: RenameAtomicUpdate<'_>,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        let batch = self.rename_batch(update)?;
        self.commit_authority_batch(batch, raft_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeAttrs;
    use crate::session_registry::CreateFileOperationId;
    use beryl_types::FileLayout;

    use beryl_types::{CallId, ClientId, ContentGeneration, LeaseEpoch, MountId};
    use openraft::{LeaderId, LogId};
    use tempfile::TempDir;
    use uuid::Uuid;

    impl RocksDBStorage {
        /// Persist the authoritative route epoch used for stale-route validation.
        pub fn put_route_epoch(&self, epoch: RouteEpoch) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let value = encode_to_vec(epoch.as_u64(), standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize route_epoch: {}", e)))?;

            db.put_cf(cf, b"route_epoch", value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Set fixture layout through the same inode value used by production.
        pub fn put_layout(&self, inode_id: InodeId, layout: FileLayout) -> MetadataResult<()> {
            let mut inode = self
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound("inode missing".into()))?;
            inode.file_mut()?.layout = layout;
            self.put_inode(&inode)
        }

        /// Put mount entry.
        pub fn put_mount(&self, entry: &MountEntry) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_MOUNTS)
                .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
            let key = format!("{}", entry.mount_id.as_raw());
            let value = encode_to_vec(entry, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize MountEntry: {}", e)))?;

            db.put_cf(cf, key.as_bytes(), value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        pub(crate) fn delete_mount(&self, mount_id: MountId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = Self::cf(db, CF_MOUNTS)?;
            db.delete_cf(cf, format!("{}", mount_id.as_raw()).as_bytes())
                .map_err(|error| MetadataError::Internal(format!("delete test mount: {error}")))
        }

        /// Persist the durable next inode ID allocator value.
        pub fn set_next_inode_id(&self, next_inode_id: InodeId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf_meta = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let value = encode_to_vec(next_inode_id.as_raw(), standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;

            db.put_cf(cf_meta, NEXT_INODE_ID_KEY, value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Seed detached-root authority for state-machine and snapshot tests.
        pub(crate) fn put_detached_root(&self, inode_id: InodeId, detached_root: DetachedRoot) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = Self::cf(db, CF_DETACHED_ROOTS)?;
            db.put_cf(
                cf,
                Self::encode_detached_root_key(inode_id),
                Self::encode_detached_root(&detached_root)?,
            )
            .map_err(|error| MetadataError::Internal(format!("RocksDB error: {error}")))
        }

        /// Seed many empty roots in one test-only RocksDB batch.
        pub(crate) fn put_empty_detached_roots(
            &self,
            first_inode_id: u64,
            count: usize,
            detached_root: DetachedRoot,
        ) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf_inodes = Self::cf(db, CF_INODES)?;
            let cf_detached_roots = Self::cf(db, CF_DETACHED_ROOTS)?;
            let encoded_marker = Self::encode_detached_root(&detached_root)?;
            let mut batch = WriteBatch::default();
            for offset in 0..count {
                let raw = first_inode_id
                    .checked_add(offset as u64)
                    .ok_or_else(|| MetadataError::Internal("test inode range overflow".to_string()))?;
                let inode = Inode::new_dir(InodeId::new(raw), InodeAttrs::new(), detached_root.mount_id);
                Self::batch_put_inode(&mut batch, cf_inodes, &inode)?;
                batch.put_cf(
                    cf_detached_roots,
                    Self::encode_detached_root_key(inode.inode_id),
                    &encoded_marker,
                );
            }
            db.write(batch)
                .map_err(|error| MetadataError::Internal(format!("RocksDB error: {error}")))
        }

        /// Put mount epoch.
        pub fn put_mount_epoch(&self, epoch: u64) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let value = encode_to_vec(epoch, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize mount_epoch: {}", e)))?;

            db.put_cf(cf, b"mount_epoch", value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Put inode.
        pub fn put_inode(&self, inode: &Inode) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_INODES)
                .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
            let key = Self::encode_inode_key(inode.inode_id);
            let value = serde_json::to_vec(inode)
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize Inode: {}", e)))?;

            db.put_cf(cf, &key, value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Seed a semantically corrupt key/value identity pair for state-machine tests.
        pub(crate) fn put_inode_at_storage_key(
            &self,
            storage_key_inode_id: InodeId,
            inode: &Inode,
        ) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_INODES)
                .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
            let key = Self::encode_inode_key(storage_key_inode_id);
            let value = serde_json::to_vec(inode)
                .map_err(|error| MetadataError::Internal(format!("Failed to serialize Inode: {error}")))?;

            db.put_cf(cf, key, value)
                .map_err(|error| MetadataError::Internal(format!("RocksDB error: {error}")))
        }

        /// Put dentry.
        pub fn put_dentry(&self, parent_inode_id: InodeId, name: &str, child_inode_id: InodeId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_DENTRIES)
                .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
            let key = Self::encode_dentry_key(parent_inode_id, name);
            let value = child_inode_id.to_be_bytes();

            db.put_cf(cf, &key, value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }
    }

    fn replay_record(operation: u128, inode_id: u64, expires_at_ms: u64) -> CreateFileReplayRecord {
        let name = format!("file-{inode_id}");
        CreateFileReplayRecord {
            operation_id: CreateFileOperationId {
                client_id: ClientId::new(operation),
                call_id: CallId::from_uuid(Uuid::from_u128(operation)),
            },
            request_deadline_ms: expires_at_ms,
            normalized_path: format!("/{name}"),
            parent_inode_id: InodeId::new(10),
            name: name.clone(),
            inode_id: InodeId::new(inode_id),
            mount_id: MountId::new(1),
            expected_mount_epoch: 1,
            mount_root_inode_id: InodeId::new(10),
            relative_components: vec![name],
            lease_epoch: LeaseEpoch::new(1),
            layout: FileLayout::new(4096),
            generation: ContentGeneration::new(0),
            expires_at_ms,
        }
    }

    fn seed_full_replay_table(storage: &RocksDBStorage, record: &CreateFileReplayRecord) {
        let generation = storage.pin_generation().unwrap();
        let db = generation.db();
        let cf_meta = RocksDBStorage::cf(db, CF_META).unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            cf_meta,
            RocksDBStorage::encode_create_file_replay_key(record.operation_id),
            encode_to_vec(record, standard()).unwrap(),
        );
        batch.put_cf(
            cf_meta,
            RocksDBStorage::encode_create_file_replay_inode_key(record.inode_id),
            RocksDBStorage::encode_create_file_operation_bytes(record.operation_id),
        );
        batch.put_cf(
            cf_meta,
            RocksDBStorage::encode_create_file_replay_expiry_key(record),
            [],
        );
        batch.put_cf(
            cf_meta,
            CREATE_FILE_REPLAY_COUNT_KEY,
            encode_to_vec(MAX_CREATE_FILE_REPLAY_RECORDS, standard()).unwrap(),
        );
        db.write(batch).unwrap();
    }

    fn replay_count(storage: &RocksDBStorage) -> u64 {
        let generation = storage.pin_generation().unwrap();
        let db = generation.db();
        let cf_meta = RocksDBStorage::cf(db, CF_META).unwrap();
        let value = db
            .get_cf(cf_meta, CREATE_FILE_REPLAY_COUNT_KEY)
            .unwrap()
            .expect("replay count");
        decode_from_slice(&value, standard()).unwrap().0
    }

    #[test]
    fn create_file_replay_capacity_rejects_or_replaces_atomically_across_restart() {
        let unexpired_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(unexpired_dir.path()).unwrap();
        let parent_inode_id = InodeId::new(10);
        let parent = Inode::new_dir(parent_inode_id, InodeAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        let retained = replay_record(1, 50, 101);
        seed_full_replay_table(&storage, &retained);
        let rejected = replay_record(2, 11, 200);
        let allocation = storage.prepare_inode_allocation().unwrap();
        let error = storage
            .create_file_atomic(
                allocation,
                parent_inode_id,
                &rejected.name,
                &Inode::new_file(
                    allocation.inode_id,
                    InodeAttrs::new(),
                    MountId::new(1),
                    beryl_types::FileLayout::new(4096),
                ),
                &parent,
                &rejected,
                100,
                &AppMetadataRaftState::default(),
            )
            .unwrap_err();
        assert!(matches!(error, MetadataError::ResourceExhausted(_)));
        assert_eq!(replay_count(&storage), MAX_CREATE_FILE_REPLAY_RECORDS);
        assert_eq!(
            storage.get_create_file_replay(retained.operation_id).unwrap(),
            Some(retained)
        );
        assert!(storage.get_create_file_replay(rejected.operation_id).unwrap().is_none());
        assert_eq!(storage.get_dentry(parent_inode_id, &rejected.name).unwrap(), None);
        assert_eq!(storage.get_next_inode_id().unwrap(), Some(InodeId::new(11)));

        let expired_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(expired_dir.path()).unwrap();
        let parent = Inode::new_dir(parent_inode_id, InodeAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        let expired = replay_record(3, 51, 100);
        seed_full_replay_table(&storage, &expired);
        let replacement = replay_record(4, 11, 200);
        let allocation = storage.prepare_inode_allocation().unwrap();
        storage
            .create_file_atomic(
                allocation,
                parent_inode_id,
                &replacement.name,
                &Inode::new_file(
                    allocation.inode_id,
                    InodeAttrs::new(),
                    MountId::new(1),
                    beryl_types::FileLayout::new(4096),
                ),
                &parent,
                &replacement,
                100,
                &AppMetadataRaftState::default(),
            )
            .unwrap();
        drop(storage);

        let storage = RocksDBStorage::open_existing_for_start(expired_dir.path()).unwrap();
        assert_eq!(replay_count(&storage), MAX_CREATE_FILE_REPLAY_RECORDS);
        assert!(storage.get_create_file_replay(expired.operation_id).unwrap().is_none());
        assert!(storage
            .get_create_file_replay_for_inode(expired.inode_id)
            .unwrap()
            .is_none());
        assert_eq!(
            storage.get_create_file_replay(replacement.operation_id).unwrap(),
            Some(replacement.clone())
        );
        assert_eq!(
            storage.get_create_file_replay_for_inode(replacement.inode_id).unwrap(),
            Some(replacement.clone())
        );
        assert_eq!(
            storage.get_dentry(parent_inode_id, &replacement.name).unwrap(),
            Some(replacement.inode_id)
        );
        assert_eq!(
            storage.get_layout_optional(replacement.inode_id).unwrap(),
            Some(replacement.layout)
        );
        assert_eq!(storage.get_next_inode_id().unwrap(), Some(InodeId::new(12)));
    }

    #[test]
    fn create_file_atomic_rejects_a_target_installed_after_allocation_preparation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let parent_inode_id = InodeId::new(10);
        let parent = Inode::new_dir(parent_inode_id, InodeAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        let allocation = storage.prepare_inode_allocation().unwrap();
        let existing = Inode::new_file(
            allocation.inode_id,
            InodeAttrs::new(),
            MountId::new(1),
            beryl_types::FileLayout::new(4096),
        );
        storage.put_inode(&existing).unwrap();
        let applied_before = storage.load_raft_state().unwrap();
        let rejected_applied_state = AppMetadataRaftState {
            last_applied_log_id: Some(LogId::new(LeaderId::new(9, 1), 901)),
            ..AppMetadataRaftState::default()
        };
        let replay_record = replay_record(1, allocation.inode_id.as_raw(), 100);

        let error = storage
            .create_file_atomic(
                allocation,
                parent_inode_id,
                &replay_record.name,
                &Inode::new_file(
                    allocation.inode_id,
                    InodeAttrs::new(),
                    MountId::new(1),
                    beryl_types::FileLayout::new(4096),
                ),
                &parent,
                &replay_record,
                1,
                &rejected_applied_state,
            )
            .unwrap_err();

        assert!(error.to_string().contains("already exists"));
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
        assert_eq!(storage.get_inode(allocation.inode_id).unwrap(), Some(existing));
        assert_eq!(storage.get_dentry(parent_inode_id, &replay_record.name).unwrap(), None);
        assert_eq!(storage.get_next_inode_id().unwrap(), Some(allocation.inode_id));
    }
}
