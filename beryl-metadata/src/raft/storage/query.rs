// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl RocksDBStorage {
    /// Load the durable result for one atomic CreateFile operation identity.
    pub(crate) fn get_create_file_replay(
        &self,
        operation_id: CreateFileOperationId,
    ) -> MetadataResult<Option<CreateFileReplayRecord>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_META)?;
        let key = Self::encode_create_file_replay_key(operation_id);
        let Some(value) = db
            .get_cf(cf, key)
            .map_err(|error| MetadataError::Internal(format!("Failed to read CreateFile replay record: {error}")))?
        else {
            return Ok(None);
        };
        let (record, consumed): (CreateFileReplayRecord, usize) = decode_from_slice(&value, standard())
            .map_err(|error| MetadataError::Internal(format!("Failed to decode CreateFile replay record: {error}")))?;
        if consumed != value.len() || record.operation_id != operation_id {
            return Err(MetadataError::Internal(
                "CreateFile replay record identity is corrupt".to_string(),
            ));
        }
        Ok(Some(record))
    }

    /// Load the replay record that temporarily reserves one newly created inode.
    pub(crate) fn get_create_file_replay_for_inode(
        &self,
        inode_id: InodeId,
    ) -> MetadataResult<Option<CreateFileReplayRecord>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_META)?;
        let key = Self::encode_create_file_replay_inode_key(inode_id);
        let Some(value) = db.get_cf(cf, key).map_err(|error| {
            MetadataError::Internal(format!("Failed to read CreateFile inode replay index: {error}"))
        })?
        else {
            return Ok(None);
        };
        let operation_id = Self::decode_create_file_operation_bytes(&value)?;
        let record = self
            .get_create_file_replay(operation_id)?
            .ok_or_else(|| MetadataError::Internal("CreateFile inode replay index has no record".to_string()))?;
        if record.inode_id != inode_id {
            return Err(MetadataError::Internal(
                "CreateFile inode replay index names another inode".to_string(),
            ));
        }
        Ok(Some(record))
    }

    /// Get the authoritative route epoch used for stale-route validation.
    pub fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match db.get_cf(cf, b"route_epoch") {
            Ok(Some(value)) => {
                let version: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize route_epoch: {}", e)))?
                    .0;
                Ok(RouteEpoch::new(version))
            }
            Ok(None) => Ok(RouteEpoch::new(1)), // Default epoch
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Load the layout for a specific inode.
    pub fn get_layout(&self, inode_id: InodeId) -> MetadataResult<FileLayout> {
        let _generation = self.pin_generation()?;
        self.get_layout_optional(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Layout not found for inode {}", inode_id)))
    }

    pub(crate) fn get_layout_optional(&self, inode_id: InodeId) -> MetadataResult<Option<FileLayout>> {
        self.get_inode(inode_id)?
            .map(|inode| match inode.kind {
                crate::inode::InodeKind::File(file) => {
                    file.layout
                        .validate()
                        .map_err(|error| MetadataError::Internal(format!("invalid file layout: {error}")))?;
                    Ok(Some(file.layout))
                }
                crate::inode::InodeKind::Dir => Ok(None),
            })
            .transpose()
            .map(Option::flatten)
    }

    fn get_meta_u64_optional(&self, key: &[u8], label: &str) -> MetadataResult<Option<u64>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_META)?;
        match db.get_cf(cf, key) {
            Ok(Some(value)) => decode_from_slice(&value, standard())
                .map(|decoded: (u64, usize)| Some(decoded.0))
                .map_err(|error| MetadataError::Internal(format!("Failed to deserialize {label}: {error}"))),
            Ok(None) => Ok(None),
            Err(error) => Err(MetadataError::Internal(format!(
                "RocksDB error reading {label}: {error}"
            ))),
        }
    }

    pub(crate) fn bootstrap_namespace_state(
        &self,
        expected_group_name: &GroupName,
    ) -> MetadataResult<BootstrapNamespaceState> {
        let _generation = self.pin_generation()?;
        let root_inode = self.get_inode(crate::mount::ROOT_INODE_ID)?;
        let mounts = self.list_mounts()?;
        let route_epoch = self.get_meta_u64_optional(b"route_epoch", "route_epoch")?;
        let mount_epoch = self.get_meta_u64_optional(b"mount_epoch", "mount_epoch")?;
        let next_inode = self.get_next_inode_id()?;
        let namespace_has_any_state = root_inode.is_some()
            || !mounts.is_empty()
            || route_epoch.is_some()
            || mount_epoch.is_some()
            || next_inode.is_some()
            || self.max_inode_id()?.is_some();
        if !namespace_has_any_state {
            return Ok(BootstrapNamespaceState::Empty);
        }

        let matching_inode = root_inode.as_ref().is_some_and(|inode| {
            inode.inode_id == crate::mount::ROOT_INODE_ID
                && inode.file_type().is_dir()
                && matches!(inode.kind, crate::inode::InodeKind::Dir)
                && inode.mount_id == MountId::new(1)
        });
        let matching_mount = mounts.len() == 1
            && mounts.first().is_some_and(|mount| {
                mount.mount_id == MountId::new(1)
                    && mount.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX
                    && mount.mount_kind == crate::mount::MountKind::Internal
                    && mount.ufs_uri.is_none()
                    && mount.data_io_policy == crate::mount::DataIoPolicy::Allow
                    && mount.mount_epoch == 1
                    && mount.namespace_owner_group_name == *expected_group_name
                    && mount.root_inode_id == crate::mount::ROOT_INODE_ID
            });
        if matching_inode
            && matching_mount
            && self.max_inode_id()? == Some(crate::mount::ROOT_INODE_ID)
            && route_epoch == Some(1)
            && mount_epoch == Some(1)
            && next_inode == Some(InodeId::new(2))
        {
            Ok(BootstrapNamespaceState::Matching)
        } else {
            Ok(BootstrapNamespaceState::Conflicting)
        }
    }

    /// Read allocator state without consuming an inode ID.
    pub(crate) fn prepare_inode_allocation(&self) -> MetadataResult<InodeAllocation> {
        let _generation = self.pin_generation()?;
        let inode_id = self.get_next_inode_id()?.ok_or_else(|| {
            MetadataError::Internal(
                "next_inode_id allocator authority is missing; reformat metadata storage".to_string(),
            )
        })?;
        if inode_id.as_raw() < 2 {
            return Err(MetadataError::Internal(format!(
                "next_inode_id allocator authority is invalid: {inode_id}; reformat metadata storage"
            )));
        }
        let max_inode_id = self.max_inode_id()?.ok_or_else(|| {
            MetadataError::Internal(
                "next_inode_id allocator exists without inode authority; reformat metadata storage".to_string(),
            )
        })?;
        if inode_id.as_raw() <= max_inode_id.as_raw() || self.get_inode(inode_id)?.is_some() {
            return Err(MetadataError::Internal(format!(
                "next_inode_id allocator {inode_id} is not ahead of inode authority {max_inode_id}; reformat metadata storage"
            )));
        }
        let next_raw = inode_id
            .as_raw()
            .checked_add(1)
            .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
        Ok(InodeAllocation {
            inode_id,
            next_inode_id: InodeId::new(next_raw),
        })
    }

    /// Read the durable next inode ID allocator value.
    pub fn get_next_inode_id(&self) -> MetadataResult<Option<InodeId>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_meta = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match db.get_cf(cf_meta, NEXT_INODE_ID_KEY) {
            Ok(Some(value)) => {
                let id: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize next_inode_id: {}", e)))?
                    .0;
                Ok(Some(InodeId::new(id)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Get one mount entry by its authority-local mount ID.
    pub fn get_mount(&self, mount_id: MountId) -> MetadataResult<Option<MountEntry>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
        let key = mount_id.as_raw().to_string();

        match db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let entry: MountEntry = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize MountEntry: {}", e)))?
                    .0;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// List all mount entries.
    pub fn list_mounts(&self) -> MetadataResult<Vec<MountEntry>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;

        let mut mounts = Vec::new();
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let entry: MountEntry = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize MountEntry: {}", e)))?
                .0;
            mounts.push(entry);
        }

        Ok(mounts)
    }

    /// Get one detached-root authority marker.
    pub(crate) fn get_detached_root(&self, inode_id: InodeId) -> MetadataResult<Option<DetachedRoot>> {
        crate::observe::record_rocksdb_read("detached_root");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_DETACHED_ROOTS)?;
        let key = Self::encode_detached_root_key(inode_id);
        match db.get_cf(cf, key) {
            Ok(Some(value)) => Self::decode_detached_root(inode_id, &value).map(Some),
            Ok(None) => Ok(None),
            Err(error) => Err(MetadataError::Internal(format!(
                "RocksDB error reading DetachedRoot for inode {inode_id}: {error}"
            ))),
        }
    }

    /// Select a key-ordered bounded page of detached roots for maintenance.
    pub(crate) fn list_detached_roots(
        &self,
        max_entries: usize,
    ) -> MetadataResult<(Vec<(InodeId, DetachedRoot)>, bool)> {
        if max_entries == 0 {
            return Err(MetadataError::InvalidArgument(
                "detached-root listing requires a positive entry limit".to_string(),
            ));
        }
        crate::observe::record_rocksdb_read("detached_root_scan");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_DETACHED_ROOTS)?;
        let mut roots = Vec::with_capacity(max_entries);
        let mut has_more = false;
        for item in db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
            let (key, value) =
                item.map_err(|error| MetadataError::Internal(format!("RocksDB detached-root scan failed: {error}")))?;
            if roots.len() == max_entries {
                has_more = true;
                break;
            }
            let inode_id = Self::decode_detached_root_key(&key)?;
            let detached_root = Self::decode_detached_root(inode_id, &value)?;
            roots.push((inode_id, detached_root));
        }
        Ok((roots, has_more))
    }

    pub fn prepare_worker_registration(
        &self,
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    ) -> MetadataResult<WorkerInfo> {
        let _generation = self.pin_generation()?;
        if worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }
        Ok(WorkerInfo {
            group_name,
            worker_id,
            address,
            worker_net_protocol,
            capacity_total: 0,
            capacity_used: 0,
            capacity_available: 0,
            active_reads: 0,
            active_writes: 0,
            health: crate::worker::HealthStatus::Healthy,
            last_heartbeat: 0,
            fault_domain,
        })
    }

    /// List all workers.
    pub fn list_workers(&self) -> MetadataResult<Vec<WorkerInfo>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_WORKERS)
            .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;

        let mut workers = Vec::new();
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let info: WorkerInfo = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize WorkerInfo: {}", e)))?
                .0;
            workers.push(info);
        }

        Ok(workers)
    }

    /// Get inode by ID.
    pub fn get_inode(&self, inode_id: InodeId) -> MetadataResult<Option<Inode>> {
        crate::observe::record_rocksdb_read("inode");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
        let key = Self::encode_inode_key(inode_id);

        match db.get_cf(cf, &key) {
            Ok(Some(value)) => {
                let inode: Inode = serde_json::from_slice(&value)
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize Inode: {}", e)))?;
                Ok(Some(inode))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Return the largest inode ID currently present in storage.
    pub fn max_inode_id(&self) -> MetadataResult<Option<InodeId>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;

        let Some(item) = db.iterator_cf(cf, rocksdb::IteratorMode::End).next() else {
            return Ok(None);
        };
        let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error (inodes): {e}")))?;
        let key = key.as_ref();
        if !key.starts_with(b"inode/") || key.len() != b"inode/".len() + 8 {
            return Err(MetadataError::Internal(format!(
                "invalid inode authority key at the allocator high watermark: {:?}",
                key
            )));
        }
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&key[b"inode/".len()..]);
        let inode_id = InodeId::new(u64::from_be_bytes(raw));
        if inode_id.as_raw() == 0 {
            return Err(MetadataError::Internal(
                "inode authority contains zero at the allocator high watermark".to_string(),
            ));
        }
        Ok(Some(inode_id))
    }

    /// Decode dentry key: extract parent_inode_id and name
    fn decode_dentry_key(key: &[u8]) -> Option<(InodeId, String)> {
        if !key.starts_with(b"dentry/") {
            return None;
        }
        let prefix_len = b"dentry/".len();
        if key.len() < prefix_len + 8 {
            return None;
        }
        let parent_bytes: [u8; 8] = key[prefix_len..prefix_len + 8].try_into().ok()?;
        let parent_inode_id = InodeId::from_be_bytes(parent_bytes);
        let name_bytes = &key[prefix_len + 8..];
        let name = String::from_utf8(name_bytes.to_vec()).ok()?;
        Some((parent_inode_id, name))
    }

    /// Get dentry (parent_inode_id, name) -> child_inode_id
    pub fn get_dentry(&self, parent_inode_id: InodeId, name: &str) -> MetadataResult<Option<InodeId>> {
        crate::observe::record_rocksdb_read("dentry");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
        let key = Self::encode_dentry_key(parent_inode_id, name);

        match db.get_cf(cf, &key) {
            Ok(Some(value)) => {
                if value.len() != 8 {
                    return Err(MetadataError::Internal(format!(
                        "Invalid dentry value length: {}",
                        value.len()
                    )));
                }
                let mut child_bytes = [0u8; 8];
                child_bytes.copy_from_slice(&value[..8]);
                let child_inode_id = InodeId::from_be_bytes(child_bytes);
                Ok(Some(child_inode_id))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Read a bounded first page for destructive detached-root reclamation.
    ///
    /// Unlike user-facing directory listing, malformed keys and values are
    /// fatal here because skipping one could retire a non-empty root.
    pub(crate) fn list_dentries_for_reclaim(
        &self,
        parent_inode_id: InodeId,
        max_entries: usize,
    ) -> MetadataResult<(Vec<(String, InodeId)>, bool)> {
        if max_entries == 0 {
            return Err(MetadataError::InvalidArgument(
                "detached-root dentry scan requires a positive entry limit".to_string(),
            ));
        }
        crate::observe::record_rocksdb_read("detached_root_dentry_scan");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_DENTRIES)?;
        let prefix = Self::encode_dentry_key(parent_inode_id, "");
        let mut entries = Vec::with_capacity(max_entries);
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward));
        for item in iter {
            let (key, value) =
                item.map_err(|error| MetadataError::Internal(format!("RocksDB dentry scan failed: {error}")))?;
            if !key.starts_with(&prefix) {
                break;
            }
            if entries.len() == max_entries {
                return Ok((entries, false));
            }
            let (decoded_parent, name) = Self::decode_dentry_key(&key).ok_or_else(|| {
                MetadataError::Internal(format!("Malformed dentry key under detached root {parent_inode_id}"))
            })?;
            if decoded_parent != parent_inode_id || value.len() != 8 {
                return Err(MetadataError::Internal(format!(
                    "Malformed dentry under detached root {parent_inode_id}"
                )));
            }
            let child_raw: [u8; 8] = value.as_ref().try_into().expect("dentry value length was checked");
            entries.push((name, InodeId::from_be_bytes(child_raw)));
        }
        Ok((entries, true))
    }

    /// Returns one bounded dentry page for a directory.
    ///
    /// `cursor_key` is normally the opaque key of the last entry returned to
    /// the caller. It must be syntactically valid for this directory, but it is
    /// not authenticated; a valid same-directory key acts as an exclusive seek
    /// position even when that exact key no longer exists.
    ///
    /// The scan is weakly consistent across calls and does not retain a RocksDB
    /// iterator or snapshot after this method returns. Entries inserted at or
    /// before the cursor may be omitted, while entries inserted after it may be
    /// returned. `max_entries` must be positive; successful pages never contain
    /// more entries than that bound.
    pub fn list_dentries_with_cursor(
        &self,
        parent_inode_id: InodeId,
        cursor_key: Option<&[u8]>,
        max_entries: usize,
    ) -> MetadataResult<DentryPage> {
        if max_entries == 0 {
            return Err(MetadataError::InvalidArgument(
                "ListStatus page size must be positive".to_string(),
            ));
        }
        crate::observe::record_rocksdb_read("dentry_scan");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;

        let prefix = Self::encode_dentry_key(parent_inode_id, "");

        let start_key = match cursor_key {
            Some(cursor) => match Self::decode_dentry_key(cursor) {
                Some((cursor_parent, name)) if cursor_parent == parent_inode_id && !name.is_empty() => cursor.to_vec(),
                _ => {
                    return Err(MetadataError::InvalidArgument(
                        "ListStatus cursor does not belong to the requested directory".to_string(),
                    ));
                }
            },
            None => prefix.clone(),
        };
        let mut skip_cursor = cursor_key.is_some();

        let mut entries = Vec::new();
        let mut iter = db.iterator_cf(cf, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));

        while let Some(item) = iter.next() {
            let (key, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            // Check if key still matches prefix (parent_inode_id)
            if !key.starts_with(&prefix) {
                break; // finished this directory
            }

            // A deleted cursor key seeks directly to its successor, which must
            // remain visible instead of being mistaken for the cursor itself.
            if skip_cursor {
                skip_cursor = false;
                if key.as_ref() == start_key.as_slice() {
                    continue;
                }
            }

            let (decoded_parent, name) = Self::decode_dentry_key(&key).ok_or_else(|| {
                MetadataError::Internal(format!("Malformed dentry key under parent inode {parent_inode_id}"))
            })?;
            if decoded_parent != parent_inode_id || name.is_empty() || value.len() != 8 {
                return Err(MetadataError::Internal(format!(
                    "Malformed dentry under parent inode {parent_inode_id}"
                )));
            }

            let mut child_bytes = [0u8; 8];
            child_bytes.copy_from_slice(&value[..8]);
            let child_inode_id = InodeId::from_be_bytes(child_bytes);
            entries.push((name, child_inode_id));

            if entries.len() == max_entries {
                // Peek ahead to know if another page exists; only set cursor when there is more.
                let has_more = if let Some(next_item) = iter.next() {
                    let (next_key, _) =
                        next_item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
                    next_key.starts_with(&prefix)
                } else {
                    false
                };
                let next_cursor_key = if has_more { Some(key.to_vec()) } else { None };
                return Ok((entries, next_cursor_key, !has_more));
            }
        }
        Ok((entries, None, true))
    }

    /// Check if directory is empty (has no dentries).
    pub fn is_directory_empty(&self, parent_inode_id: InodeId) -> MetadataResult<bool> {
        let _generation = self.pin_generation()?;
        let (entries, _, _) = self.list_dentries_with_cursor(parent_inode_id, None, 1)?;
        Ok(entries.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeAttrs;

    use tempfile::TempDir;

    impl RocksDBStorage {
        /// Get worker info accepted by a metadata group.
        pub fn get_worker_in_group(
            &self,
            group_name: &GroupName,
            worker_id: WorkerId,
        ) -> MetadataResult<Option<WorkerInfo>> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_WORKERS)
                .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;
            let key = worker_key(group_name, worker_id);

            match db.get_cf(cf, key.as_bytes()) {
                Ok(Some(value)) => {
                    let info: WorkerInfo = decode_from_slice(&value, standard())
                        .map_err(|e| MetadataError::Internal(format!("Failed to deserialize WorkerInfo: {}", e)))?
                        .0;
                    Ok(Some(info))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
            }
        }
    }

    fn setup_dir_with_entries(parent_inode_id: InodeId, entries: &[(&str, InodeId)]) -> (TempDir, RocksDBStorage) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_dentries");
        let storage = RocksDBStorage::create_for_format(&db_path).unwrap();

        // Create parent dir and some child nodes.
        let parent_inode = Inode::new_dir(parent_inode_id, InodeAttrs::new(), MountId::new(1));
        storage.put_inode(&parent_inode).unwrap();

        for (name, child) in entries {
            storage.put_dentry(parent_inode_id, name, *child).unwrap();
        }

        (temp_dir, storage)
    }

    fn put_numbered_dentries(storage: &RocksDBStorage, parent_inode_id: InodeId, count: usize) {
        storage
            .with_pinned_db(|db| {
                let cf = db
                    .cf_handle(CF_DENTRIES)
                    .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
                let mut batch = WriteBatch::default();
                for index in 0..count {
                    let name = format!("{index:06}");
                    let child_inode_id = InodeId::new(index as u64 + 2);
                    batch.put_cf(
                        cf,
                        RocksDBStorage::encode_dentry_key(parent_inode_id, &name),
                        child_inode_id.to_be_bytes(),
                    );
                }
                db.write(batch)
                    .map_err(|error| MetadataError::Internal(format!("RocksDB batch write failed: {error}")))?;
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn list_dentries_with_cursor_fails_closed_on_malformed_dentry() {
        let (_tmp_dir, storage) = setup_dir_with_entries(InodeId::new(1), &[]);
        storage
            .with_pinned_db(|db| {
                let cf = db
                    .cf_handle(CF_DENTRIES)
                    .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
                db.put_cf(cf, RocksDBStorage::encode_dentry_key(InodeId::new(1), "bad"), b"x")
                    .map_err(|error| MetadataError::Internal(format!("RocksDB write failed: {error}")))?;
                Ok(())
            })
            .unwrap();

        let error = storage
            .list_dentries_with_cursor(InodeId::new(1), None, 10)
            .expect_err("malformed authority must fail closed");
        assert!(matches!(error, MetadataError::Internal(_)));
    }

    #[test]
    fn list_dentries_with_cursor_fails_closed_on_malformed_dentry_key() {
        let (_tmp_dir, storage) = setup_dir_with_entries(InodeId::new(1), &[]);
        storage
            .with_pinned_db(|db| {
                let cf = db
                    .cf_handle(CF_DENTRIES)
                    .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
                let mut key = RocksDBStorage::encode_dentry_key(InodeId::new(1), "");
                key.push(0xff);
                db.put_cf(cf, key, InodeId::new(2).to_be_bytes())
                    .map_err(|error| MetadataError::Internal(format!("RocksDB write failed: {error}")))?;
                Ok(())
            })
            .unwrap();

        let error = storage
            .list_dentries_with_cursor(InodeId::new(1), None, 10)
            .expect_err("malformed authority keys must fail closed");
        assert!(matches!(error, MetadataError::Internal(_)));
    }

    #[test]
    fn list_dentries_with_cursor_pages_one_hundred_thousand_entries() {
        const ENTRY_COUNT: usize = 100_000;
        const PAGE_SIZE: usize = 1_000;

        let parent_inode_id = InodeId::new(1);
        let (_tmp_dir, storage) = setup_dir_with_entries(parent_inode_id, &[]);
        put_numbered_dentries(&storage, parent_inode_id, ENTRY_COUNT);

        let mut cursor = None;
        let mut expected_index = 0;
        loop {
            let (page, next_cursor, eof) = storage
                .list_dentries_with_cursor(parent_inode_id, cursor.as_deref(), PAGE_SIZE)
                .unwrap();
            assert!(!page.is_empty());
            assert!(page.len() <= PAGE_SIZE);
            for (name, child_inode_id) in page {
                assert_eq!(name, format!("{expected_index:06}"));
                assert_eq!(child_inode_id, InodeId::new(expected_index as u64 + 2));
                expected_index += 1;
            }
            assert_eq!(next_cursor.is_none(), eof);
            if eof {
                break;
            }
            cursor = next_cursor;
        }

        assert_eq!(expected_index, ENTRY_COUNT);
    }
}
