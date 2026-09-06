// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Deterministic, bounded reclamation of unreachable namespace roots.

use super::*;
use crate::path_resolver::MAX_PATH_COMPONENT_BYTES;
use crate::raft::{
    MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES, MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
    MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
};
use std::collections::BTreeSet;

impl AppRaftStateMachine {
    /// Apply one bounded reclamation batch from a leader-selected root set.
    ///
    /// Marker absence is an idempotent no-op. Every marker that is present is
    /// validated with its inode, mount, descendants, and layouts before
    /// one authority batch is published.
    pub(super) fn apply_reclaim_detached_roots(
        &self,
        candidate_root_inode_ids: Vec<InodeId>,
        max_entries: u32,
        max_batch_bytes: u32,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<DetachedRootReclaimResult> {
        Self::validate_detached_root_reclaim_command(&candidate_root_inode_ids, max_entries, max_batch_bytes)?;

        let max_entries = max_entries as usize;
        let max_batch_bytes = max_batch_bytes as usize;
        let mut update = DetachedRootReclaimUpdate::default();
        let mut logical_batch_bytes = update.logical_batch_bytes(raft_state)?;
        if logical_batch_bytes > max_batch_bytes {
            return Err(MetadataError::Internal(format!(
                "Raft apply state requires {logical_batch_bytes} logical bytes, exceeding detached-root batch budget {max_batch_bytes}"
            )));
        }

        let mut seen_candidates = BTreeSet::new();
        let mut planned_entry_inode_ids = BTreeSet::new();
        let mut processed_entries = 0usize;
        let mut created_roots = 0usize;

        'candidates: for root_inode_id in candidate_root_inode_ids {
            if !seen_candidates.insert(root_inode_id) {
                continue;
            }
            let Some(detached_root) = self.storage.get_detached_root(root_inode_id)? else {
                continue;
            };
            let mount_root_inode_id = self.validate_detached_root(root_inode_id, detached_root)?;
            let scan_limit = max_entries.saturating_sub(processed_entries).max(1);
            let (entries, eof) = self.storage.list_dentries_for_reclaim(root_inode_id, scan_limit)?;
            let mut consumed_page = true;

            for (name, child_inode_id) in entries {
                if processed_entries == max_entries {
                    consumed_page = false;
                    break;
                }
                if !planned_entry_inode_ids.insert(child_inode_id) {
                    return Err(MetadataError::Internal(format!(
                        "detached-root forest contains duplicate inode {child_inode_id}"
                    )));
                }
                let entry = self.prepare_detached_root_entry(
                    root_inode_id,
                    name,
                    child_inode_id,
                    detached_root,
                    mount_root_inode_id,
                )?;
                let entry_logical_bytes = entry.logical_bytes()?;
                let next_logical_bytes = logical_batch_bytes
                    .checked_add(entry_logical_bytes)
                    .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
                if next_logical_bytes > max_batch_bytes {
                    if update.entries.is_empty() && update.completed_root_inode_ids.is_empty() {
                        return Err(MetadataError::Internal(format!(
                            "detached-root entry {child_inode_id} requires {next_logical_bytes} logical bytes, exceeding batch budget {max_batch_bytes}"
                        )));
                    }
                    break 'candidates;
                }

                created_roots += usize::from(entry.child_detached_root.is_some());
                update.entries.push(entry);
                processed_entries += 1;
                logical_batch_bytes = next_logical_bytes;
            }

            if consumed_page && eof {
                let completion_bytes = DetachedRootReclaimUpdate::completed_root_logical_bytes(root_inode_id)?;
                let next_logical_bytes = logical_batch_bytes
                    .checked_add(completion_bytes)
                    .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
                if next_logical_bytes > max_batch_bytes {
                    if update.entries.is_empty() && update.completed_root_inode_ids.is_empty() {
                        return Err(MetadataError::Internal(format!(
                            "detached-root completion for inode {root_inode_id} requires {next_logical_bytes} logical bytes, exceeding batch budget {max_batch_bytes}"
                        )));
                    }
                    break;
                }
                update.completed_root_inode_ids.push(root_inode_id);
                logical_batch_bytes = next_logical_bytes;
            }
        }

        let verified_logical_bytes = update.logical_batch_bytes(raft_state)?;
        if verified_logical_bytes != logical_batch_bytes {
            return Err(MetadataError::Internal(format!(
                "detached-root logical byte accounting diverged: prepared={logical_batch_bytes}, verified={verified_logical_bytes}"
            )));
        }
        let completed_roots = update.completed_root_inode_ids.len();
        self.storage.reclaim_detached_roots_atomic(update, raft_state)?;

        Ok(DetachedRootReclaimResult {
            processed_entries: u32::try_from(processed_entries)
                .expect("processed entries are bounded by a u32 protocol limit"),
            completed_roots: u32::try_from(completed_roots)
                .expect("candidate roots are bounded by a u32 protocol limit"),
            created_roots: u32::try_from(created_roots).expect("created roots are bounded by a u32 protocol limit"),
            logical_batch_bytes: u32::try_from(logical_batch_bytes)
                .expect("logical bytes are bounded by a u32 protocol limit"),
        })
    }

    fn validate_detached_root_reclaim_command(
        candidate_root_inode_ids: &[InodeId],
        max_entries: u32,
        max_batch_bytes: u32,
    ) -> MetadataResult<()> {
        if candidate_root_inode_ids.is_empty()
            || candidate_root_inode_ids.len() > MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize
            || candidate_root_inode_ids.iter().any(|inode_id| inode_id.as_raw() == 0)
        {
            return Err(MetadataError::Internal(format!(
                "invalid detached-root candidate count or identity: count={}, maximum={MAX_RECLAIM_DETACHED_ROOT_CANDIDATES}",
                candidate_root_inode_ids.len()
            )));
        }
        if max_entries == 0 || max_entries > MAX_RECLAIM_DETACHED_ROOT_ENTRIES {
            return Err(MetadataError::Internal(format!(
                "invalid detached-root entry budget {max_entries}; maximum={MAX_RECLAIM_DETACHED_ROOT_ENTRIES}"
            )));
        }
        if !(MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES..=MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES).contains(&max_batch_bytes) {
            return Err(MetadataError::Internal(format!(
                "invalid detached-root byte budget {max_batch_bytes}; range={MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES}..={MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES}"
            )));
        }
        Ok(())
    }

    /// Validate the marker-to-root relationship and return its active mount root.
    fn validate_detached_root(&self, root_inode_id: InodeId, detached_root: DetachedRoot) -> MetadataResult<InodeId> {
        let root_inode = self.storage.get_inode(root_inode_id)?.ok_or_else(|| {
            MetadataError::Internal(format!("DetachedRoot for inode {root_inode_id} has no directory inode"))
        })?;
        if root_inode.inode_id != root_inode_id {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode key {root_inode_id} contains inode {}",
                root_inode.inode_id
            )));
        }
        if !root_inode.file_type().is_dir() {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} is not a directory"
            )));
        }

        if root_inode.mount_id != detached_root.mount_id {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} belongs to mount {}, marker names mount {}",
                root_inode.mount_id, detached_root.mount_id
            )));
        }
        let mount = self.storage.get_mount(detached_root.mount_id)?.ok_or_else(|| {
            MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} references missing mount {}",
                detached_root.mount_id
            ))
        })?;
        if mount.mount_id != detached_root.mount_id {
            return Err(MetadataError::Internal(format!(
                "mount key {} contains mount {} while reclaiming inode {root_inode_id}",
                detached_root.mount_id, mount.mount_id
            )));
        }
        if mount.root_inode_id == root_inode_id {
            return Err(MetadataError::Internal(format!(
                "mount root inode {root_inode_id} cannot be a DetachedRoot"
            )));
        }
        Ok(mount.root_inode_id)
    }

    fn prepare_detached_root_entry(
        &self,
        parent_inode_id: InodeId,
        name: String,
        child_inode_id: InodeId,
        parent_detached_root: DetachedRoot,
        mount_root_inode_id: InodeId,
    ) -> MetadataResult<DetachedRootReclaimEntry> {
        if name.is_empty() || name.len() > MAX_PATH_COMPONENT_BYTES || name.contains('/') || name.contains('\0') {
            return Err(MetadataError::Internal(format!(
                "invalid dentry name under DetachedRoot inode {parent_inode_id}"
            )));
        }
        if child_inode_id.as_raw() == 0 {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {parent_inode_id} references zero child inode"
            )));
        }
        if self.storage.get_detached_root(child_inode_id)?.is_some() {
            return Err(MetadataError::Internal(format!(
                "inode {child_inode_id} is both reachable from DetachedRoot {parent_inode_id} and independently detached"
            )));
        }
        let child_inode = self.storage.get_inode(child_inode_id)?.ok_or_else(|| {
            MetadataError::Internal(format!(
                "DetachedRoot inode {parent_inode_id} references missing child inode {child_inode_id}"
            ))
        })?;
        if child_inode.inode_id != child_inode_id {
            return Err(MetadataError::Internal(format!(
                "inode key {child_inode_id} under DetachedRoot {parent_inode_id} contains inode {}",
                child_inode.inode_id
            )));
        }
        if child_inode.mount_id != parent_detached_root.mount_id {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {parent_inode_id} crosses from mount {} to child inode {child_inode_id} in mount {}",
                parent_detached_root.mount_id, child_inode.mount_id
            )));
        }

        let child_detached_root = match child_inode.kind {
            InodeKind::Dir => {
                if child_inode_id == mount_root_inode_id {
                    return Err(MetadataError::Internal(format!(
                        "DetachedRoot inode {parent_inode_id} reaches mount root inode {child_inode_id}"
                    )));
                }
                Some(parent_detached_root)
            }
            InodeKind::File(crate::inode::FileData { .. }) => None,
        };

        Ok(DetachedRootReclaimEntry {
            parent_inode_id,
            name,
            inode_id: child_inode_id,
            child_detached_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeKind;
    use crate::raft::state_machine::tests::*;

    fn new_state_machine() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(state_machine.apply(bootstrap_command("root", 1)).unwrap());
        (dir, storage, state_machine)
    }

    fn detached_root(mount_id: MountId, detached_at_ms: u64) -> DetachedRoot {
        DetachedRoot {
            mount_id,
            detached_at_ms,
        }
    }

    fn seed_directory(storage: &RocksDBStorage, inode_id: InodeId, mount_id: MountId) {
        storage
            .put_inode(&Inode::new_dir(inode_id, InodeAttrs::new(), mount_id))
            .unwrap();
    }

    fn seed_file(storage: &RocksDBStorage, parent_inode_id: InodeId, name: &str, inode_id: InodeId, mount_id: MountId) {
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                InodeAttrs::new(),
                mount_id,
                beryl_types::FileLayout::new(4096),
            ))
            .unwrap();
        storage.put_dentry(parent_inode_id, name, inode_id).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096)).unwrap();
    }

    fn reclaim(
        state_machine: &AppRaftStateMachine,
        candidates: Vec<InodeId>,
        max_entries: u32,
    ) -> MetadataResult<DetachedRootReclaimResult> {
        reclaim_with_budget(
            state_machine,
            candidates,
            max_entries,
            MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
        )
    }

    fn reclaim_with_budget(
        state_machine: &AppRaftStateMachine,
        candidates: Vec<InodeId>,
        max_entries: u32,
        max_batch_bytes: u32,
    ) -> MetadataResult<DetachedRootReclaimResult> {
        match state_machine.apply(Command::ReclaimDetachedRoots {
            candidate_root_inode_ids: candidates,
            max_entries,
            max_batch_bytes,
        })? {
            ApplySuccess::DetachedRootsReclaimed(result) => Ok(result),
            other => panic!("unexpected reclaim response: {other:?}"),
        }
    }

    #[test]
    fn mixed_tree_reclaims_in_bounded_batches_and_inherits_detach_age() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(10);
        let child_dir_id = InodeId::new(11);
        let file_id = InodeId::new(12);
        let second_file_id = InodeId::new(13);
        let marker = detached_root(MountId::new(1), 77);
        seed_directory(&storage, root_id, marker.mount_id);
        seed_directory(&storage, child_dir_id, marker.mount_id);
        storage.put_dentry(root_id, "a", child_dir_id).unwrap();
        seed_file(&storage, root_id, "b", file_id, marker.mount_id);
        storage
            .put_inode(&Inode::new_file(
                second_file_id,
                InodeAttrs::new(),
                marker.mount_id,
                beryl_types::FileLayout::new(4096),
            ))
            .unwrap();
        storage.put_layout(second_file_id, FileLayout::new(4096)).unwrap();
        storage.put_dentry(root_id, "c", second_file_id).unwrap();
        storage.put_detached_root(root_id, marker).unwrap();

        let first = reclaim(&state_machine, vec![root_id], 2).unwrap();
        assert_eq!(first.processed_entries, 2);
        assert_eq!(first.created_roots, 1);
        assert_eq!(first.completed_roots, 0);
        assert!(first.logical_batch_bytes <= MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES);
        assert_eq!(storage.get_detached_root(child_dir_id).unwrap(), Some(marker));
        assert!(storage.get_inode(file_id).unwrap().is_none());
        assert!(storage.get_layout_optional(file_id).unwrap().is_none());
        assert!(storage.get_inode(root_id).unwrap().is_some());

        let second = reclaim(&state_machine, vec![root_id], 2).unwrap();
        assert_eq!(second.processed_entries, 1);
        assert_eq!(second.completed_roots, 1);
        assert!(storage.get_inode(root_id).unwrap().is_none());
        assert!(storage.get_detached_root(root_id).unwrap().is_none());
        assert!(storage.get_inode(second_file_id).unwrap().is_none());

        let third = reclaim(&state_machine, vec![child_dir_id], 2).unwrap();
        assert_eq!(third.processed_entries, 0);
        assert_eq!(third.completed_roots, 1);
        assert!(storage.get_inode(child_dir_id).unwrap().is_none());

        let replay = reclaim(&state_machine, vec![root_id, child_dir_id, root_id], 2).unwrap();
        assert_eq!(replay.processed_entries, 0);
        assert_eq!(replay.completed_roots, 0);
    }

    #[test]
    fn reopen_resumes_from_deleted_dentries_without_a_cursor() {
        let dir = TempDir::new().unwrap();
        let root_id = InodeId::new(50);
        {
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
            expect_mount_upserted(state_machine.apply(bootstrap_command("root", 1)).unwrap());
            let marker = detached_root(MountId::new(1), 111);
            seed_directory(&storage, root_id, marker.mount_id);
            seed_file(&storage, root_id, "a", InodeId::new(51), marker.mount_id);
            seed_file(&storage, root_id, "b", InodeId::new(52), marker.mount_id);
            storage.put_detached_root(root_id, marker).unwrap();
            let first = reclaim(&state_machine, vec![root_id], 1).unwrap();
            assert_eq!(first.processed_entries, 1);
        }

        let storage = Arc::new(RocksDBStorage::open_existing_for_start(dir.path()).unwrap());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let second = reclaim(&state_machine, vec![root_id], 1).unwrap();
        assert_eq!(second.processed_entries, 1);
        assert_eq!(second.completed_roots, 1);
        assert!(storage.get_detached_root(root_id).unwrap().is_none());
        assert!(storage.get_inode(root_id).unwrap().is_none());
    }

    #[test]
    fn protocol_limits_are_rejected_before_authority_changes() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(60);
        let marker = detached_root(MountId::new(1), 123);
        seed_directory(&storage, root_id, marker.mount_id);
        storage.put_detached_root(root_id, marker).unwrap();

        let error = state_machine
            .apply(Command::ReclaimDetachedRoots {
                candidate_root_inode_ids: vec![root_id],
                max_entries: MAX_RECLAIM_DETACHED_ROOT_ENTRIES + 1,
                max_batch_bytes: MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
            })
            .unwrap_err();

        assert!(error.to_string().contains("invalid detached-root entry budget"));
        assert_eq!(storage.get_detached_root(root_id).unwrap(), Some(marker));
        assert!(matches!(
            storage.get_inode(root_id).unwrap().unwrap().kind,
            InodeKind::Dir
        ));
    }

    #[test]
    fn byte_budget_stops_before_the_entry_budget_without_partial_overrun() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(80);
        let marker = detached_root(MountId::new(1), 345);
        seed_directory(&storage, root_id, marker.mount_id);
        for index in 0..64u64 {
            let inode_id = InodeId::new(81 + index);
            let name = format!("{index:03}-{}", "x".repeat(96));
            storage
                .put_inode(&Inode::new_file(
                    inode_id,
                    InodeAttrs::new(),
                    marker.mount_id,
                    beryl_types::FileLayout::new(4096),
                ))
                .unwrap();
            storage.put_layout(inode_id, FileLayout::new(4096)).unwrap();
            storage.put_dentry(root_id, &name, inode_id).unwrap();
        }
        storage.put_detached_root(root_id, marker).unwrap();

        let result = reclaim_with_budget(
            &state_machine,
            vec![root_id],
            MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
            MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
        )
        .unwrap();

        assert!(result.processed_entries > 0);
        assert!(result.processed_entries < 64);
        assert!(result.logical_batch_bytes <= MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES);
        assert!(storage.get_detached_root(root_id).unwrap().is_some());
        let (remaining, _, eof) = storage.list_dentries_with_cursor(root_id, None, 64).unwrap();
        assert!(eof);
        assert_eq!(remaining.len(), 64 - result.processed_entries as usize);
    }

    #[test]
    fn hundred_thousand_empty_roots_are_selected_and_reclaimed_by_bounded_candidate_batch() {
        let (_dir, storage, state_machine) = new_state_machine();
        let marker = detached_root(MountId::new(1), 456);
        storage.put_empty_detached_roots(10_000, 100_000, marker).unwrap();
        let (candidates, has_more) = storage
            .list_detached_roots(MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize)
            .unwrap();
        assert_eq!(candidates.len(), MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize);
        assert!(has_more);

        let result = reclaim(
            &state_machine,
            candidates.into_iter().map(|(inode_id, _)| inode_id).collect(),
            MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
        )
        .unwrap();

        assert_eq!(result.processed_entries, 0);
        assert_eq!(result.completed_roots, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES);
        assert!(result.logical_batch_bytes <= MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES);
        let (next_candidates, still_has_more) = storage
            .list_detached_roots(MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize)
            .unwrap();
        assert_eq!(next_candidates.len(), MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize);
        assert!(still_has_more);
    }
}
