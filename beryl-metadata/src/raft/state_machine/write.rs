// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{
    AppMetadataRaftState, AppRaftStateMachine, BlockId, BlockIndex, Inode, InodeId, MetadataError, MetadataResult,
};
use crate::inode::{FileCommit, FilePublication};
use beryl_types::{CallId, ClientId, ContentGeneration, LeaseEpoch};

impl AppRaftStateMachine {
    /// Check the storage key and complete fixed-block shape before file mutation.
    fn ensure_file_inode_authority(inode_id: InodeId, inode: &Inode) -> MetadataResult<()> {
        if inode.inode_id != inode_id || !inode.file_type().is_file() {
            return Err(MetadataError::Internal(format!(
                "invalid file authority for inode {inode_id}"
            )));
        }
        inode.file()?.validate(inode_id)
    }

    /// Reserve a never-reused index under the exact durable writer epoch.
    pub(super) fn apply_allocate_block(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<BlockId> {
        let mut inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let file = inode.file_mut()?;
        if file.lease_epoch != lease_epoch {
            return Err(MetadataError::LeaseFenced {
                expected: file.lease_epoch,
                got: lease_epoch,
            });
        }
        let index = u32::try_from(file.next_index)
            .map_err(|_| MetadataError::InvalidArgument("block index exhausted".into()))?;
        file.next_index += 1;
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(BlockId::new(inode_id, BlockIndex::new(index)))
    }

    /// Acquire new durable writer authority from the exact previous epoch.
    pub(super) fn apply_acquire_write_lease(
        &self,
        inode_id: InodeId,
        expected_lease_epoch: LeaseEpoch,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<LeaseEpoch> {
        let mut inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let file = inode.file_mut()?;
        if let Some(record) = self.storage.get_create_file_replay_for_inode(inode_id)? {
            if record.expires_at_ms > proposed_at_ms
                && file.len == 0
                && file.next_index == 0
                && file.generation == record.generation
                && file.lease_epoch == record.lease_epoch
            {
                return Err(MetadataError::Again(
                    "CreateFile replay still owns the initial session".into(),
                ));
            }
        }
        if file.lease_epoch != expected_lease_epoch {
            return Err(MetadataError::Again("write lease epoch changed".into()));
        }
        let next = file
            .lease_epoch
            .checked_next()
            .ok_or_else(|| MetadataError::InvalidArgument("write lease epoch overflow".into()))?;
        file.lease_epoch = next;
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(next)
    }

    /// End an exact lease; immediate-successor replay cannot change content.
    pub(super) fn apply_end_write_lease(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<LeaseEpoch> {
        let mut inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let file = inode.file_mut()?;
        let next = lease_epoch
            .checked_next()
            .ok_or_else(|| MetadataError::InvalidArgument("write lease epoch overflow".into()))?;
        if file.lease_epoch == next {
            self.storage.commit_applied_state(raft_state)?;
            return Ok(next);
        }
        if file.lease_epoch != lease_epoch {
            return Err(MetadataError::LeaseFenced {
                expected: file.lease_epoch,
                got: lease_epoch,
            });
        }
        file.lease_epoch = next;
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(next)
    }

    /// Publish a durable prefix while retaining the writer lease.
    pub(super) fn apply_publish_file(
        &self,
        inode_id: InodeId,
        publication: FilePublication,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<ContentGeneration> {
        let inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let (inode, generation, changed) = self.prepare_file_publication(inode, publication, proposed_at_ms)?;
        if changed {
            self.storage.put_inode_atomic(&inode, raft_state)?;
        } else {
            self.storage.commit_applied_state(raft_state)?;
        }
        Ok(generation)
    }

    /// Prepare a single inode update. Sync and Commit share prefix and replay
    /// rules while Commit additionally records an exact operation and closes.
    fn prepare_file_publication(
        &self,
        mut inode: Inode,
        publication: FilePublication,
        proposed_at_ms: u64,
    ) -> MetadataResult<(Inode, ContentGeneration, bool)> {
        let inode_id = inode.inode_id;
        let file = inode.file()?;
        if file.lease_epoch != publication.lease_epoch {
            return Err(MetadataError::LeaseFenced {
                expected: file.lease_epoch,
                got: publication.lease_epoch,
            });
        }
        let generation = file.generation;
        let matches = publication.matches_visible(file)?;
        if publication.expected_generation.checked_next() == Some(generation) && matches {
            return Ok((inode, generation, false));
        }
        if generation != publication.expected_generation || file.len != publication.expected_file_size {
            return Err(MetadataError::Again("file publication precondition changed".into()));
        }
        if matches {
            return Ok((inode, generation, false));
        }
        let blocks = publication.merged_blocks(inode_id, file)?;
        let generation = generation
            .checked_next()
            .ok_or_else(|| MetadataError::InvalidArgument("content generation overflow".into()))?;
        let file = inode.file_mut()?;
        file.blocks = blocks;
        file.len = publication.target_size;
        file.generation = generation;
        file.last_commit = None;
        inode.attrs.set_modify_time(proposed_at_ms);
        Ok((inode, generation, true))
    }

    /// Atomically publish, revoke the writer, and persist bounded completion
    /// evidence. Content equality alone never acknowledges a Commit replay.
    pub(super) fn apply_commit_file(
        &self,
        inode_id: InodeId,
        operation: (ClientId, CallId),
        publication: FilePublication,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<ContentGeneration> {
        let inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        if let Some(generation) = publication.resolve_commit(&inode, operation.0, operation.1)? {
            self.storage.commit_applied_state(raft_state)?;
            return Ok(generation);
        }
        if inode.file()?.generation != publication.expected_generation {
            return Err(MetadataError::Again(
                "CommitFile generation changed without completion evidence".into(),
            ));
        }
        let ended_epoch = publication
            .lease_epoch
            .checked_next()
            .ok_or_else(|| MetadataError::InvalidArgument("write lease epoch overflow".into()))?;
        let mut commit = FileCommit {
            client_id: operation.0,
            call_id: operation.1,
            lease_epoch: publication.lease_epoch,
            expected_generation: publication.expected_generation,
            expected_file_size: publication.expected_file_size,
            mode: publication.mode,
            committed_size: publication.target_size,
            generation: publication.expected_generation,
        };
        let (mut inode, generation, _) = self.prepare_file_publication(inode, publication, proposed_at_ms)?;
        commit.generation = generation;
        let file = inode.file_mut()?;
        file.lease_epoch = ended_epoch;
        file.last_commit = Some(commit);
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::{InodeAttrs, PublishMode};
    use crate::raft::state_machine::tests::*;
    use crate::raft::{ApplySuccess, Command, RocksDBStorage};
    use beryl_types::{CommittedBlock, FileLayout, MountId};
    use openraft::{LeaderId, LogId};
    use std::sync::Arc;

    fn fixture() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine, InodeId) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let id = InodeId::new(42);
        let mut inode = Inode::new_file(id, InodeAttrs::new(), MountId::new(1), FileLayout::new(4));
        inode.file_mut().unwrap().lease_epoch = LeaseEpoch::new(1);
        storage.put_inode(&inode).unwrap();
        (dir, storage.clone(), AppRaftStateMachine::new(storage), id)
    }

    fn allocate(sm: &AppRaftStateMachine, id: InodeId, epoch: u64) -> BlockId {
        match sm
            .apply(Command::AllocateBlock {
                inode_id: id,
                lease_epoch: LeaseEpoch::new(epoch),
            })
            .unwrap()
        {
            ApplySuccess::BlockAllocated(block) => block,
            other => panic!("unexpected {other:?}"),
        }
    }
    fn publication(blocks: &[(BlockId, u64)], base: u64, target: u64, generation: u64, epoch: u64) -> FilePublication {
        FilePublication {
            blocks: blocks
                .iter()
                .map(|&(block_id, len)| CommittedBlock { block_id, len })
                .collect(),
            target_size: target,
            expected_file_size: base,
            expected_generation: ContentGeneration::new(generation),
            lease_epoch: LeaseEpoch::new(epoch),
            mode: PublishMode::AppendIfUnchanged,
        }
    }
    fn sync(id: InodeId, publication: FilePublication) -> Command {
        Command::PublishFile {
            proposed_at_ms: 10,
            inode_id: id,
            publication,
        }
    }
    fn commit(id: InodeId, publication: FilePublication) -> Command {
        Command::CommitFile {
            proposed_at_ms: 20,
            inode_id: id,
            client_id: ClientId::new(9),
            call_id: CallId::new(),
            publication,
        }
    }

    #[test]
    fn publication_preserves_partial_tail_and_rejects_old_or_malformed_prefixes() {
        let (_dir, storage, sm, id) = fixture();
        let a = allocate(&sm, id, 1);
        let b = allocate(&sm, id, 1);
        let first = sync(id, publication(&[(a, 2)], 0, 2, 0, 1));
        sm.apply(first.clone()).unwrap();
        let prefix = storage.get_inode(id).unwrap().unwrap();
        sm.apply(first.clone()).unwrap();
        assert_eq!(storage.get_inode(id).unwrap().unwrap(), prefix);
        for blocks in [vec![(b, 3)], vec![(a, 2), (b, 1)], vec![(a, 4), (a, 1)]] {
            let before = storage.get_inode(id).unwrap();
            let target = blocks.iter().map(|b| b.1).sum();
            assert!(sm.apply(sync(id, publication(&blocks, 2, target, 1, 1))).is_err());
            assert_eq!(storage.get_inode(id).unwrap(), before);
        }
        sm.apply(sync(id, publication(&[(a, 4), (b, 1)], 2, 5, 1, 1))).unwrap();
        let visible = storage.get_inode(id).unwrap().unwrap();
        assert_eq!(visible.file().unwrap().blocks, vec![a, b]);
        assert_eq!(visible.file().unwrap().generation, ContentGeneration::new(2));
        assert!(sm.apply(first).is_err());
        assert_eq!(storage.get_inode(id).unwrap().unwrap(), visible);
        sm.apply(sync(id, publication(&[], 5, 5, 2, 1))).unwrap();
        assert_eq!(
            storage.get_inode(id).unwrap().unwrap(),
            visible,
            "no-op preserves generation and mtime"
        );
    }

    #[test]
    fn commit_evidence_is_exact_atomic_and_survives_reopen_until_content_changes() {
        for (len, already_synced) in [(0, false), (2, false), (2, true)] {
            let (_dir, storage, sm, id) = fixture();
            let block = allocate(&sm, id, 1);
            if already_synced {
                sm.apply(sync(id, publication(&[(block, len)], 0, len, 0, 1))).unwrap();
            }
            let body = if already_synced {
                publication(&[], len, len, 1, 1)
            } else {
                publication(&if len == 0 { vec![] } else { vec![(block, len)] }, 0, len, 0, 1)
            };
            let request = commit(id, body);
            sm.apply(request.clone()).unwrap();
            let committed = storage.get_inode(id).unwrap().unwrap();
            assert_eq!(committed.file().unwrap().lease_epoch, LeaseEpoch::new(2));
            assert_eq!(
                committed.file().unwrap().generation,
                ContentGeneration::new(u64::from(len > 0))
            );
            assert!(committed.file().unwrap().last_commit.is_some());
            let reopened_sm = AppRaftStateMachine::new(storage.clone());
            reopened_sm.apply(request.clone()).unwrap();
            assert_eq!(storage.get_inode(id).unwrap().unwrap(), committed);
            for mutation in 0..5 {
                let mut changed = request.clone();
                if let Command::CommitFile {
                    call_id, publication, ..
                } = &mut changed
                {
                    match mutation {
                        0 => *call_id = CallId::new(),
                        1 => publication.target_size += 1,
                        2 => publication.expected_generation = ContentGeneration::new(99),
                        3 => publication.expected_file_size += 1,
                        _ => publication.mode = PublishMode::ReplaceIfUnchanged,
                    }
                }
                assert!(reopened_sm.apply(changed).is_err());
            }
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 30,
                inode_id: id,
                expected_lease_epoch: LeaseEpoch::new(2),
            })
            .unwrap();
            sm.apply(request.clone()).unwrap();
            let new = allocate(&sm, id, 3);
            let mut replacement = publication(&[(new, 1)], len, 1, u64::from(len > 0), 3);
            replacement.mode = PublishMode::ReplaceIfUnchanged;
            sm.apply(sync(id, replacement)).unwrap();
            assert!(storage
                .get_inode(id)
                .unwrap()
                .unwrap()
                .file()
                .unwrap()
                .last_commit
                .is_none());
            assert!(sm.apply(request).is_err());
        }
    }

    #[test]
    fn sync_or_lease_end_cannot_confirm_commit_and_ended_writes_cannot_publish() {
        let (_dir, storage, sm, id) = fixture();
        let block = allocate(&sm, id, 1);
        let payload = publication(&[(block, 2)], 0, 2, 0, 1);
        sm.apply(sync(id, payload.clone())).unwrap();
        assert!(sm.apply(commit(id, payload.clone())).is_err());
        sm.apply(Command::EndWriteLease {
            proposed_at_ms: 12,
            inode_id: id,
            lease_epoch: LeaseEpoch::new(1),
        })
        .unwrap();
        let ended = storage.get_inode(id).unwrap().unwrap();
        assert!(sm.apply(sync(id, publication(&[(block, 3)], 2, 3, 1, 1))).is_err());
        assert!(sm.apply(commit(id, publication(&[], 2, 2, 1, 1))).is_err());
        assert!(ended.file().unwrap().last_commit.is_none());
        assert_eq!(storage.get_inode(id).unwrap().unwrap(), ended);
    }

    #[test]
    fn allocation_and_counter_exhaustion_never_reuse_identity_or_mutate_content() {
        let (_dir, storage, sm, id) = fixture();
        let first = allocate(&sm, id, 1);
        let restarted = AppRaftStateMachine::new(storage.clone());
        assert_eq!(allocate(&restarted, id, 1).index.as_raw(), first.index.as_raw() + 1);
        assert!(sm
            .apply(Command::AllocateBlock {
                inode_id: id,
                lease_epoch: LeaseEpoch::new(2)
            })
            .is_err());
        let mut inode = storage.get_inode(id).unwrap().unwrap();
        inode.file_mut().unwrap().next_index = u64::from(u32::MAX);
        storage.put_inode(&inode).unwrap();
        let last = allocate(&sm, id, 1);
        assert_eq!(last.index.as_raw(), u32::MAX);
        let exhausted = storage.get_inode(id).unwrap();
        assert!(sm
            .apply(Command::AllocateBlock {
                inode_id: id,
                lease_epoch: LeaseEpoch::new(1)
            })
            .is_err());
        assert_eq!(storage.get_inode(id).unwrap(), exhausted);
        for epoch_overflow in [false, true] {
            let mut inode = exhausted.clone().unwrap();
            let file = inode.file_mut().unwrap();
            if epoch_overflow {
                file.lease_epoch = LeaseEpoch::new(u64::MAX);
            } else {
                file.generation = ContentGeneration::new(u64::MAX);
            }
            let payload = publication(&[(first, 1)], 0, 1, file.generation.as_raw(), file.lease_epoch.as_raw());
            storage.put_inode(&inode).unwrap();
            assert!(sm.apply(commit(id, payload)).is_err());
            assert_eq!(storage.get_inode(id).unwrap().unwrap(), inode);
        }
    }

    #[test]
    fn corrupt_inode_authority_fails_without_advancing_applied_state() {
        let (_dir, storage, sm, id) = fixture();
        let mut inode = storage.get_inode(id).unwrap().unwrap();
        inode.file_mut().unwrap().len = 1; // Missing block for the claimed visible byte.
        storage.put_inode(&inode).unwrap();
        let before = storage.load_raft_state().unwrap();
        let next = AppMetadataRaftState {
            last_applied_log_id: Some(LogId::new(LeaderId::new(7, 1), 703)),
            ..Default::default()
        };
        for command in [
            Command::AllocateBlock {
                inode_id: id,
                lease_epoch: LeaseEpoch::new(1),
            },
            sync(id, publication(&[], 0, 0, 0, 1)),
            commit(id, publication(&[], 0, 0, 0, 1)),
        ] {
            assert!(sm.apply_with_raft_state(command, &next).is_err());
            assert_eq!(storage.get_inode(id).unwrap().unwrap(), inode);
            assert_eq!(storage.load_raft_state().unwrap(), before);
        }
    }
}
