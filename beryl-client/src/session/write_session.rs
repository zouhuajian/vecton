// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-side sequential write session state.

use crate::error::{ClientError, ClientResult};
use crate::runtime::context::{Operation, OperationContext, OperationDeadline};
use beryl_types::{
    BlockId, BlockShape, CallId, ClientId, CommittedBlock, ContentGeneration, FileLayout, InodeId, LocatedBlock,
    WriteHandle, WriteMode,
};
use std::fmt::{Debug, Formatter, Result};
use std::time::{SystemTime, UNIX_EPOCH};

const LEASE_EXPIRY_SAFETY_WINDOW_MS: u64 = 1_000;

/// Sole mutable lifecycle state for one open sequential writer.
#[derive(Debug)]
pub(crate) struct WriteSession {
    path: String,
    inode_id: InodeId,
    layout: FileLayout,
    generation: ContentGeneration,
    mode: WriteMode,
    write_handle: WriteHandle,
    base_size: u64,
    cursor: u64,
    flush_cursor: u64,
    expires_at_ms: Option<u64>,
    ready_blocks: Vec<ReadyBlock>,
    write_group: Option<beryl_types::GroupName>,
    state: WriteSessionState,
    sync: Option<SyncWriteState>,
    commit: Option<CommitFileState>,
    abort: Option<AbortCleanupState>,
}

impl WriteSession {
    /// Create a session from Metadata state whose handle passed wire validation.
    /// Layout and expiry are checked before accepting further write operations.
    pub(crate) fn new(
        path: String,
        layout: FileLayout,
        write_handle: WriteHandle,
        base_size: u64,
        expires_at_ms: u64,
        generation: ContentGeneration,
        mode: WriteMode,
    ) -> ClientResult<Self> {
        let inode_id = write_handle.inode_id;
        if expires_at_ms == 0 {
            return Err(ClientError::invalid_argument(
                "write session expires_at_ms must be non-zero".to_string(),
            ));
        }
        layout
            .validate()
            .map_err(|err| ClientError::invalid_layout(format!("write session layout invalid: {err}")))?;
        Ok(Self {
            path,
            inode_id,
            layout,
            generation,
            mode,
            write_handle,
            base_size,
            cursor: if mode == WriteMode::Overwrite { 0 } else { base_size },
            flush_cursor: if mode == WriteMode::Overwrite { 0 } else { base_size },
            expires_at_ms: Some(expires_at_ms),
            ready_blocks: Vec::new(),
            write_group: None,
            state: WriteSessionState::Open,
            sync: None,
            commit: None,
            abort: None,
        })
    }

    /// Path associated with the original open operation.
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    /// Current sequential write cursor.
    pub(crate) fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Advances the SDK-visible cursor after the current Worker request stream
    /// accepts ownership of bytes.
    pub(crate) fn advance_cursor(&mut self, len: usize) -> ClientResult<()> {
        self.cursor = self
            .cursor
            .checked_add(len as u64)
            .ok_or_else(|| ClientError::invalid_argument("write cursor overflow".to_string()))?;
        Ok(())
    }

    /// Metadata write handle.
    pub(crate) fn write_handle(&self) -> WriteHandle {
        self.write_handle
    }

    /// Predecessor that identifies the next logical AllocateBlock step.
    /// It advances only after Worker completion, so a definite rejection before
    /// Worker IO leaves subsequent allocation calls addressing the same block.
    pub(crate) fn previous_block_id(&self) -> Option<BlockId> {
        self.ready_blocks.last().map(|block| block.target.block_id)
    }

    /// Validate a metadata write target before opening the worker stream.
    pub(crate) fn validate_target(&mut self, target: &LocatedBlock) -> ClientResult<()> {
        self.ensure_open_for_write()?;
        if target.file_offset.checked_add(target.write_offset) != Some(self.flush_cursor) {
            return Err(ClientError::invalid_layout(format!(
                "write target file_offset mismatch: expected {}, got {}",
                self.flush_cursor, target.file_offset
            )));
        }
        BlockShape::new(
            target.block_format_id,
            target.block_size,
            target.chunk_size,
            target.block_size,
        )
        .map_err(|err| ClientError::invalid_layout(format!("write target has invalid shape: {err}")))?;
        let storage_chunk_size = self
            .layout
            .block_format_id
            .spec()
            .map_err(|err| ClientError::invalid_layout(format!("session block format is invalid: {err}")))?
            .storage_chunk_size;
        if target.block_format_id != self.layout.block_format_id
            || target.block_size != u64::from(self.layout.block_size)
            || target.chunk_size != storage_chunk_size
        {
            return Err(ClientError::invalid_layout(format!(
                "write target layout does not match session layout: target=({}, {}, {}), session=({}, {}, {})",
                target.block_format_id.as_raw(),
                target.block_size,
                target.chunk_size,
                self.layout.block_format_id.as_raw(),
                self.layout.block_size,
                storage_chunk_size
            )));
        }
        let block = target.block_id;
        if block.inode_id != self.inode_id {
            return Err(ClientError::stale_handle(format!(
                "write target inode_id {} does not match session inode_id {}",
                block.inode_id.as_raw(),
                self.inode_id.as_raw()
            )));
        }
        if target.write_offset >= target.block_size || target.fencing_token.epoch != self.write_handle.lease_epoch {
            return Err(ClientError::invalid_layout(
                "write target offset or writer epoch is invalid".to_string(),
            ));
        }
        Ok(())
    }

    /// Binds the returned tail to the opened file and its validated Metadata group.
    pub(crate) fn accept_open_tail(
        &mut self,
        group: beryl_types::GroupName,
        tail: Option<LocatedBlock>,
    ) -> ClientResult<()> {
        let needs_tail =
            self.mode == WriteMode::Append && !self.base_size.is_multiple_of(u64::from(self.layout.block_size));
        if needs_tail != tail.is_some() {
            return Err(ClientError::invalid_layout("OpenWrite tail does not match file length"));
        }
        self.record_write_group(group)?;
        if let Some(target) = tail {
            self.validate_target(&target)?;
            self.ready_blocks.push(ReadyBlock {
                written_len: target.write_offset,
                target,
            });
        }
        Ok(())
    }

    pub(crate) fn record_write_group(&mut self, group: beryl_types::GroupName) -> ClientResult<()> {
        if self.write_group.as_ref().is_some_and(|current| current != &group) {
            return Err(ClientError::invalid_layout("write target changed Metadata group"));
        }
        self.write_group = Some(group);
        Ok(())
    }

    /// Reuses the most recent partial checkpoint; it is not another allocation step.
    pub(crate) fn reusable_tail(&self) -> Option<(beryl_types::GroupName, LocatedBlock)> {
        let last = self.ready_blocks.last()?;
        if last.written_len >= last.target.block_size {
            return None;
        }
        let mut target = last.target.clone();
        target.write_offset = last.written_len;
        Some((self.write_group.clone()?, target))
    }

    /// Records a total durable block length and advances only by this stream's appended bytes.
    pub(crate) fn push_ready_block(&mut self, target: LocatedBlock, written_len: u64) -> ClientResult<()> {
        if written_len < target.write_offset
            || written_len > target.block_size
            || target.file_offset.checked_add(target.write_offset) != Some(self.flush_cursor)
        {
            return Err(ClientError::invalid_layout(
                "Worker checkpoint does not match the flush cursor",
            ));
        }
        let final_offset = target
            .file_offset
            .checked_add(written_len)
            .ok_or_else(|| ClientError::invalid_argument("write flush cursor overflow"))?;
        if final_offset != self.cursor {
            return Err(ClientError::invalid_layout(
                "Worker checkpoint does not match accepted bytes",
            ));
        }
        if self
            .ready_blocks
            .last()
            .is_some_and(|last| last.target.block_id == target.block_id)
        {
            self.ready_blocks.pop();
        }
        self.ready_blocks.push(ReadyBlock { target, written_len });
        self.flush_cursor = final_offset;
        Ok(())
    }

    /// Return only the tail growth and new blocks beyond the last publication.
    pub(crate) fn publication_blocks(&self) -> Vec<CommittedBlock> {
        self.ready_blocks
            .iter()
            .filter(|block| {
                self.mode == WriteMode::Overwrite || block.target.file_offset + block.written_len > self.base_size
            })
            .map(|block| CommittedBlock {
                block_id: block.target.block_id,
                len: block.written_len,
            })
            .collect()
    }

    /// Freezes one SyncWrite identity and payload before Metadata can observe it.
    ///
    /// A later call may only replay this exact plan until a validated success
    /// returns the session to `Open`.
    pub(crate) fn prepare_sync_write(
        &mut self,
        client_id: ClientId,
        client_name: &str,
        committed_blocks: Vec<CommittedBlock>,
        target_size: u64,
        deadline: OperationDeadline,
    ) -> ClientResult<SyncWritePlan> {
        match self.state {
            WriteSessionState::Open => {
                self.sync = Some(SyncWriteState {
                    call_id: CallId::new(),
                    write_handle: self.write_handle,
                    committed_blocks,
                    target_size,
                    expected_generation: self.generation,
                    expected_file_size: self.base_size,
                    write_mode: self.mode,
                });
                self.state = WriteSessionState::SyncPending;
            }
            WriteSessionState::SyncPending => {
                let sync = self.sync.as_ref().ok_or_else(|| {
                    ClientError::invalid_argument("SyncWrite state missing frozen identity".to_string())
                })?;
                if sync.target_size != target_size || sync.committed_blocks != committed_blocks {
                    return Err(ClientError::invalid_argument(
                        "SyncWrite payload changed after sync started".to_string(),
                    ));
                }
                if sync.write_handle != self.write_handle
                    || sync.expected_generation != self.generation
                    || sync.expected_file_size != self.base_size
                    || sync.write_mode != self.mode
                {
                    return Err(ClientError::invalid_argument(
                        "SyncWrite session state changed after sync started".to_string(),
                    ));
                }
            }
            _ => return Err(self.state_error_value()),
        }

        let sync = self
            .sync
            .as_ref()
            .ok_or_else(|| ClientError::invalid_argument("SyncWrite state missing frozen identity".to_string()))?;
        let operation = OperationContext::with_call_id_named(
            client_id,
            client_name,
            sync.call_id,
            Operation::SyncWrite,
            Some(self.path.clone()),
            deadline,
        )?;
        Ok(SyncWritePlan {
            operation,
            write_handle: sync.write_handle,
            committed_blocks: sync.committed_blocks.clone(),
            target_size: sync.target_size,
            expected_generation: sync.expected_generation,
            expected_file_size: sync.expected_file_size,
            write_mode: sync.write_mode,
        })
    }

    /// Freeze and return the CommitFile operation for this write session.
    pub(crate) fn prepare_commit_file(
        &mut self,
        client_id: ClientId,
        client_name: &str,
        committed_blocks: Vec<CommittedBlock>,
        final_size: u64,
        deadline: OperationDeadline,
    ) -> ClientResult<CommitFilePlan> {
        match self.state {
            WriteSessionState::Open => {
                self.commit = Some(CommitFileState {
                    commit_call_id: CallId::new(),
                    commit_write_handle: self.write_handle,
                    commit_final_size: final_size,
                    commit_committed_blocks_snapshot: committed_blocks,
                    expected_generation: self.generation,
                    expected_file_size: self.base_size,
                    write_mode: self.mode,
                });
                self.state = WriteSessionState::CommitStarted;
            }
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => {
                let commit = self.commit.as_ref().ok_or_else(|| {
                    ClientError::invalid_argument("CommitFile state missing frozen identity".to_string())
                })?;
                if commit.commit_final_size != final_size || commit.commit_committed_blocks_snapshot != committed_blocks
                {
                    return Err(ClientError::invalid_argument(
                        "CommitFile payload changed after commit started".to_string(),
                    ));
                }
                if commit.commit_write_handle != self.write_handle {
                    return Err(ClientError::invalid_argument(
                        "CommitFile write handle changed after commit started".to_string(),
                    ));
                }
            }
            WriteSessionState::Closed => {
                return Err(ClientError::stale_handle("write handle is closed"));
            }
            WriteSessionState::Aborted => {
                return Err(ClientError::stale_handle("write handle is aborted"));
            }
            WriteSessionState::UnknownOutcome => {
                return Err(ClientError::stale_handle("write handle has an unknown outcome"));
            }
            WriteSessionState::SessionInvalid => {
                return Err(ClientError::stale_handle("write session is invalid"));
            }
            WriteSessionState::SessionExpired => {
                return Err(ClientError::stale_handle("write session lease expired"));
            }
            WriteSessionState::AbortUnknown => {
                return Err(ClientError::stale_handle("write handle abort outcome is unknown"));
            }
            WriteSessionState::SyncPending => {
                return Err(ClientError::stale_handle("write handle has an unresolved SyncWrite"));
            }
        }

        let commit = self
            .commit
            .as_ref()
            .ok_or_else(|| ClientError::invalid_argument("CommitFile state missing frozen identity".to_string()))?;
        let operation = OperationContext::with_call_id_named(
            client_id,
            client_name,
            commit.commit_call_id,
            Operation::CommitFile,
            Some(self.path.clone()),
            deadline,
        )?;
        Ok(CommitFilePlan {
            operation,
            write_handle: commit.commit_write_handle,
            committed_blocks: commit.commit_committed_blocks_snapshot.clone(),
            final_size: commit.commit_final_size,
            expected_generation: commit.expected_generation,
            expected_file_size: commit.expected_file_size,
            write_mode: commit.write_mode,
        })
    }

    /// Freeze and return the abort cleanup plan for this write session.
    pub(crate) fn prepare_abort_cleanup(
        &mut self,
        client_id: ClientId,
        client_name: &str,
        deadline: OperationDeadline,
    ) -> ClientResult<AbortCleanupPlan> {
        match self.state {
            WriteSessionState::Open => {
                self.abort = Some(AbortCleanupState {
                    metadata_call_id: CallId::new(),
                    metadata_write_handle: self.write_handle,
                });
                self.state = WriteSessionState::AbortUnknown;
            }
            WriteSessionState::AbortUnknown => {
                let abort = self.abort.as_ref().ok_or_else(|| {
                    ClientError::invalid_argument("AbortUnknown state missing frozen cleanup plan".to_string())
                })?;
                if abort.metadata_write_handle != self.write_handle {
                    return Err(ClientError::invalid_argument(
                        "Abort cleanup handle changed after cleanup started".to_string(),
                    ));
                }
            }
            _ => return Err(self.state_error_value()),
        }

        let abort = self
            .abort
            .as_ref()
            .ok_or_else(|| ClientError::invalid_argument("abort cleanup state missing frozen plan".to_string()))?;
        let metadata_operation = OperationContext::with_call_id_named(
            client_id,
            client_name,
            abort.metadata_call_id,
            Operation::AbortFileWrite,
            Some(self.path.clone()),
            deadline.clone(),
        )?;
        Ok(AbortCleanupPlan {
            metadata_operation,
            metadata_write_handle: abort.metadata_write_handle,
        })
    }

    /// Mark CommitFile outcome as unknown and keep the session retryable.
    pub(crate) fn mark_commit_unknown(&mut self) {
        if matches!(self.state, WriteSessionState::CommitStarted) {
            self.state = WriteSessionState::CommitUnknown;
        }
    }

    /// Mark the session closed after metadata commit succeeds.
    pub(crate) fn mark_closed(&mut self) {
        self.state = WriteSessionState::Closed;
    }

    /// Completes the frozen SyncWrite and restores normal writer operations.
    pub(crate) fn mark_sync_completed(&mut self, generation: ContentGeneration, file_size: u64) -> ClientResult<()> {
        if !matches!(self.state, WriteSessionState::SyncPending) {
            return Err(self.state_error_value());
        }
        self.generation = generation;
        self.base_size = file_size;
        self.mode = WriteMode::Append;
        self.sync = None;
        self.state = WriteSessionState::Open;
        Ok(())
    }

    /// Marks the session aborted after Metadata accepts `AbortFileWrite`.
    /// Metadata cleanup owns any durable Worker Ready blocks left unpublished.
    pub(crate) fn mark_aborted(&mut self) {
        self.abort = None;
        self.state = WriteSessionState::Aborted;
    }

    /// Mark the session as blocked by an unknown write outcome.
    pub(crate) fn mark_unknown_outcome(&mut self) {
        self.state = WriteSessionState::UnknownOutcome;
    }

    /// Mark the session invalid after a fencing or lease failure.
    pub(crate) fn mark_session_invalid(&mut self) {
        self.state = WriteSessionState::SessionInvalid;
    }

    /// Mark the session expired after local or metadata lease expiration.
    pub(crate) fn mark_session_expired(&mut self) {
        self.state = WriteSessionState::SessionExpired;
    }

    /// Mark abort cleanup as uncertain while keeping retry metadata.
    pub(crate) fn mark_abort_unknown(&mut self) {
        self.state = WriteSessionState::AbortUnknown;
    }

    /// Record the latest metadata lease expiration returned by RenewLease.
    pub(crate) fn update_expires_at_ms(&mut self, expires_at_ms: u64) {
        self.expires_at_ms = Some(expires_at_ms);
    }

    /// Current Metadata lease expiry used to bound an open Worker block RPC.
    pub(crate) fn expires_at_ms(&self) -> ClientResult<u64> {
        self.expires_at_ms
            .ok_or_else(|| ClientError::invalid_argument("write session expiry is missing".to_string()))
    }

    /// Return whether the open session should renew before another side-effecting operation.
    pub(crate) fn should_renew_lease(&mut self, renew_before_expiry_ms: u64) -> ClientResult<bool> {
        self.should_renew_lease_at_ms(unix_now_ms(), renew_before_expiry_ms)
    }

    /// Return whether CommitFile outcome is unresolved and retryable.
    pub(crate) fn is_commit_unknown(&self) -> bool {
        matches!(
            self.state,
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown
        )
    }

    /// Reject writes unless the session is open and the lease is locally valid.
    pub(crate) fn ensure_open_for_write(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Write)
    }

    /// Reject close unless the session can start or continue a safe close.
    pub(crate) fn ensure_open_for_close(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Close)
    }

    /// Reject abort unless cleanup is safe to attempt.
    pub(crate) fn ensure_open_for_abort(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Abort)
    }

    /// Reject lease renew unless the handle still represents an open session.
    pub(crate) fn ensure_open_for_renew(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Renew)
    }

    /// Reject sync unless it can start or safely replay a frozen plan.
    pub(crate) fn ensure_open_for_sync(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Sync)
    }

    fn ensure_operation_allowed(&mut self, operation: WriteSessionOperation) -> ClientResult<()> {
        self.ensure_operation_allowed_at_ms(operation, unix_now_ms())
    }

    fn ensure_operation_allowed_at_ms(&mut self, operation: WriteSessionOperation, now_ms: u64) -> ClientResult<()> {
        let safety_window_ms = match (self.state, operation) {
            (WriteSessionState::Open, WriteSessionOperation::Renew) => 0,
            (
                WriteSessionState::Open,
                WriteSessionOperation::Write
                | WriteSessionOperation::Close
                | WriteSessionOperation::Abort
                | WriteSessionOperation::Sync,
            ) => LEASE_EXPIRY_SAFETY_WINDOW_MS,
            (WriteSessionState::SyncPending, WriteSessionOperation::Sync)
            | (WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown, WriteSessionOperation::Close)
            | (WriteSessionState::AbortUnknown, WriteSessionOperation::Abort) => return Ok(()),
            _ => return Err(self.state_error_value()),
        };
        self.ensure_lease_valid_at_ms(now_ms, safety_window_ms)
    }

    fn should_renew_lease_at_ms(&mut self, now_ms: u64, renew_before_expiry_ms: u64) -> ClientResult<bool> {
        if !matches!(self.state, WriteSessionState::Open) {
            return Ok(false);
        }
        let Some(expires_at_ms) = self.expires_at_ms else {
            return Ok(false);
        };
        if expires_at_ms <= now_ms {
            self.mark_session_expired();
            return Err(ClientError::stale_handle("write session lease expired"));
        }
        Ok(expires_at_ms.saturating_sub(now_ms) <= renew_before_expiry_ms)
    }

    fn ensure_lease_valid_at_ms(&mut self, now_ms: u64, safety_window_ms: u64) -> ClientResult<()> {
        let Some(expires_at_ms) = self.expires_at_ms else {
            return Ok(());
        };
        if expires_at_ms <= now_ms {
            self.mark_session_expired();
            return Err(ClientError::stale_handle("write session lease expired"));
        }
        if expires_at_ms.saturating_sub(now_ms) <= safety_window_ms {
            self.mark_session_expired();
            return Err(ClientError::stale_handle("write session lease is near expiry"));
        }
        Ok(())
    }

    fn state_error_value(&self) -> ClientError {
        match self.state {
            WriteSessionState::Open => ClientError::invalid_argument("write session is open".to_string()),
            WriteSessionState::SyncPending => ClientError::stale_handle("write handle has an unresolved SyncWrite"),
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => {
                ClientError::stale_handle("write handle has an in-progress CommitFile")
            }
            WriteSessionState::Closed => ClientError::stale_handle("write handle is closed"),
            WriteSessionState::Aborted => ClientError::stale_handle("write handle is aborted"),
            WriteSessionState::UnknownOutcome => ClientError::stale_handle("write handle has an unknown outcome"),
            WriteSessionState::SessionInvalid => ClientError::stale_handle("write session is invalid"),
            WriteSessionState::SessionExpired => ClientError::stale_handle("write session lease expired"),
            WriteSessionState::AbortUnknown => ClientError::stale_handle("write handle abort outcome is unknown"),
        }
    }
}

/// Durable Worker Ready block pending Metadata publication.
#[derive(Clone, Debug)]
pub(crate) struct ReadyBlock {
    target: LocatedBlock,
    written_len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteSessionState {
    Open,
    SyncPending,
    CommitStarted,
    CommitUnknown,
    Closed,
    Aborted,
    UnknownOutcome,
    SessionInvalid,
    SessionExpired,
    AbortUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteSessionOperation {
    Write,
    Close,
    Abort,
    Renew,
    Sync,
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
struct SyncWriteState {
    call_id: CallId,
    write_handle: WriteHandle,
    committed_blocks: Vec<CommittedBlock>,
    target_size: u64,
    expected_generation: ContentGeneration,
    expected_file_size: u64,
    write_mode: WriteMode,
}

#[derive(Clone, Debug)]
struct CommitFileState {
    commit_call_id: CallId,
    commit_write_handle: WriteHandle,
    commit_final_size: u64,
    commit_committed_blocks_snapshot: Vec<CommittedBlock>,
    expected_generation: ContentGeneration,
    expected_file_size: u64,
    write_mode: WriteMode,
}

#[derive(Clone)]
struct AbortCleanupState {
    metadata_call_id: CallId,
    metadata_write_handle: WriteHandle,
}

impl Debug for AbortCleanupState {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("AbortCleanupState").finish_non_exhaustive()
    }
}

/// Frozen metadata SyncWrite operation and request payload.
#[derive(Clone, Debug)]
pub(crate) struct SyncWritePlan {
    pub(crate) operation: OperationContext,
    pub(crate) write_handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) target_size: u64,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) write_mode: WriteMode,
}

/// Frozen metadata CommitFile operation and request payload.
#[derive(Clone, Debug)]
pub(crate) struct CommitFilePlan {
    pub(crate) operation: OperationContext,
    pub(crate) write_handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) final_size: u64,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) write_mode: WriteMode,
}

/// Frozen side-effecting abort cleanup plan.
#[derive(Clone)]
pub(crate) struct AbortCleanupPlan {
    metadata_operation: OperationContext,
    metadata_write_handle: WriteHandle,
}

impl AbortCleanupPlan {
    /// Metadata AbortFileWrite operation with stable call identity.
    pub(crate) fn metadata_operation(&self) -> OperationContext {
        self.metadata_operation.clone()
    }

    /// Metadata write handle payload frozen before cleanup starts.
    pub(crate) fn metadata_write_handle(&self) -> WriteHandle {
        self.metadata_write_handle
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientErrorKind;
    use beryl_types::{BlockId, BlockIndex, ClientId, CommittedBlock, InodeId, LeaseEpoch};

    fn assert_error(error: &ClientError, kind: ClientErrorKind, message: &str) {
        assert_eq!(error.kind(), kind);
        assert!(error.message().contains(message), "unexpected error: {error:?}");
    }

    #[test]
    fn frozen_publication_plans_reject_drift_and_reuse_call_ids() {
        let mut session = new_session(1_000);

        let prepare = |session: &mut WriteSession, len| {
            session.prepare_commit_file(
                ClientId::new(7),
                "test-client",
                vec![committed_block(302, 0, len)],
                len,
                OperationDeadline::new(1_000),
            )
        };
        let first = prepare(&mut session, 5).expect("first commit plan");
        let err = prepare(&mut session, 6).expect_err("changed commit payload must fail");
        assert_error(&err, ClientErrorKind::InvalidArgument, "payload changed");
        session.mark_commit_unknown();
        session.write_handle.lease_epoch = LeaseEpoch::new(2);
        let err = prepare(&mut session, 5).expect_err("changed session identity must fail");
        assert_error(&err, ClientErrorKind::InvalidArgument, "write handle changed");
        session.write_handle.lease_epoch = LeaseEpoch::new(1);
        let retry = prepare(&mut session, 5).expect("retry commit plan");

        assert_eq!(first.operation.call_id(), retry.operation.call_id());
        assert_eq!(first.write_handle, retry.write_handle);
        assert_eq!(first.expected_generation, retry.expected_generation);
        assert_eq!(retry.final_size, 5);
        assert_eq!(retry.committed_blocks, vec![committed_block(302, 0, 5)]);

        let mut sync_session = new_session(2_000);
        let prepare_sync = |session: &mut WriteSession| {
            session.prepare_sync_write(
                ClientId::new(7),
                "test-client",
                vec![committed_block(302, 0, 5)],
                5,
                OperationDeadline::new(1_000),
            )
        };
        let first_sync = prepare_sync(&mut sync_session).expect("first sync plan");
        sync_session.generation = ContentGeneration::new(1);
        let error = prepare_sync(&mut sync_session).expect_err("generation drift must fail");
        assert_error(&error, ClientErrorKind::InvalidArgument, "session state changed");
        sync_session.generation = ContentGeneration::new(0);
        let retry_sync = prepare_sync(&mut sync_session).expect("retry after cancelled future");
        assert_eq!(first_sync.operation.call_id(), retry_sync.operation.call_id());
        assert_eq!(retry_sync.target_size, first_sync.target_size);
        assert_eq!(retry_sync.committed_blocks, first_sync.committed_blocks);
    }

    #[test]
    fn prepare_abort_cleanup_rejects_session_identity_drift_after_unknown_without_replacing_call_id() {
        let mut session = new_session(1_000);

        let prepare = |session: &mut WriteSession| {
            session.prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
        };
        let first = prepare(&mut session).expect("first abort plan");
        session.write_handle.lease_epoch = LeaseEpoch::new(2);
        let err = prepare(&mut session)
            .err()
            .expect("identity drift must reject abort replay");
        assert_error(&err, ClientErrorKind::InvalidArgument, "handle changed");
        session.write_handle.lease_epoch = LeaseEpoch::new(1);
        let retry = prepare(&mut session).expect("retry abort plan");
        assert_eq!(
            first.metadata_operation().call_id(),
            retry.metadata_operation().call_id()
        );
    }

    #[test]
    fn operation_gate_preserves_lease_and_retry_semantics() {
        let mut renew = new_session(1_000);
        renew
            .ensure_operation_allowed_at_ms(WriteSessionOperation::Renew, 1)
            .expect("renew may run inside the side-effect safety window");

        let mut expired = new_session(1_000);
        for operation in [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Renew,
            WriteSessionOperation::Sync,
        ] {
            let error = expired
                .ensure_operation_allowed_at_ms(operation, 1_001)
                .expect_err("expired lease must block every operation");
            assert_error(&error, ClientErrorKind::StaleHandle, "expired");
        }

        for operation in [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Sync,
        ] {
            let mut session = new_session(1_000);
            let error = session
                .ensure_operation_allowed_at_ms(operation, 1)
                .expect_err("new side effects must stop near lease expiry");
            assert_error(&error, ClientErrorKind::StaleHandle, "near expiry");
        }

        for (state, operation) in [
            (WriteSessionState::SyncPending, WriteSessionOperation::Sync),
            (WriteSessionState::CommitStarted, WriteSessionOperation::Close),
            (WriteSessionState::CommitUnknown, WriteSessionOperation::Close),
            (WriteSessionState::AbortUnknown, WriteSessionOperation::Abort),
        ] {
            let mut session = new_session(1);
            session.state = state;
            session
                .ensure_operation_allowed_at_ms(operation, 2)
                .expect("frozen lifecycle retry must not be blocked by lease expiry");
            assert_eq!(session.state, state);
        }
    }

    fn new_session(expires_at_ms: u64) -> WriteSession {
        WriteSession::new(
            "/alpha".to_string(),
            test_layout(),
            write_handle(302),
            0,
            expires_at_ms,
            ContentGeneration::new(0),
            WriteMode::Overwrite,
        )
        .expect("session")
    }

    fn test_layout() -> FileLayout {
        FileLayout::new(1024)
    }

    fn write_handle(inode_id: u64) -> WriteHandle {
        WriteHandle {
            inode_id: InodeId::new(inode_id),
            lease_epoch: LeaseEpoch::new(1),
        }
    }

    fn committed_block(inode_id: u64, block_index: u32, len: u64) -> CommittedBlock {
        CommittedBlock {
            block_id: BlockId::new(InodeId::new(inode_id), BlockIndex::new(block_index)),
            len,
        }
    }
}
