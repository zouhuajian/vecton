// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Block runtime metadata, validation, and local access lifecycle boundary.

use crate::data::core::{ReadBlockRequest, WorkerCoreResult};
use crate::error::WorkerError;
use crate::report::BlockReportChangeTracker;
use crate::store::block::{BlockState, LocalBlockStore};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::ids::BlockId;
use beryl_types::layout::BlockShape;
use beryl_types::GroupName;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BlockAccessKey {
    group_name: GroupName,
    block_id: BlockId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockAccessState {
    Available { pins: usize },
    Reclaiming { pins: usize, operation_active: bool },
}

/// Coordinates access pins and destructive block lifecycle transitions.
///
/// `changed` wakes lifecycle waiters. `block_report_changes` retains the exact
/// identities whose reportable state changed before any notification is lost.
#[derive(Debug, Default)]
struct BlockAccessRegistry {
    states: Mutex<HashMap<BlockAccessKey, BlockAccessState>>,
    changed: Notify,
    block_report_changes: BlockReportChangeTracker,
}

/// Exact block currently excluded from new access for reclamation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReclaimingBlock {
    pub block_id: BlockId,
}

/// RAII guard that keeps a Ready block available for one complete read RPC.
///
/// The guard is acquired before local metadata validation so cleanup cannot pass
/// between validation and response-stream ownership. A blocking read clones the
/// guard so cancellation cannot release reclamation before filesystem IO exits.
#[derive(Clone, Debug)]
pub(crate) struct BlockPin {
    _inner: Arc<BlockPinInner>,
}

impl BlockPin {
    /// Checked after write registration to close the pending-authorization/reclaim race.
    pub(crate) fn is_reclaiming(&self) -> bool {
        matches!(
            self._inner
                .registry
                .states
                .lock()
                .expect("block access state poisoned")
                .get(&self._inner.key),
            Some(BlockAccessState::Reclaiming { .. })
        )
    }
}

#[derive(Debug)]
struct BlockPinInner {
    registry: Arc<BlockAccessRegistry>,
    key: BlockAccessKey,
}

impl Drop for BlockPinInner {
    fn drop(&mut self) {
        self.registry.release_pin(&self.key);
    }
}

/// Exclusive permission to reclaim one local block after all prior readers exit.
///
/// A failed or cancelled operation leaves the block in `Reclaiming` so new
/// readers remain rejected and a later cleanup retry can safely resume.
#[derive(Debug)]
pub(crate) struct ReclaimPermit {
    registry: Arc<BlockAccessRegistry>,
    key: BlockAccessKey,
    completed: bool,
}

impl ReclaimPermit {
    /// Waits after new admission is closed and existing writers have been retired.
    pub(crate) async fn wait_for_pins(&self) {
        loop {
            let notified = self.registry.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let pins = match self
                .registry
                .states
                .lock()
                .expect("block access state poisoned")
                .get(&self.key)
            {
                Some(BlockAccessState::Reclaiming { pins, .. }) => *pins,
                _ => 0,
            };
            if pins == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Completes reclamation and removes the transient lifecycle entry.
    ///
    /// Removing the entry also wakes block reporting after `Reclaiming` can no
    /// longer override a missing filesystem block as `Deleting`.
    pub(crate) fn complete(mut self) {
        self.registry.complete_reclaim(&self.key);
        self.completed = true;
    }
}

impl Drop for ReclaimPermit {
    fn drop(&mut self) {
        if !self.completed {
            self.registry.release_reclaim_operation(&self.key);
        }
    }
}

impl BlockAccessRegistry {
    /// Atomically pins an available block or rejects a read after reclaim starts.
    fn pin_block(self: &Arc<Self>, key: BlockAccessKey) -> WorkerCoreResult<BlockPin> {
        let mut states = self.states.lock().expect("block access state poisoned");
        match states.get_mut(&key) {
            Some(BlockAccessState::Available { pins }) => {
                *pins = pins.checked_add(1).expect("block read pin count overflow");
            }
            Some(BlockAccessState::Reclaiming { .. }) => {
                return Err(WorkerError::RefreshMetadata {
                    kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    message: format!(
                        "local block reclamation has started: group_name={}, block_id={}",
                        key.group_name, key.block_id
                    ),
                });
            }
            None => {
                states.insert(key.clone(), BlockAccessState::Available { pins: 1 });
            }
        }
        drop(states);
        Ok(BlockPin {
            _inner: Arc::new(BlockPinInner {
                registry: Arc::clone(self),
                key,
            }),
        })
    }

    /// Starts or resumes reclamation and waits for all previously pinned readers.
    fn begin_reclaim(self: &Arc<Self>, key: BlockAccessKey) -> WorkerCoreResult<ReclaimPermit> {
        {
            let mut states = self.states.lock().expect("block access state poisoned");
            match states.get_mut(&key) {
                Some(BlockAccessState::Available { pins }) => {
                    let pins = *pins;
                    states.insert(
                        key.clone(),
                        BlockAccessState::Reclaiming {
                            pins,
                            operation_active: true,
                        },
                    );
                }
                Some(BlockAccessState::Reclaiming {
                    operation_active: true, ..
                }) => {
                    return Err(WorkerError::Unavailable(
                        "local block reclamation is already running".into(),
                    ));
                }
                Some(BlockAccessState::Reclaiming { operation_active, .. }) => {
                    *operation_active = true;
                }
                None => {
                    states.insert(
                        key.clone(),
                        BlockAccessState::Reclaiming {
                            pins: 0,
                            operation_active: true,
                        },
                    );
                }
            }
        }
        self.block_report_changes.record(&key.group_name, key.block_id);

        let permit = ReclaimPermit {
            registry: Arc::clone(self),
            key,
            completed: false,
        };
        Ok(permit)
    }

    fn release_pin(&self, key: &BlockAccessKey) {
        let mut states = self.states.lock().expect("block access state poisoned");
        let mut remove = false;
        if let Some(state) = states.get_mut(key) {
            match state {
                BlockAccessState::Available { pins } => {
                    *pins = pins.checked_sub(1).expect("available block read pin underflow");
                    remove = *pins == 0;
                }
                BlockAccessState::Reclaiming { pins, .. } => {
                    *pins = pins.checked_sub(1).expect("reclaiming block read pin underflow");
                }
            }
        }
        if remove {
            states.remove(key);
        }
        drop(states);
        self.changed.notify_waiters();
    }

    fn release_reclaim_operation(&self, key: &BlockAccessKey) {
        let mut states = self.states.lock().expect("block access state poisoned");
        if let Some(BlockAccessState::Reclaiming { operation_active, .. }) = states.get_mut(key) {
            *operation_active = false;
        }
        drop(states);
        self.changed.notify_waiters();
    }

    /// Clears a completed reclaim fence before advertising the lifecycle change.
    fn complete_reclaim(&self, key: &BlockAccessKey) {
        let mut states = self.states.lock().expect("block access state poisoned");
        match states.get(key) {
            Some(BlockAccessState::Reclaiming { pins: 0, .. }) => {
                states.remove(key);
            }
            Some(BlockAccessState::Reclaiming { pins, .. }) => {
                panic!("completed block reclamation with {pins} active access pins");
            }
            _ => {}
        }
        drop(states);
        self.changed.notify_waiters();
        self.block_report_changes.record(&key.group_name, key.block_id);
    }

    /// Snapshots exact identities currently fenced from new access for reporting.
    fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<ReclaimingBlock> {
        let states = self.states.lock().expect("block access state poisoned");
        let mut blocks = states
            .iter()
            .filter_map(|(key, state)| match state {
                BlockAccessState::Reclaiming { .. } if &key.group_name == group_name => {
                    Some(ReclaimingBlock { block_id: key.block_id })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        blocks.sort_by_key(|block| (block.block_id.inode_id.as_raw(), block.block_id.index.as_raw()));
        blocks
    }

    /// Returns the exact reclaim fence for one block, if present.
    fn reclaiming_block(&self, group_name: &GroupName, block_id: BlockId) -> Option<ReclaimingBlock> {
        let states = self.states.lock().expect("block access state poisoned");
        match states.get(&BlockAccessKey {
            group_name: group_name.clone(),
            block_id,
        }) {
            Some(BlockAccessState::Reclaiming { .. }) => Some(ReclaimingBlock { block_id }),
            _ => None,
        }
    }

    /// Returns the retained reclaim-lifecycle change source.
    fn block_report_changes(&self) -> &BlockReportChangeTracker {
        &self.block_report_changes
    }

    /// Waits until a reclaim lifecycle transition changes the reportable view.
    async fn wait_for_block_report_change(&self) {
        self.block_report_changes.wait().await;
    }
}

/// Block-level facade for open and commit decisions.
///
/// The manager owns block metadata checks, range validation,
/// fencing decisions, and reader-versus-reclaimer lifecycle coordination. It
/// does not perform block data reads or writes.
#[derive(Clone, Debug)]
pub struct BlockManager {
    /// Transport frame payload size used when a caller does not request one.
    /// This controls network batching and does not define StorageChunk size.
    default_frame_size: u32,
    /// Upper bound for Worker-selected read response payload size.
    max_frame_size: u32,
    access: Arc<BlockAccessRegistry>,
}

impl BlockManager {
    pub const DEFAULT_FRAME_SIZE: u32 = 1024 * 1024;
    pub const MAX_FRAME_SIZE: u32 = beryl_proto::MAX_WORKER_DATA_FRAME_SIZE;
    pub fn new(default_frame_size: u32, max_frame_size: u32) -> Self {
        Self {
            default_frame_size,
            max_frame_size,
            access: Arc::new(BlockAccessRegistry {
                states: Mutex::new(HashMap::new()),
                changed: Notify::new(),
                block_report_changes: BlockReportChangeTracker::default(),
            }),
        }
    }

    pub const fn default_frame_size(&self) -> u32 {
        self.default_frame_size
    }

    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Pins a block before read validation and holds it through the `ReadBlock` response lifetime.
    pub(crate) fn pin_block(&self, group_name: &GroupName, block_id: BlockId) -> WorkerCoreResult<BlockPin> {
        self.access.pin_block(BlockAccessKey {
            group_name: group_name.clone(),
            block_id,
        })
    }

    /// Prevents new access and waits for existing `ReadBlock` pins before cleanup.
    pub(crate) fn begin_reclaim(&self, group_name: &GroupName, block_id: BlockId) -> WorkerCoreResult<ReclaimPermit> {
        self.access.begin_reclaim(BlockAccessKey {
            group_name: group_name.clone(),
            block_id,
        })
    }

    /// Lists exact block versions currently fenced from new access.
    pub(crate) fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<ReclaimingBlock> {
        self.access.reclaiming_blocks(group_name)
    }

    /// Returns the reportable deleting state for one exact block.
    pub(crate) fn reclaiming_block(&self, group_name: &GroupName, block_id: BlockId) -> Option<ReclaimingBlock> {
        self.access.reclaiming_block(group_name, block_id)
    }

    /// Returns the retained reclaim-lifecycle change source.
    pub(crate) fn block_report_changes(&self) -> &BlockReportChangeTracker {
        self.access.block_report_changes()
    }

    /// Waits for a reclaim lifecycle transition.
    pub(crate) async fn wait_for_block_report_change(&self) {
        self.access.wait_for_block_report_change().await;
    }

    /// Validates local Ready state against metadata facts while the caller holds a read pin.
    pub(crate) fn validate_read(
        &self,
        store: &(dyn LocalBlockStore + Send + Sync),
        req: &ReadBlockRequest,
    ) -> WorkerCoreResult<()> {
        self.validate_read_request(req)?;
        let meta = match store.load_meta(&req.group_name, req.block_id) {
            Ok(meta) => meta,
            Err(WorkerError::NotFound(message)) => {
                return Err(Self::refresh_metadata(
                    ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    format!("local block is not available for read: {message}"),
                ));
            }
            Err(error) => return Err(error),
        };
        if meta.visibility.block_state != BlockState::Ready {
            return Err(Self::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!(
                    "local block is not Ready: group_name={}, block_id={}, state={:?}",
                    req.group_name, req.block_id, meta.visibility.block_state
                ),
            ));
        }

        if req.block_format_id != meta.format.format_id
            || req.block_size != meta.format.block_size
            || u64::from(req.chunk_size) != meta.format.chunk_size
            || req.effective_len > meta.source.durable_len
        {
            return Err(Self::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                format!(
                    "block layout mismatch: group_name={}, block_id={}, requested_format={}, local_format={}, requested_block_size={}, local_block_size={}, requested_chunk_size={}, local_chunk_size={}, requested_effective_len={}, local_effective_len={}",
                    req.group_name,
                    req.block_id,
                    req.block_format_id.as_raw(),
                    meta.format.format_id.as_raw(),
                    req.block_size,
                    meta.format.block_size,
                    req.chunk_size,
                    meta.format.chunk_size,
                    req.effective_len,
                    meta.source.durable_len
                ),
            ));
        }

        let range_end = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        if req.byte_range.offset > meta.source.durable_len || range_end > meta.source.durable_len {
            return Err(WorkerError::InvalidArgument(format!(
                "byte range exceeds effective block length: group_name={}, block_id={}, offset={}, len={}, effective_len={}",
                req.group_name, req.block_id, req.byte_range.offset, req.byte_range.len, meta.source.durable_len
            )));
        }

        Ok(())
    }

    /// Rejects malformed or internally inconsistent read authority before pinning.
    pub(crate) fn validate_read_request(&self, req: &ReadBlockRequest) -> WorkerCoreResult<()> {
        BlockShape::new(req.block_format_id, req.block_size, req.chunk_size, req.effective_len)
            .map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;

        let range_end = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        if req.byte_range.offset > req.effective_len || range_end > req.effective_len {
            return Err(WorkerError::InvalidArgument(format!(
                "byte range exceeds expected block length: offset={}, len={}, effective_len={}",
                req.byte_range.offset, req.byte_range.len, req.effective_len
            )));
        }
        Ok(())
    }

    fn refresh_metadata(kind: ErrorKind, message: String) -> WorkerError {
        WorkerError::RefreshMetadata { kind, message }
    }
}

impl Default for BlockManager {
    fn default() -> Self {
        Self::new(Self::DEFAULT_FRAME_SIZE, Self::MAX_FRAME_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockIndex, InodeId};
    use std::time::Duration;

    #[tokio::test]
    async fn reclaim_drains_shared_io_pins_and_failure_keeps_admission_closed() {
        let manager = BlockManager::default();
        let group = GroupName::parse("root").unwrap();
        let id = BlockId::new(InodeId::new(7), BlockIndex::new(3));
        let pin = manager.pin_block(&group, id).unwrap();
        let io_pin = pin.clone();
        let permit = manager.begin_reclaim(&group, id).unwrap();
        assert!(pin.is_reclaiming());
        assert!(manager.pin_block(&group, id).is_err());
        assert!(manager.begin_reclaim(&group, id).is_err());
        drop(pin);
        assert!(tokio::time::timeout(Duration::from_millis(20), permit.wait_for_pins())
            .await
            .is_err());
        drop(io_pin);
        permit.wait_for_pins().await;
        drop(permit); // A cancelled destructive operation retains exclusion.
        assert!(manager.pin_block(&group, id).is_err());
        let retry = manager.begin_reclaim(&group, id).unwrap();
        retry.wait_for_pins().await;
        retry.complete();
        assert!(manager.pin_block(&group, id).is_ok());
    }
}
