// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker core domain types and data-plane facade.

use crate::error::WorkerError;
use crate::observe;
use crate::report::BlockReportChangeTracker;
use crate::runtime::block::{BlockManager, BlockPin, ReclaimingBlock};
use crate::runtime::write::{BlockWriteIoGuard, BlockWriteKey, BlockWriteRegistration, BlockWriteRegistry};
use crate::runtime::DataRpcPermit;
use crate::store::block::{
    CheckpointBlockRequest, ChecksumKind, LocalBlockStore, OpenBlockWriteRequest, ReclaimBlockRequest,
    ReclaimBlockResult,
};
use beryl_types::chunk::ByteRange;
use beryl_types::ids::BlockId;
use beryl_types::layout::{BlockFormatId, BlockShape, BlockShapeError};
use beryl_types::{FencingToken, GroupName, Tier, WorkerRunId};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

pub type WorkerCoreResult<T> = Result<T, WorkerError>;

const WRITE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const MAX_WRITE_CLEANUPS_PER_PASS: usize = 64;

/// Metadata-authorized block-local range requested by one read RPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadBlockRequest {
    pub(crate) group_name: GroupName,
    pub(crate) block_id: BlockId,
    /// Block-local range; its offset is relative to `block_id`.
    pub(crate) byte_range: ByteRange,

    pub(crate) block_format_id: BlockFormatId,
    pub(crate) block_size: u64,
    pub(crate) chunk_size: u32,
    pub(crate) effective_len: u64,
    /// Requested transport frame size, independent of the storage chunk size.
    pub(crate) frame_size: u32,
}

/// RPC-owned read cursor, pin, and admission permit. The pin and permit span
/// the response stream and every blocking read so reclamation and new admission
/// cannot pass unfinished filesystem access.
#[derive(Debug)]
pub(crate) struct ActiveBlockRead {
    group_name: GroupName,
    block_id: BlockId,
    next_offset: u64,
    end_offset: u64,
    frame_size: u32,
    read_pin: BlockPin,
    rpc_permit: Arc<DataRpcPermit>,
}

/// Metadata-issued facts fixed by the first message of one `WriteBlock` RPC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WriteBlockRequest {
    pub(crate) group_name: GroupName,
    pub(crate) block_id: BlockId,
    pub(crate) worker_run_id: WorkerRunId,
    pub(crate) fencing_token: FencingToken,
    pub(crate) write_offset: u64,
    pub(crate) block_size: u64,
    pub(crate) block_format_id: BlockFormatId,
    pub(crate) chunk_size: u32,
    pub(crate) checksum_kind: ChecksumKind,
    pub(crate) tier: Tier,
}

/// Mutable state owned by exactly one `WriteBlock` RPC.
///
/// Ordered gRPC delivery makes `next_offset` the only progress state required;
/// dropping this value schedules write cleanup through its registration.
pub(crate) struct ActiveBlockWrite {
    group_name: GroupName,
    block_id: BlockId,
    worker_run_id: WorkerRunId,
    fencing_token: FencingToken,
    block_size: u64,
    next_offset: u64,
    registration: Option<BlockWriteRegistration>,
}

impl ActiveBlockWrite {
    /// Waits for revocation even when the client sends no more request frames.
    pub(crate) async fn retired(&self) {
        self.registration
            .as_ref()
            .expect("active write owns its registration")
            .retired()
            .await;
    }
}

/// Data-plane lifecycle boundary used by the gRPC service.
#[derive(Clone)]
pub struct WorkerCore {
    block_manager: Arc<BlockManager>,
    block_store: Arc<dyn LocalBlockStore + Send + Sync>,
    block_writes: Arc<BlockWriteRegistry>,
}

impl WorkerCore {
    /// Creates a Worker data core around the configured local-store boundary.
    pub fn with_local_store(
        default_frame_size: u32,
        max_frame_size: u32,
        block_store: Arc<dyn LocalBlockStore + Send + Sync>,
    ) -> Self {
        Self {
            block_manager: Arc::new(BlockManager::new(default_frame_size, max_frame_size)),
            block_store,
            block_writes: Arc::new(BlockWriteRegistry::new()),
        }
    }

    /// Returns the frame size used when a read request does not specify one.
    pub fn default_frame_size(&self) -> u32 {
        self.block_manager.default_frame_size()
    }

    /// Returns the largest read response payload selected by Worker.
    pub fn max_frame_size(&self) -> u32 {
        self.block_manager.max_frame_size()
    }

    /// Validates one metadata-authorized range and binds its pin to an RPC-owned read.
    pub(crate) async fn begin_block_read(
        &self,
        req: ReadBlockRequest,
        rpc_permit: Arc<DataRpcPermit>,
    ) -> WorkerCoreResult<ActiveBlockRead> {
        let frame_size = self.negotiate_read_frame_size(req.frame_size)?;
        self.block_manager.validate_read_request(&req)?;
        let read_pin = self.block_manager.pin_block(&req.group_name, req.block_id)?;
        let validation_pin = read_pin.clone();
        let validation_rpc_permit = Arc::clone(&rpc_permit);
        let block_manager = Arc::clone(&self.block_manager);
        let block_store = Arc::clone(&self.block_store);
        let validation_request = req.clone();
        tokio::task::spawn_blocking(move || {
            let _pin = validation_pin;
            let _rpc_permit = validation_rpc_permit;
            block_manager.validate_read(block_store.as_ref(), &validation_request)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block read validation task failed: {error}")))??;
        let end_offset = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        Ok(ActiveBlockRead {
            group_name: req.group_name,
            block_id: req.block_id,
            next_offset: req.byte_range.offset,
            end_offset,
            frame_size,
            read_pin,
            rpc_permit,
        })
    }

    /// Pins pending online authorization so a delayed success cannot resurrect a reclaimed block.
    pub(crate) fn pin_write_authorization(&self, req: &WriteBlockRequest) -> WorkerCoreResult<BlockPin> {
        validate_write_block_request(req)?;
        self.block_manager.pin_block(&req.group_name, req.block_id)
    }

    /// Creates write state and transfers the RPC permit to its cleanup-owned entry.
    ///
    /// Success is the first local side effect acknowledged to the client. Any
    /// later transport failure therefore has an unknown outcome at the client.
    pub(crate) async fn begin_block_write(
        &self,
        req: WriteBlockRequest,
        rpc_permit: DataRpcPermit,
        block_pin: BlockPin,
        visible_len: u64,
    ) -> WorkerCoreResult<ActiveBlockWrite> {
        validate_write_block_request(&req)?;
        let key = BlockWriteKey {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
        };
        let authorization_pin = block_pin.clone();
        let registration = self
            .block_writes
            .register(key, rpc_permit, req.fencing_token, block_pin)
            .await
            .ok_or_else(|| {
                WorkerError::ResourceExhausted(format!(
                    "block already has an active write: group_name={}, block_id={}",
                    req.group_name, req.block_id
                ))
            })?;

        if authorization_pin.is_reclaiming() {
            return Err(WorkerError::Unavailable(
                "block reclamation started during write authorization".into(),
            ));
        }

        let block_store = Arc::clone(&self.block_store);
        let io_request = req.clone();
        let io_guard = begin_write_io(&registration)?;
        let create = tokio::task::spawn_blocking(move || {
            let _io_guard = io_guard;
            block_store.open_block_write(OpenBlockWriteRequest {
                group_name: io_request.group_name,
                block_id: io_request.block_id,
                block_size: io_request.block_size,
                block_format_id: io_request.block_format_id,
                chunk_size: io_request.chunk_size,
                checksum_kind: io_request.checksum_kind,
                tier: io_request.tier,
                fencing_token: io_request.fencing_token,
                write_offset: io_request.write_offset,
                visible_len,
            })
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block block-open checkpoint task failed: {error}")))?;

        if let Err(error) = create {
            let block_store = Arc::clone(&self.block_store);
            let group_name = req.group_name.clone();
            let block_id = req.block_id;
            let io_guard = begin_write_io(&registration)?;
            let cleanup = tokio::task::spawn_blocking(move || {
                let _io_guard = io_guard;
                block_store.discard_unsynced_suffix(&group_name, block_id)
            })
            .await
            .map_err(|join_error| WorkerError::Internal(format!("failed write cleanup task failed: {join_error}")))?;
            if cleanup.is_ok() {
                registration.complete();
            }
            return Err(error);
        }

        tracing::info!(
            target: "worker.state",
            op = "BeginBlockWrite",
            result = "accepted",
            error_code = "none",
            group_id = %req.group_name,
            block_id = %req.block_id,
            inode_id = req.block_id.inode_id.as_raw(),
            worker_run_id = %req.worker_run_id,
            lease_epoch = %req.fencing_token.epoch,
            "Block write accepted"
        );
        Ok(ActiveBlockWrite {
            group_name: req.group_name,
            block_id: req.block_id,
            worker_run_id: req.worker_run_id,
            fencing_token: req.fencing_token,
            block_size: req.block_size,
            next_offset: req.write_offset,
            registration: Some(registration),
        })
    }

    /// Appends one ordered, nonempty data message at the RPC-owned cursor.
    pub(crate) async fn write_block_data(&self, write: &mut ActiveBlockWrite, data: Bytes) -> WorkerCoreResult<()> {
        if data.is_empty() {
            return Err(WorkerError::InvalidArgument(
                "WriteBlock data must be nonempty".to_string(),
            ));
        }
        if data.len() > beryl_proto::MAX_WORKER_DATA_FRAME_SIZE as usize {
            return Err(WorkerError::InvalidArgument(format!(
                "WriteBlock data exceeds maximum frame size: actual={}, maximum={}",
                data.len(),
                beryl_proto::MAX_WORKER_DATA_FRAME_SIZE
            )));
        }
        let len = u64::try_from(data.len())
            .map_err(|_| WorkerError::InvalidArgument("write data length does not fit in u64".to_string()))?;
        let end_offset = write
            .next_offset
            .checked_add(len)
            .ok_or_else(|| WorkerError::InvalidArgument("write cursor overflow".to_string()))?;
        if end_offset > write.block_size {
            return Err(WorkerError::InvalidArgument(format!(
                "write exceeds block_size: end_offset={end_offset}, block_size={}",
                write.block_size
            )));
        }

        let block_store = Arc::clone(&self.block_store);
        let group_name = write.group_name.clone();
        let block_id = write.block_id;
        let offset = write.next_offset;
        let io_guard = begin_active_write_io(write)?;
        let store_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let _io_guard = io_guard;
            block_store.write_at(&group_name, block_id, offset, data)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block write task failed: {error}")))?;
        match result {
            Ok(()) => {
                write.next_offset = end_offset;
                observe::record_store_io("write", "ok", "none", len, store_started.elapsed().as_secs_f64());
                Ok(())
            }
            Err(error) => {
                observe::record_store_io(
                    "write",
                    "error",
                    observe::worker_error_kind(&error),
                    0,
                    store_started.elapsed().as_secs_f64(),
                );
                Err(error)
            }
        }
    }

    /// Publishes the complete staged prefix as durable local `Ready` state.
    /// Normal RPC completion is emitted only after this method succeeds.
    pub(crate) async fn finish_block_write(&self, write: &mut ActiveBlockWrite) -> WorkerCoreResult<()> {
        validate_effective_len(write.block_size, write.next_offset)?;
        let block_store = Arc::clone(&self.block_store);
        let group_name = write.group_name.clone();
        let block_id = write.block_id;
        let effective_len = write.next_offset;
        let fencing_token = write.fencing_token;
        let io_guard = begin_active_write_io(write)?;
        let meta = tokio::task::spawn_blocking(move || {
            let _io_guard = io_guard;
            block_store.checkpoint_block(CheckpointBlockRequest {
                group_name,
                block_id,
                effective_len,
                fencing_token,
            })
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block publish task failed: {error}")))??;

        tracing::info!(
            target: "worker.state",
            op = "FinishBlockWrite",
            result = "completed",
            error_code = "none",
            group_id = %write.group_name,
            block_id = %write.block_id,
            inode_id = write.block_id.inode_id.as_raw(),
            worker_run_id = %write.worker_run_id,
            lease_epoch = %meta.visibility.fencing_token.epoch,
            committed_length = meta.source.durable_len,
            "Block write completed"
        );
        write
            .registration
            .take()
            .expect("active block write owns one registration")
            .complete();
        Ok(())
    }

    /// Releases one failed RPC's uncheckpointed suffix before returning a protocol error.
    /// If cleanup itself fails, dropping the registration leaves it for retry.
    pub(crate) async fn abort_block_write(&self, mut write: ActiveBlockWrite) -> WorkerCoreResult<()> {
        let block_store = Arc::clone(&self.block_store);
        let group_name = write.group_name.clone();
        let block_id = write.block_id;
        let io_guard = begin_active_write_io(&write)?;
        tokio::task::spawn_blocking(move || {
            let _io_guard = io_guard;
            block_store.discard_unsynced_suffix(&group_name, block_id)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block abort task failed: {error}")))??;
        write
            .registration
            .take()
            .expect("active block write owns one registration")
            .complete();
        Ok(())
    }

    /// Runs bounded cleanup for writes whose owning RPC was cancelled or dropped.
    pub async fn run_block_write_cleanup(&self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(WRITE_CLEANUP_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {
                    self.cleanup_block_write_batch(false).await;
                }
            }
        }
    }

    /// Drains all RPC-owned write state until completion or process deadline.
    pub async fn drain_block_writes_until(&self, deadline: TokioInstant) -> bool {
        loop {
            if self.block_writes.active_count() == 0 {
                return false;
            }
            if TokioInstant::now() >= deadline {
                return true;
            }
            if tokio::time::timeout_at(deadline, self.cleanup_block_write_batch(true))
                .await
                .is_err()
            {
                return true;
            }
            if self.block_writes.active_count() == 0 {
                return false;
            }
            if TokioInstant::now() >= deadline {
                return true;
            }
            tokio::time::sleep_until((TokioInstant::now() + Duration::from_millis(10)).min(deadline)).await;
        }
    }

    /// Moves a whole claimed batch into one blocking job so cancellation of
    /// the async waiter cannot strand claims or detach unowned cleanup IO.
    async fn cleanup_block_write_batch(&self, drain: bool) -> usize {
        let candidates = self.block_writes.take_cleanup_batch(MAX_WRITE_CLEANUPS_PER_PASS, drain);
        if candidates.is_empty() {
            return 0;
        }
        let block_store = Arc::clone(&self.block_store);
        let cleanup = tokio::task::spawn_blocking(move || {
            let mut completed = 0;
            for candidate in candidates {
                let group_name = candidate.key.group_name.clone();
                let block_id = candidate.key.block_id;
                let result = block_store.discard_unsynced_suffix(&group_name, block_id);
                match result {
                    Ok(()) => {
                        completed += usize::from(candidate.complete());
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "worker.state",
                            op = "CleanupBlockWrite",
                            group_id = %candidate.key.group_name,
                            block_id = %candidate.key.block_id,
                            error_code = observe::worker_error_kind(&error),
                            error = %error,
                            "Block write cleanup retained local resources for retry"
                        );
                    }
                }
            }
            completed
        });
        match cleanup.await {
            Ok(completed) => completed,
            Err(error) => {
                tracing::warn!(
                    target: "worker.state",
                    op = "CleanupBlockWriteBatch",
                    error = %error,
                    "Block write cleanup batch task failed"
                );
                0
            }
        }
    }

    /// Reclaims one exact metadata-authorized block identity.
    pub async fn reclaim_block(&self, req: ReclaimBlockRequest) -> WorkerCoreResult<ReclaimBlockResult> {
        let permit = self.block_manager.begin_reclaim(&req.group_name, req.block_id)?;
        self.block_writes.retire(&BlockWriteKey {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
        });
        permit.wait_for_pins().await;
        let store = Arc::clone(&self.block_store);
        tokio::task::spawn_blocking(move || {
            let result = store.reclaim_block(&req)?;
            permit.complete();
            Ok(result)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block reclaim task failed: {error}")))?
    }

    pub(crate) fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<ReclaimingBlock> {
        self.block_manager.reclaiming_blocks(group_name)
    }

    /// Returns the reportable deleting state for one exact block.
    pub(crate) fn reclaiming_block(&self, group_name: &GroupName, block_id: BlockId) -> Option<ReclaimingBlock> {
        self.block_manager.reclaiming_block(group_name, block_id)
    }

    /// Returns retained runtime lifecycle changes for incremental reporting.
    pub(crate) fn block_report_changes(&self) -> &BlockReportChangeTracker {
        self.block_manager.block_report_changes()
    }

    /// Waits for a coalesced runtime block-report wake-up.
    pub(crate) async fn wait_for_block_report_change(&self) {
        self.block_manager.wait_for_block_report_change().await;
    }

    /// Reads the next exact chunk without executing filesystem work on Tokio workers.
    pub(crate) async fn read_block_chunk(&self, read: &mut ActiveBlockRead) -> WorkerCoreResult<Option<Bytes>> {
        if read.next_offset >= read.end_offset {
            return Ok(None);
        }
        let read_len = (read.end_offset - read.next_offset).min(u64::from(read.frame_size));
        let expected_len = usize::try_from(read_len)
            .map_err(|_| WorkerError::InvalidArgument("read length does not fit in usize".to_string()))?;
        let block_store = Arc::clone(&self.block_store);
        let group_name = read.group_name.clone();
        let block_id = read.block_id;
        let offset = read.next_offset;
        let io_pin = read.read_pin.clone();
        let io_rpc_permit = Arc::clone(&read.rpc_permit);
        let store_started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let _pin = io_pin;
            let _rpc_permit = io_rpc_permit;
            block_store.read_at(&group_name, block_id, offset, read_len)
        })
        .await
        .map_err(|error| WorkerError::Internal(format!("block read task failed: {error}")))?;
        let data = match result {
            Ok(data) => {
                observe::record_store_io(
                    "read",
                    "ok",
                    "none",
                    data.len() as u64,
                    store_started.elapsed().as_secs_f64(),
                );
                data
            }
            Err(error) => {
                observe::record_store_io(
                    "read",
                    "error",
                    observe::worker_error_kind(&error),
                    0,
                    store_started.elapsed().as_secs_f64(),
                );
                return Err(error);
            }
        };
        if data.len() != expected_len {
            return Err(WorkerError::Corrupt(format!(
                "block read returned {} bytes, expected {expected_len}",
                data.len()
            )));
        }
        read.next_offset = read
            .next_offset
            .checked_add(
                u64::try_from(data.len())
                    .map_err(|_| WorkerError::InvalidArgument("read chunk length does not fit in u64".to_string()))?,
            )
            .ok_or_else(|| WorkerError::InvalidArgument("read cursor overflow".to_string()))?;
        Ok(Some(data))
    }

    fn negotiate_read_frame_size(&self, requested_frame_size: u32) -> WorkerCoreResult<u32> {
        let frame_size = if requested_frame_size == 0 {
            self.default_frame_size()
        } else {
            requested_frame_size
        }
        .min(self.max_frame_size());
        if frame_size == 0 {
            return Err(WorkerError::InvalidArgument(
                "frame_size must be greater than zero after negotiation".to_string(),
            ));
        }
        Ok(frame_size)
    }
}

fn validate_write_block_request(req: &WriteBlockRequest) -> WorkerCoreResult<()> {
    if req.fencing_token.block_id != req.block_id
        || req.fencing_token.owner.is_zero()
        || req.fencing_token.epoch.as_raw() == 0
        || req.write_offset >= req.block_size
    {
        return Err(WorkerError::InvalidArgument(
            "invalid block writer token or offset".into(),
        ));
    }
    validate_block_shape(
        req.block_format_id,
        req.block_size,
        req.chunk_size,
        req.block_size,
        req.checksum_kind,
    )
}

fn validate_block_shape(
    block_format_id: BlockFormatId,
    block_size: u64,
    chunk_size: u32,
    effective_len: u64,
    checksum_kind: ChecksumKind,
) -> WorkerCoreResult<()> {
    BlockShape::new(block_format_id, block_size, chunk_size, effective_len)
        .map_err(|error| WorkerError::InvalidArgument(error.to_string()))?;
    if checksum_kind != ChecksumKind::None {
        return Err(WorkerError::InvalidArgument(
            "only checksum_kind None is supported".to_string(),
        ));
    }
    Ok(())
}

fn validate_effective_len(block_size: u64, effective_len: u64) -> WorkerCoreResult<()> {
    BlockShape::validate_effective_len(block_size, effective_len).map_err(|error| match error {
        BlockShapeError::ZeroEffectiveLen => {
            WorkerError::InvalidArgument("WriteBlock requires at least one data byte".to_string())
        }
        BlockShapeError::EffectiveLenExceedsBlock => WorkerError::InvalidArgument(format!(
            "effective_len exceeds block_size: effective_len={effective_len}, block_size={block_size}"
        )),
        other => WorkerError::InvalidArgument(other.to_string()),
    })
}

fn begin_write_io(registration: &BlockWriteRegistration) -> WorkerCoreResult<BlockWriteIoGuard> {
    registration
        .begin_io()
        .ok_or_else(|| WorkerError::Cancelled("block write is retiring".to_string()))
}

fn begin_active_write_io(write: &ActiveBlockWrite) -> WorkerCoreResult<BlockWriteIoGuard> {
    begin_write_io(
        write
            .registration
            .as_ref()
            .ok_or_else(|| WorkerError::Internal("active block write is missing its registration".to_string()))?,
    )
}

#[cfg(test)]
mod tests {
    use super::{ReadBlockRequest, WorkerCore, WriteBlockRequest};
    use crate::error::WorkerError;
    use crate::runtime::DataRpcPermit;
    use crate::store::block::{BlockMetaPayload, BlockState};
    use crate::store::block::{
        CheckpointBlockRequest, ChecksumKind, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
        OpenBlockWriteRequest, ReclaimBlockRequest, ReclaimBlockResult, ReclaimBlockState, StoreResult,
    };
    use beryl_common::error::rpc::{ErrorKind, WorkerErrorKind};
    use beryl_types::chunk::ByteRange;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::{GroupName, Tier, WorkerRunId};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{Receiver, Sender};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::Semaphore;
    use tokio::time::Instant;

    const BLOCK_SIZE: u64 = 4096;
    const LEASE_EPOCH: u64 = 55;

    fn chunk_size() -> u32 {
        BlockFormatId::DURABLE_PREFIX.spec().unwrap().storage_chunk_size
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("group name")
    }

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(3))
    }

    fn write_request() -> WriteBlockRequest {
        WriteBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: WorkerRunId::new(),
            fencing_token: beryl_types::FencingToken::new(
                block_id(),
                beryl_types::ClientId::new(9),
                beryl_types::LeaseEpoch::new(LEASE_EPOCH),
            ),
            write_offset: 0,
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::DURABLE_PREFIX,
            chunk_size: chunk_size(),
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        }
    }

    fn write_request_for(block_id: BlockId) -> WriteBlockRequest {
        let mut req = write_request();
        req.block_id = block_id;
        req.fencing_token.block_id = block_id;
        req
    }

    impl WorkerCore {
        async fn begin_test_write(
            &self,
            req: WriteBlockRequest,
            permit: DataRpcPermit,
        ) -> super::WorkerCoreResult<super::ActiveBlockWrite> {
            let pin = self.pin_write_authorization(&req)?;
            self.begin_block_write(req, permit, pin, 0).await
        }
    }

    fn rpc_permit(slots: Arc<Semaphore>, mode: &'static str) -> DataRpcPermit {
        DataRpcPermit::new(slots.try_acquire_owned().expect("test RPC capacity"), mode)
    }

    fn write_rpc_permit() -> DataRpcPermit {
        rpc_permit(Arc::new(Semaphore::new(1)), "write")
    }

    fn read_rpc_permit() -> Arc<DataRpcPermit> {
        Arc::new(rpc_permit(Arc::new(Semaphore::new(1)), "read"))
    }

    fn core_with_store() -> (TempDir, Arc<FullBlockFileStore>, WorkerCore) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let core = WorkerCore::with_local_store(512, 2048, store.clone());
        (temp, store, core)
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum BlockingOperation {
        Abort,
        Create,
        PanicFirstAbort,
        Publish,
        Write,
        Read,
    }

    struct BlockingStore {
        inner: Arc<FullBlockFileStore>,
        operation: BlockingOperation,
        started: Mutex<Option<Sender<()>>>,
        release: Mutex<Receiver<()>>,
        abort_calls: Arc<AtomicUsize>,
    }

    impl BlockingStore {
        fn block_once(&self, operation: BlockingOperation) {
            if self.operation != operation {
                return;
            }
            let Some(started) = self.started.lock().expect("blocking store started sender").take() else {
                return;
            };
            started.send(()).expect("report blocking store operation");
            self.release
                .lock()
                .expect("blocking store release receiver")
                .recv()
                .expect("release blocking store operation");
        }
    }

    impl LocalBlockStore for BlockingStore {
        fn open_block_write(&self, req: OpenBlockWriteRequest) -> StoreResult<BlockMetaPayload> {
            self.block_once(BlockingOperation::Create);
            self.inner.open_block_write(req)
        }

        fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
            self.block_once(BlockingOperation::Write);
            self.inner.write_at(group_name, block_id, offset, data)
        }

        fn checkpoint_block(&self, req: CheckpointBlockRequest) -> StoreResult<BlockMetaPayload> {
            self.block_once(BlockingOperation::Publish);
            self.inner.checkpoint_block(req)
        }

        fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
            self.block_once(BlockingOperation::Read);
            self.inner.read_at(group_name, block_id, offset, len)
        }

        fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
            self.inner.load_meta(group_name, block_id)
        }

        fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
            self.inner.inspect_reclaim_block(req)
        }

        fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
            self.inner.reclaim_block(req)
        }

        fn discard_unsynced_suffix(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
            let call = self.abort_calls.fetch_add(1, Ordering::SeqCst);
            if self.operation == BlockingOperation::PanicFirstAbort && call == 0 {
                panic!("injected abort panic");
            }
            self.block_once(BlockingOperation::Abort);
            self.inner.discard_unsynced_suffix(group_name, block_id)
        }
    }

    struct BlockingCoreFixture {
        _temp: TempDir,
        store: Arc<FullBlockFileStore>,
        core: Arc<WorkerCore>,
        started: Receiver<()>,
        release: Sender<()>,
        abort_calls: Arc<AtomicUsize>,
    }

    fn blocking_core(operation: BlockingOperation) -> BlockingCoreFixture {
        let temp = TempDir::new().expect("tempdir");
        let inner = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let abort_calls = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn LocalBlockStore + Send + Sync> = Arc::new(BlockingStore {
            inner: Arc::clone(&inner),
            operation,
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
            abort_calls: Arc::clone(&abort_calls),
        });
        let core = Arc::new(WorkerCore::with_local_store(512, 2048, store));
        BlockingCoreFixture {
            _temp: temp,
            store: inner,
            core,
            started: started_rx,
            release: release_tx,
            abort_calls,
        }
    }

    fn read_request(len: u32) -> ReadBlockRequest {
        ReadBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
            byte_range: ByteRange { offset: 0, len },

            block_format_id: BlockFormatId::DURABLE_PREFIX,
            block_size: BLOCK_SIZE,
            chunk_size: chunk_size(),
            effective_len: 8,
            frame_size: 512,
        }
    }

    #[tokio::test]
    async fn failed_or_cancelled_write_is_cleaned_and_can_be_reused() {
        let (_temp, _store, core) = core_with_store();
        let mut write = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("begin write");
        core.write_block_data(&mut write, Bytes::from_static(b"partial"))
            .await
            .expect("partial data");
        drop(write);
        assert!(
            !core
                .drain_block_writes_until(Instant::now() + Duration::from_secs(1))
                .await
        );

        let reused = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("reuse block");
        core.abort_block_write(reused).await.expect("abort reuse");
    }

    #[tokio::test]
    async fn cancelled_create_stays_owned_until_blocking_io_exits() {
        let BlockingCoreFixture {
            _temp,
            store: _store,
            core,
            started,
            release,
            abort_calls: _abort_calls,
        } = blocking_core(BlockingOperation::Create);
        let write_slots = Arc::new(Semaphore::new(1));
        let rpc_permit = rpc_permit(Arc::clone(&write_slots), "write");
        let task_core = Arc::clone(&core);
        let write = tokio::spawn(async move { task_core.begin_test_write(write_request(), rpc_permit).await });
        tokio::task::spawn_blocking(move || started.recv().expect("blocking create started"))
            .await
            .expect("wait for create");

        write.abort();
        match write.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("begin write task must be cancelled"),
        }
        assert_eq!(write_slots.available_permits(), 0);
        assert!(
            core.drain_block_writes_until(Instant::now() + Duration::from_millis(50))
                .await,
            "cleanup must not release ownership while create IO is still running"
        );

        release.send(()).expect("release create");
        assert!(
            !core
                .drain_block_writes_until(Instant::now() + Duration::from_secs(1))
                .await
        );
        assert_eq!(write_slots.available_permits(), 1);
        let reused = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("reuse after cleanup");
        core.abort_block_write(reused).await.expect("abort reused write");
    }

    #[tokio::test]
    async fn cancelled_publish_cannot_be_cleaned_before_ready_io_exits() {
        let BlockingCoreFixture {
            _temp,
            store,
            core,
            started,
            release,
            abort_calls: _abort_calls,
        } = blocking_core(BlockingOperation::Publish);
        let mut write = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("begin write");
        core.write_block_data(&mut write, Bytes::from_static(b"ready"))
            .await
            .expect("write data");
        let task_core = Arc::clone(&core);
        let publish = tokio::spawn(async move { task_core.finish_block_write(&mut write).await });
        tokio::task::spawn_blocking(move || started.recv().expect("blocking publish started"))
            .await
            .expect("wait for publish");

        publish.abort();
        assert!(publish.await.expect_err("finish write cancelled").is_cancelled());
        assert!(
            core.drain_block_writes_until(Instant::now() + Duration::from_millis(50))
                .await,
            "cleanup must not race a detached publish"
        );

        release.send(()).expect("release publish");
        assert!(
            !core
                .drain_block_writes_until(Instant::now() + Duration::from_secs(1))
                .await
        );
        let meta = store.load_meta(&group_name(), block_id()).expect("ready metadata");
        assert_eq!(meta.visibility.block_state, BlockState::Ready);
        assert_eq!(store.read_at(&group_name(), block_id(), 0, 5).unwrap(), b"ready"[..]);
    }

    #[tokio::test]
    async fn cancelled_cleanup_waiter_keeps_claim_owned_until_abort_finishes() {
        let BlockingCoreFixture {
            _temp,
            store: _store,
            core,
            started,
            release,
            abort_calls,
        } = blocking_core(BlockingOperation::Abort);
        let write = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("begin write");
        drop(write);

        let cleanup_core = Arc::clone(&core);
        let cleanup = tokio::spawn(async move { cleanup_core.cleanup_block_write_batch(false).await });
        tokio::task::spawn_blocking(move || started.recv().expect("blocking abort started"))
            .await
            .expect("wait for abort");
        cleanup.abort();
        assert!(cleanup.await.expect_err("cleanup waiter cancelled").is_cancelled());

        assert!(
            core.drain_block_writes_until(Instant::now() + Duration::from_millis(50))
                .await,
            "a second cleanup pass must wait for the claimed abort"
        );
        assert_eq!(abort_calls.load(Ordering::SeqCst), 1);
        match core.begin_test_write(write_request(), write_rpc_permit()).await {
            Err(WorkerError::ResourceExhausted(_)) => {}
            Err(error) => panic!("unexpected reuse error while cleanup is claimed: {error:?}"),
            Ok(_) => panic!("new write must not replace an entry with cleanup in progress"),
        }

        release.send(()).expect("release abort");
        assert!(
            !core
                .drain_block_writes_until(Instant::now() + Duration::from_secs(1))
                .await
        );
        let reused = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("reuse after claimed cleanup exits");
        core.abort_block_write(reused).await.expect("abort reused write");
    }

    #[tokio::test]
    async fn drain_deadline_detaches_owned_cleanup_and_returns_forced() {
        let BlockingCoreFixture {
            _temp,
            store: _store,
            core,
            started,
            release,
            abort_calls,
        } = blocking_core(BlockingOperation::Abort);
        let write = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("begin write");
        drop(write);

        let drain_core = Arc::clone(&core);
        let drain = tokio::spawn(async move {
            drain_core
                .drain_block_writes_until(Instant::now() + Duration::from_millis(50))
                .await
        });
        tokio::task::spawn_blocking(move || started.recv().expect("drain abort started"))
            .await
            .expect("wait for drain abort");

        assert!(
            tokio::time::timeout(Duration::from_secs(1), drain)
                .await
                .expect("drain observes deadline")
                .expect("drain task"),
            "blocked cleanup must force the shutdown drain"
        );
        assert_eq!(abort_calls.load(Ordering::SeqCst), 1);

        release.send(()).expect("release drain abort");
        assert!(
            !core
                .drain_block_writes_until(Instant::now() + Duration::from_secs(1))
                .await
        );
        let reused = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("reuse after detached cleanup exits");
        core.abort_block_write(reused).await.expect("abort reused write");
    }

    #[tokio::test]
    async fn cleanup_panic_releases_the_whole_claimed_batch_for_retry() {
        let BlockingCoreFixture {
            _temp,
            store: _store,
            core,
            started: _started,
            release: _release,
            abort_calls,
        } = blocking_core(BlockingOperation::PanicFirstAbort);
        let other_block_id = BlockId::new(InodeId::new(7), BlockIndex::new(4));
        let first = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("first write");
        let second = core
            .begin_test_write(write_request_for(other_block_id), write_rpc_permit())
            .await
            .expect("second write");
        drop((first, second));

        assert_eq!(core.cleanup_block_write_batch(false).await, 0);
        assert!(
            !core
                .drain_block_writes_until(Instant::now() + Duration::from_secs(1))
                .await,
            "all claims in the panicked batch must be retryable"
        );
        assert_eq!(abort_calls.load(Ordering::SeqCst), 3);

        let first = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("reuse first block");
        core.abort_block_write(first).await.expect("abort first reuse");
        let second = core
            .begin_test_write(write_request_for(other_block_id), write_rpc_permit())
            .await
            .expect("reuse second block");
        core.abort_block_write(second).await.expect("abort second reuse");
    }

    #[tokio::test]
    async fn block_write_rejects_empty_and_capacity_overflow() {
        let (_temp, _store, core) = core_with_store();
        let mut request = write_request();
        request.block_size = 3;
        let mut write = core
            .begin_test_write(request, write_rpc_permit())
            .await
            .expect("begin write");
        assert!(core.write_block_data(&mut write, Bytes::new()).await.is_err());
        assert!(core
            .write_block_data(&mut write, Bytes::from_static(b"four"))
            .await
            .is_err());
        core.abort_block_write(write).await.expect("abort");
    }

    #[tokio::test]
    async fn cancelled_blocking_read_keeps_reclaim_pin_until_io_exits() {
        let (_temp, store, core) = core_with_store();
        let mut write = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .expect("begin write");
        core.write_block_data(&mut write, Bytes::from_static(b"abcdefgh"))
            .await
            .expect("write data");
        core.finish_block_write(&mut write).await.expect("finish write");
        drop(core);

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocking_store: Arc<dyn LocalBlockStore + Send + Sync> = Arc::new(BlockingStore {
            inner: store,
            operation: BlockingOperation::Read,
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
            abort_calls: Arc::new(AtomicUsize::new(0)),
        });
        let core = Arc::new(WorkerCore::with_local_store(512, 2048, blocking_store));
        let read_slots = Arc::new(Semaphore::new(1));
        let rpc_permit = Arc::new(rpc_permit(Arc::clone(&read_slots), "read"));
        let mut read = core
            .begin_block_read(read_request(8), rpc_permit)
            .await
            .expect("begin read");
        let read_core = Arc::clone(&core);
        let read_task = tokio::spawn(async move { read_core.read_block_chunk(&mut read).await });
        tokio::task::spawn_blocking(move || started_rx.recv().expect("blocking read started"))
            .await
            .expect("wait for read");
        read_task.abort();
        assert!(read_task.await.expect_err("read task cancelled").is_cancelled());
        assert_eq!(read_slots.available_permits(), 0);

        let reclaim_core = Arc::clone(&core);
        let reclaim = tokio::spawn(async move {
            reclaim_core
                .reclaim_block(ReclaimBlockRequest {
                    group_name: group_name(),
                    block_id: block_id(),
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match core.begin_block_read(read_request(1), read_rpc_permit()).await {
                    Err(WorkerError::RefreshMetadata {
                        kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                        ..
                    }) => break,
                    Ok(extra) => drop(extra),
                    Err(error) => panic!("unexpected read-open result while reclaim starts: {error:?}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reclaim starts");
        assert!(!reclaim.is_finished(), "reclaim passed cancelled blocking IO");

        release_tx.send(()).expect("release read");
        assert!(tokio::time::timeout(Duration::from_secs(1), reclaim)
            .await
            .expect("reclaim completes")
            .expect("reclaim task")
            .is_ok());
        assert_eq!(read_slots.available_permits(), 1);
    }
    #[tokio::test]
    async fn new_epoch_waits_for_old_io_and_fences_the_lingering_stream() {
        let BlockingCoreFixture {
            _temp,
            store,
            core,
            started,
            release,
            ..
        } = blocking_core(BlockingOperation::Write);
        let mut old = core
            .begin_test_write(write_request(), write_rpc_permit())
            .await
            .unwrap();
        let old_core = core.clone();
        let io = tokio::spawn(async move {
            old_core
                .write_block_data(&mut old, Bytes::from_static(b"orphan"))
                .await
                .unwrap();
            old
        });
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        let mut next = write_request();
        next.fencing_token.epoch = beryl_types::LeaseEpoch::new(56);
        let takeover = core.begin_test_write(next, write_rpc_permit());
        tokio::pin!(takeover);
        assert!(futures::poll!(&mut takeover).is_pending());
        assert_eq!(
            core.cleanup_block_write_batch(false).await,
            0,
            "actual IO pins old ownership"
        );
        assert_eq!(
            store
                .load_meta(&group_name(), block_id())
                .unwrap()
                .visibility
                .fencing_token
                .epoch
                .as_raw(),
            55
        );
        release.send(()).unwrap();
        let mut old = io.await.unwrap();
        assert!(core
            .write_block_data(&mut old, Bytes::from_static(b"late"))
            .await
            .is_err());
        assert_eq!(core.cleanup_block_write_batch(false).await, 1);
        let mut new = takeover.await.unwrap();
        assert!(
            core.finish_block_write(&mut old).await.is_err(),
            "old EOF cannot checkpoint the new epoch"
        );
        core.write_block_data(&mut new, Bytes::from_static(b"new"))
            .await
            .unwrap();
        core.finish_block_write(&mut new).await.unwrap();
        assert_eq!(store.read_at(&group_name(), block_id(), 0, 3).unwrap(), b"new"[..]);
    }

    #[tokio::test]
    async fn reclaim_fences_authorization_that_finishes_after_the_delete_gate() {
        let (_temp, store, core) = core_with_store();
        let request = write_request();
        let pending_authorization = core.pin_write_authorization(&request).unwrap();
        let reclaim = core.reclaim_block(ReclaimBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
        });
        tokio::pin!(reclaim);
        assert!(futures::poll!(&mut reclaim).is_pending());
        assert!(core.pin_write_authorization(&request).is_err());
        assert!(core
            .begin_block_write(request, write_rpc_permit(), pending_authorization, 0)
            .await
            .is_err());
        assert_eq!(core.cleanup_block_write_batch(false).await, 1);
        assert_eq!(reclaim.await.unwrap(), ReclaimBlockResult::AlreadyAbsent);
        assert!(matches!(
            store.load_meta(&group_name(), block_id()),
            Err(WorkerError::NotFound(_))
        ));
    }
}
