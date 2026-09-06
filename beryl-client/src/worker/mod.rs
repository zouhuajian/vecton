// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-owned Worker orchestration, transport, and block-stream state.
//!
//! This module stays private to the crate, so stream handles and block-local
//! worker operations do not appear in the public API.

mod channel_pool;
mod client;
mod protocol;
mod transport;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use beryl_types::{GroupName, LocatedBlock};
use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::error::{ClientError, ClientResult};
use crate::planner::PlannedBlockRead;
use crate::runtime::{AttemptContext, OperationContext};

/// Internal boundary that isolates Worker RPC transport from client runtime
/// and provides a narrow seam for orchestration tests.
#[async_trait]
pub(crate) trait WorkerTransport: Send + Sync {
    /// Reads one metadata-planned block-local range with exact-length semantics.
    async fn read_block_range(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_read: &PlannedBlockRead,
        output: &mut [u8],
    ) -> ClientResult<()>;

    /// Opens one metadata-authorized block write and returns only after the
    /// Worker acknowledges write ownership.
    async fn open_write_block(
        &self,
        attempt: AttemptContext,
        target: WorkerWriteTarget,
        lease_expires_at_ms: u64,
    ) -> ClientResult<BlockWrite>;
}

/// Internal worker write target derived from metadata AllocateBlock.
#[derive(Clone, Debug)]
pub(crate) struct WorkerWriteTarget {
    /// Metadata owner group for the target block.
    pub(crate) group_name: GroupName,
    /// Metadata AllocateBlock target.
    pub(crate) target: LocatedBlock,
}

/// Renewable deadline shared by one `BlockWrite` and its response task.
///
/// Renewal and expiry use compare-and-swap so a timely renewal cannot be lost
/// to delayed task polling, while an expiry that wins cannot be revived.
pub(crate) struct BlockWriteLease {
    expires_at_ms: AtomicU64,
    updates: watch::Sender<()>,
}

impl BlockWriteLease {
    /// Creates the lease state from Metadata's acknowledged session deadline.
    pub(crate) fn new(expires_at_ms: u64) -> Self {
        let (updates, _) = watch::channel(());
        Self {
            expires_at_ms: AtomicU64::new(expires_at_ms),
            updates,
        }
    }

    /// Subscribes a blocked send or response task to successful renewals.
    fn subscribe(&self) -> watch::Receiver<()> {
        self.updates.subscribe()
    }

    /// Returns the current absolute deadline, or zero after expiry wins.
    fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms.load(Ordering::Acquire)
    }

    /// Fails if this block RPC has already crossed its current lease boundary.
    fn ensure_live(&self) -> ClientResult<()> {
        if self.expire_if_due() {
            return Err(write_lease_expired_error());
        }
        Ok(())
    }

    /// Installs a renewed deadline only while the previous lease is still live.
    fn renew(&self, expires_at_ms: u64) -> ClientResult<()> {
        let observed_at_ms = unix_now_ms();
        if expires_at_ms <= observed_at_ms {
            return Err(write_lease_expired_error());
        }
        loop {
            let current = self.expires_at_ms.load(Ordering::Acquire);
            if current == 0 {
                return Err(write_lease_expired_error());
            }
            if current <= observed_at_ms {
                match self
                    .expires_at_ms
                    .compare_exchange(current, 0, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return Err(write_lease_expired_error()),
                    Err(_) => continue,
                }
            }
            match self
                .expires_at_ms
                .compare_exchange(current, expires_at_ms, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.updates.send_replace(());
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }

    /// Atomically marks an elapsed deadline. Returns true once expiry has won.
    fn expire_if_due(&self) -> bool {
        let now_ms = unix_now_ms();
        loop {
            let current = self.expires_at_ms.load(Ordering::Acquire);
            if current == 0 {
                return true;
            }
            if current > now_ms {
                return false;
            }
            if self
                .expires_at_ms
                .compare_exchange(current, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }
}

/// Inputs accepted by an acknowledged block RPC. Only `Finish` may become a
/// normal request-stream EOF; dropping the sender is cancellation.
pub(crate) enum BlockWriteInput {
    Data(beryl_proto::worker::WriteBlockRequestProto),
    Finish,
}

/// One acknowledged Worker `WriteBlock` RPC owned by a sequential file writer.
///
/// Dropping this value aborts the response task. The transport request stream
/// treats a dropped sender as pending cancellation, never as a successful EOF.
pub(crate) struct BlockWrite {
    operation: OperationContext,
    target: LocatedBlock,
    written_len: u64,
    requests: Option<mpsc::Sender<BlockWriteInput>>,
    cancellation: Option<watch::Sender<bool>>,
    lease: Arc<BlockWriteLease>,
    completion: Option<JoinHandle<ClientResult<()>>>,
}

impl BlockWrite {
    /// Takes ownership of an acknowledged RPC and its exact metadata target.
    pub(crate) fn new(
        operation: OperationContext,
        target: LocatedBlock,
        requests: mpsc::Sender<BlockWriteInput>,
        cancellation: watch::Sender<bool>,
        lease: Arc<BlockWriteLease>,
        completion: JoinHandle<ClientResult<()>>,
    ) -> Self {
        Self {
            operation,
            written_len: target.write_offset,
            target,
            requests: Some(requests),
            cancellation: Some(cancellation),
            lease,
            completion: Some(completion),
        }
    }

    /// Remaining authorized bytes in this block.
    pub(crate) fn remaining(&self) -> u64 {
        self.target.block_size - self.written_len
    }

    /// Updates the deadline only if the previous lease is still locally live.
    /// A renewal observed after the old expiry cannot revive this block RPC.
    pub(crate) fn update_lease_expiry(&self, expires_at_ms: u64) -> ClientResult<()> {
        self.lease
            .renew(expires_at_ms)
            .map_err(|error| error.with_operation_context(&self.operation))
    }

    /// Fails promptly when the response task has already observed a terminal
    /// Worker result while the caller was between public write operations.
    pub(crate) async fn check_open(&mut self) -> ClientResult<()> {
        self.lease
            .ensure_live()
            .map_err(|error| error.with_operation_context(&self.operation))?;
        if !self.completion.as_ref().is_some_and(JoinHandle::is_finished) {
            return Ok(());
        }
        let result = self.await_completion().await?;
        match result {
            Ok(()) => Err(ClientError::unknown_outcome(
                "worker WriteBlock ended before the client finished its request stream".to_string(),
            )
            .with_operation_context(&self.operation)),
            Err(error) => Err(error.with_operation_context(&self.operation)),
        }
    }

    /// Sends one bounded, nonempty data frame and advances the block cursor only
    /// after the request channel accepts ownership of the bytes.
    pub(crate) async fn write(&mut self, data: Bytes) -> ClientResult<()> {
        self.check_open().await?;
        let len = u64::try_from(data.len())
            .map_err(|_| ClientError::invalid_argument("WriteBlock data length does not fit in u64".to_string()))?;
        if len > self.remaining() {
            return Err(ClientError::invalid_argument(format!(
                "WriteBlock data exceeds remaining block capacity: actual={len}, remaining={}",
                self.remaining()
            )));
        }
        let request = protocol::build_write_block_data(data)?;
        self.send_before_lease_expiry(BlockWriteInput::Data(request)).await?;
        self.written_len = self
            .written_len
            .checked_add(len)
            .ok_or_else(|| ClientError::invalid_argument("WriteBlock cursor overflow".to_string()))?;
        Ok(())
    }

    /// Half-closes the request stream and accepts the block only after the
    /// Worker response stream ends normally, which is the Ready boundary.
    pub(crate) async fn finish(mut self) -> ClientResult<(LocatedBlock, u64)> {
        self.check_open().await?;
        self.send_before_lease_expiry(BlockWriteInput::Finish).await?;
        self.requests.take();
        let result = self.await_completion().await?;
        self.cancellation.take();
        match result {
            Ok(()) => Ok((self.target.clone(), self.written_len)),
            Err(error) => Err(error.with_operation_context(&self.operation)),
        }
    }

    /// Requests Worker failure cleanup and waits only within the current public
    /// operation. A timed-out completion task stays detached under lease expiry.
    pub(crate) async fn cancel(mut self, timeout: Duration) -> bool {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.send_replace(true);
        }
        self.requests.take();
        if let Some(completion) = self.completion.take() {
            if timeout.is_zero() {
                return false;
            }
            return tokio::time::timeout(timeout, completion).await.is_ok();
        }
        true
    }

    /// Waits for one bounded request slot while synchronously arbitrating every
    /// lease renewal and the current absolute expiry.
    async fn send_before_lease_expiry(&mut self, input: BlockWriteInput) -> ClientResult<()> {
        let accepted = {
            let sender = self
                .requests
                .as_ref()
                .ok_or_else(|| ClientError::worker("WriteBlock request stream is closed".to_string()))?;
            let mut lease_updates = self.lease.subscribe();
            loop {
                self.lease
                    .ensure_live()
                    .map_err(|error| error.with_operation_context(&self.operation))?;
                let expires_at_ms = self.lease.expires_at_ms();
                let lease_expiry = tokio::time::sleep(duration_until_unix_ms(expires_at_ms));
                tokio::pin!(lease_expiry);
                tokio::select! {
                    biased;
                    changed = lease_updates.changed() => {
                        if changed.is_err() {
                            return Err(ClientError::unknown_outcome(
                                "worker WriteBlock lost its lease owner before request send".to_string(),
                            )
                            .with_operation_context(&self.operation));
                        }
                    }
                    _ = &mut lease_expiry => {
                        self.lease
                            .ensure_live()
                            .map_err(|error| error.with_operation_context(&self.operation))?;
                    }
                    permit = sender.reserve() => {
                        break match permit {
                            Ok(permit) => {
                                permit.send(input);
                                true
                            }
                            Err(_) => false,
                        };
                    }
                }
            }
        };
        if accepted {
            return Ok(());
        }
        match self.await_completion().await? {
            Ok(()) => Err(ClientError::unknown_outcome(
                "worker WriteBlock request stream closed before finish".to_string(),
            )
            .with_operation_context(&self.operation)),
            Err(error) => Err(error.with_operation_context(&self.operation)),
        }
    }

    async fn await_completion(&mut self) -> ClientResult<ClientResult<()>> {
        let completion = self
            .completion
            .take()
            .ok_or_else(|| ClientError::worker("WriteBlock completion task is missing".to_string()))?;
        completion.await.map_err(|error| {
            ClientError::unknown_outcome(format!(
                "worker WriteBlock completion task failed after acknowledgement: {error}"
            ))
            .with_operation_context(&self.operation)
        })
    }
}

impl Drop for BlockWrite {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.send_replace(true);
        }
        // Dropping the handle detaches the task so it can observe Worker's
        // cleanup status after the cancellation frame is sent.
        self.completion.take();
        self.requests.take();
    }
}

pub(crate) use client::WorkerClient;

pub(super) fn duration_until_unix_ms(expires_at_ms: u64) -> Duration {
    Duration::from_millis(expires_at_ms.saturating_sub(unix_now_ms()))
}

pub(super) fn write_lease_expired_error() -> ClientError {
    ClientError::session_expired_unknown("worker WriteBlock write lease expired after acknowledgement")
}

pub(super) fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
