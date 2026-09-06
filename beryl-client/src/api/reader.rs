// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Bounded sequential and positioned file reads.

use crate::client_inner::{metric_labels, refresh_hint_from_error, ClientInner};
use crate::error::{ClientError, ClientResult};
use crate::metadata::{OpenedFile, ReadLayout};
use crate::metrics::ClientMetric;
use crate::planner;
use crate::planner::{PlannedBlockRead, RequestedReadRange};
use crate::runtime::{retry_decision, Operation, OperationContext, OperationDeadline, RetryDecision};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::GroupName;
use bytes::Bytes;
use std::fmt::{Debug, Formatter, Result};
use std::sync::Arc;

/// Reads against the inode, content generation, and length captured at open.
///
/// Fresh layouts must match that authority; cached block plans retain their
/// existing lifetime. This reader does not retain historical file contents.
pub struct FileReader {
    inner: Arc<ClientInner>,
    file: OpenedFile,
    position: u64,
    current_block: Option<CurrentBlockPlan>,
}

impl FileReader {
    /// Creates a reader from Metadata-validated opened-file state.
    pub(crate) fn new(inner: Arc<ClientInner>, file: OpenedFile) -> Self {
        Self {
            inner,
            file,
            position: 0,
            current_block: None,
        }
    }

    /// Returns the namespace path used to open this file.
    pub fn path(&self) -> &str {
        self.file.path()
    }

    /// Returns the immutable file length observed by `open`.
    pub fn len(&self) -> u64 {
        self.file.len()
    }

    /// Returns whether the opened file is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the next offset used by the sequential read methods.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Reads one sequential step and advances only after full success.
    ///
    /// A successful read may stop at EOF, the configured request bound, or the
    /// current block boundary. Zero is returned only for an empty buffer or EOF.
    pub async fn read(&mut self, dst: &mut [u8]) -> ClientResult<usize> {
        if dst.is_empty() || self.position >= self.len() {
            return Ok(0);
        }
        let mut current_block = self.current_block.take();
        let result = self
            .read_sequential_step(
                self.position,
                dst,
                &mut current_block,
                self.inner.metadata.operation_deadline(),
            )
            .await;
        match result {
            Ok(read) => {
                self.position += read as u64;
                self.current_block = current_block;
                Ok(read)
            }
            Err(error) => Err(error),
        }
    }

    /// Reads one positioned step without changing the sequential position.
    ///
    /// A successful read may stop at EOF or the configured request bound.
    pub async fn read_at(&self, offset: u64, dst: &mut [u8]) -> ClientResult<usize> {
        let Some(range) = self.bounded_range(offset, dst.len())? else {
            return Ok(0);
        };
        self.read_range_step(
            range,
            &mut dst[..range.len as usize],
            self.inner.metadata.operation_deadline(),
        )
        .await?;
        Ok(range.len as usize)
    }

    /// Fills the entire positioned buffer without changing the sequential position.
    ///
    /// The method uses bounded internal steps and returns `UnexpectedEof` before
    /// issuing IO when the requested range exceeds the opened file length.
    pub async fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> ClientResult<()> {
        if dst.is_empty() {
            return Ok(());
        }
        let end = offset.checked_add(dst.len() as u64).ok_or_else(|| {
            ClientError::unexpected_eof(format!(
                "exact read at offset {offset} with length {} exceeds the opened file",
                dst.len()
            ))
        })?;
        if end > self.len() {
            return Err(ClientError::unexpected_eof(format!(
                "exact read range {offset}..{end} exceeds opened file length {}",
                self.len()
            )));
        }

        let deadline = self.inner.metadata.operation_deadline();
        let mut filled = 0usize;
        while filled < dst.len() {
            let step_len = (dst.len() - filled).min(self.inner.config.max_read_step_bytes() as usize);
            let step_offset = offset + filled as u64;
            let range = RequestedReadRange {
                file_offset: step_offset,
                len: step_len as u32,
            };
            self.read_range_step(range, &mut dst[filled..filled + step_len], deadline.clone())
                .await?;
            filled += step_len;
        }
        Ok(())
    }

    /// Reads from the current position to EOF within the configured owned-buffer bound.
    ///
    /// The bound is checked before allocation or IO. The position changes only
    /// when the complete remaining range succeeds.
    pub async fn read_to_end(&mut self) -> ClientResult<Bytes> {
        let remaining = self.len().saturating_sub(self.position);
        if remaining == 0 {
            return Ok(Bytes::new());
        }
        if remaining > self.inner.config.read_to_end_limit() {
            return Err(ClientError::invalid_argument(format!(
                "remaining file length {remaining} exceeds configured read_to_end maximum {}",
                self.inner.config.read_to_end_limit()
            )));
        }
        let capacity = usize::try_from(remaining)
            .map_err(|_| ClientError::invalid_argument("remaining file length exceeds addressable memory"))?;
        let mut output = Vec::new();
        output.try_reserve_exact(capacity).map_err(|error| {
            ClientError::resource_exhausted(format!(
                "failed to reserve {capacity} read_to_end buffer bytes: {error}"
            ))
        })?;
        output.resize(capacity, 0);

        let deadline = self.inner.metadata.operation_deadline();
        let mut staged_position = self.position;
        let mut current_block = self.current_block.take();
        let mut filled = 0usize;
        while filled < output.len() {
            let read = self
                .read_sequential_step(
                    staged_position,
                    &mut output[filled..],
                    &mut current_block,
                    deadline.clone(),
                )
                .await?;
            if read == 0 {
                return Err(ClientError::unexpected_eof(format!(
                    "read_to_end stopped at offset {staged_position} before opened file length {}",
                    self.len()
                )));
            }
            staged_position += read as u64;
            filled += read;
        }
        self.position = staged_position;
        self.current_block = current_block;
        Ok(Bytes::from(output))
    }

    /// Returns the EOF-truncated range for one bounded public read step.
    fn bounded_range(&self, offset: u64, output_len: usize) -> ClientResult<Option<RequestedReadRange>> {
        let step_len = output_len.min(self.inner.config.max_read_step_bytes() as usize);
        let step_len =
            u32::try_from(step_len).map_err(|_| ClientError::invalid_argument("bounded read length exceeds u32"))?;
        planner::requested_range(offset, step_len, self.len())
    }

    /// Executes one sequential step, stopping at the current block boundary.
    async fn read_sequential_step(
        &self,
        offset: u64,
        dst: &mut [u8],
        current_block: &mut Option<CurrentBlockPlan>,
        deadline: OperationDeadline,
    ) -> ClientResult<usize> {
        let Some(target) = self.bounded_range(offset, dst.len())? else {
            return Ok(0);
        };
        let operation = self.read_operation(deadline)?;

        for attempt_index in 0..self.inner.config.max_attempts() {
            if !current_block.as_ref().is_some_and(|plan| plan.contains(offset)) {
                let layout = self
                    .inner
                    .metadata
                    .read_layout_for_inode(operation.clone(), self.file.inode_id(), target.file_offset, target.len)
                    .await?;
                *current_block = Some(CurrentBlockPlan::new(&self.file, target, layout)?);
            }
            let plan = current_block.as_ref().expect("current block plan was initialized");
            let read_len = (u64::from(target.len).min(plan.end - offset)) as u32;
            let range = RequestedReadRange {
                file_offset: offset,
                len: read_len,
            };
            let (group_name, block_reads) = plan.plan(&self.file, range)?;
            let ctx = self.inner.data_context(&operation, attempt_index as u32);
            match self
                .inner
                .worker_rpc_with_timeout(
                    &operation,
                    self.inner.worker.read_block_ranges_into(
                        ctx,
                        group_name,
                        &block_reads,
                        &mut dst[..read_len as usize],
                    ),
                )
                .await
            {
                Ok(()) => {
                    if offset + u64::from(read_len) == plan.end {
                        *current_block = None;
                    }
                    return Ok(read_len as usize);
                }
                Err(error) => {
                    let decision = self.handle_worker_failure(&operation, attempt_index, &error).await?;
                    match decision {
                        RetryDecision::RefreshMetadata(_) => *current_block = None,
                        RetryDecision::Retry => {}
                        _ => return Err(error),
                    }
                }
            }
        }
        unreachable!("read attempt loop returns on its final attempt")
    }

    /// Executes one positioned range with one call identity and absolute deadline.
    async fn read_range_step(
        &self,
        range: RequestedReadRange,
        dst: &mut [u8],
        deadline: OperationDeadline,
    ) -> ClientResult<()> {
        let operation = self.read_operation(deadline)?;
        let mut layout = None;
        for attempt_index in 0..self.inner.config.max_attempts() {
            if layout.is_none() {
                layout = Some(
                    self.inner
                        .metadata
                        .read_layout_for_inode(operation.clone(), self.file.inode_id(), range.file_offset, range.len)
                        .await?,
                );
            }
            let (group_name, block_reads) = planner::plan_block_reads_from_layout(
                self.file.inode_id(),
                self.file.generation(),
                self.file.len(),
                range,
                layout.as_ref().expect("read layout was initialized"),
            )?;
            let ctx = self.inner.data_context(&operation, attempt_index as u32);
            match self
                .inner
                .worker_rpc_with_timeout(
                    &operation,
                    self.inner
                        .worker
                        .read_block_ranges_into(ctx, group_name, &block_reads, dst),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let decision = self.handle_worker_failure(&operation, attempt_index, &error).await?;
                    match decision {
                        RetryDecision::RefreshMetadata(_) => layout = None,
                        RetryDecision::Retry => {}
                        _ => return Err(error),
                    }
                }
            }
        }
        unreachable!("read attempt loop returns on its final attempt")
    }

    /// Applies bounded read retry policy and returns only an authorized next action.
    async fn handle_worker_failure(
        &self,
        operation: &OperationContext,
        attempt_index: usize,
        error: &ClientError,
    ) -> ClientResult<RetryDecision> {
        let decision = retry_decision(error, operation.retry_safety());
        self.inner.record_error_metric("Read", "worker", error);
        let has_next = attempt_index + 1 < self.inner.config.max_attempts();
        match (decision, has_next) {
            (RetryDecision::RefreshMetadata(reason), true) if should_replan_after_worker_error(error) => {
                self.inner
                    .metadata
                    .record_data_refresh(operation, reason, &refresh_hint_from_error(error))?;
                self.inner.record_metric(
                    ClientMetric::RetryAttempt,
                    metric_labels("Read", "worker").with_error_class(error.classification_label()),
                );
                Ok(decision)
            }
            (RetryDecision::Retry, true) => {
                self.inner.record_metric(
                    ClientMetric::RetryAttempt,
                    metric_labels("Read", "worker").with_error_class(error.classification_label()),
                );
                self.inner.sleep_before_retry(attempt_index, operation).await?;
                Ok(decision)
            }
            (RetryDecision::Retry | RetryDecision::RefreshMetadata(_), false) => {
                self.inner.record_metric(
                    ClientMetric::RetryExhausted,
                    metric_labels("Read", "worker").with_error_class(error.classification_label()),
                );
                Err(error.clone())
            }
            _ => Err(error.clone()),
        }
    }

    /// Creates one stable operation identity for a bounded Worker read step.
    fn read_operation(&self, deadline: OperationDeadline) -> ClientResult<OperationContext> {
        OperationContext::new_named(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            Operation::Read,
            Some(self.path().to_string()),
            deadline,
        )
    }
}

impl Debug for FileReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("FileReader")
            .field("path", &self.path())
            .field("len", &self.len())
            .field("position", &self.position())
            .finish()
    }
}

/// Metadata-authorized layout retained only for the current sequential block.
#[derive(Clone)]
struct CurrentBlockPlan {
    layout: ReadLayout,
    start: u64,
    end: u64,
}

impl CurrentBlockPlan {
    /// Validates a fresh layout and identifies the first block serving the cursor.
    fn new(file: &OpenedFile, range: RequestedReadRange, mut layout: ReadLayout) -> ClientResult<Self> {
        let (_, reads) =
            planner::plan_block_reads_from_layout(file.inode_id(), file.generation(), file.len(), range, &layout)?;
        let first = reads
            .first()
            .ok_or_else(|| ClientError::invalid_layout("read layout produced no block plan"))?;
        let start = first
            .file_offset
            .checked_sub(first.block_offset)
            .ok_or_else(|| ClientError::invalid_layout("planned block start underflow"))?;
        let location = layout
            .locations
            .iter()
            .find(|location| location.block_id == first.block_id && location.file_offset == start)
            .cloned()
            .ok_or_else(|| ClientError::invalid_layout("planned block is missing from its layout"))?;
        let end = start
            .checked_add(location.len)
            .ok_or_else(|| ClientError::invalid_layout("planned block end overflow"))?;
        layout.locations = vec![location];
        Ok(Self { layout, start, end })
    }

    /// Returns whether this plan authorizes the supplied sequential cursor.
    fn contains(&self, offset: u64) -> bool {
        self.start <= offset && offset < self.end
    }

    /// Re-plans a subrange while rechecking opened-file authority invariants.
    fn plan(&self, file: &OpenedFile, range: RequestedReadRange) -> ClientResult<(GroupName, Vec<PlannedBlockRead>)> {
        planner::plan_block_reads_from_layout(file.inode_id(), file.generation(), file.len(), range, &self.layout)
    }
}

/// Returns true when a structured Worker failure invalidates cached layout authority.
fn should_replan_after_worker_error(error: &ClientError) -> bool {
    error.remote_error().is_some_and(|detail| {
        matches!(
            detail.kind,
            ErrorKind::Metadata(MetadataErrorKind::StaleState | MetadataErrorKind::RouteEpochMismatch)
                | ErrorKind::Worker(
                    WorkerErrorKind::BlockLocationUnavailable
                        | WorkerErrorKind::RunMismatch
                        | WorkerErrorKind::FullReportRequired
                        | WorkerErrorKind::NotRegistered
                )
        )
    })
}
