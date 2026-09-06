// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared client ownership and cross-plane orchestration.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::config::ClientConfig;
use crate::error::side_effect_response_body_mismatch;
use crate::error::{ClientError, ClientErrorKind, ClientResult, RefreshHint};
use crate::metadata::{GrpcMetadataTransport, MetadataClient, MetadataTransport};
use crate::metrics::{self, ClientMetric, ClientMetricLabels};
use crate::runtime::{
    is_definite_worker_capacity_rejection, AttemptContext, ClientIdentity, MetadataTargets, Operation,
    OperationContext, OperationDeadline,
};
use crate::session::write_session::WriteSession;
use crate::worker::{BlockWrite, WorkerClient};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction, WorkerErrorKind};
use bytes::Bytes;

/// Shared owner for client configuration, Metadata orchestration, and Worker IO
/// used by the filesystem facade and open handles.
pub(crate) struct ClientInner {
    /// Immutable client configuration used by metadata and data-plane attempts.
    pub(crate) config: ClientConfig,
    /// Metadata client with bounded retry, refresh, and deadline handling.
    pub(crate) metadata: MetadataClient,
    /// Worker client used only after Metadata returns validated targets.
    pub(crate) worker: WorkerClient,
}

impl ClientInner {
    /// Builds the production owner and both concrete transports from validated
    /// client configuration.
    pub(crate) fn from_config(config: ClientConfig) -> ClientResult<Self> {
        config.validate()?;
        let metadata_targets = MetadataTargets::from_config(&config)?;
        let metadata_transport = Arc::new(GrpcMetadataTransport::new_lazy_with_config(&config)?);
        let worker = WorkerClient::from_config(&config);
        Self::from_parts(config, metadata_transport, metadata_targets, worker)
    }

    /// Establishes exactly one owner for identity, authority state, Worker
    /// orchestration, and immutable configuration.
    fn from_parts(
        config: ClientConfig,
        metadata_transport: Arc<dyn MetadataTransport>,
        metadata_targets: MetadataTargets,
        worker: WorkerClient,
    ) -> ClientResult<Self> {
        config.validate()?;
        let identity = ClientIdentity::generate(config.client_name().to_string())?;
        let metadata = MetadataClient::new(identity, metadata_transport, metadata_targets, &config)?;
        Ok(Self {
            config,
            metadata,
            worker,
        })
    }

    /// Reopens the partial tail or allocates a new block, then crosses the Worker acknowledgement boundary.
    /// Only an explicit capacity rejection before side effects permits a retry.
    pub(crate) async fn open_block_write(
        &self,
        session: &mut WriteSession,
        deadline: OperationDeadline,
    ) -> ClientResult<BlockWrite> {
        let (allocate_block_operation, allocate_block) = if let Some((group_name, block)) = session.reusable_tail() {
            (
                worker_write_context(
                    self.metadata.client_id(),
                    self.metadata.client_name(),
                    Operation::WriteBlock,
                    session.path(),
                    deadline.clone(),
                )?,
                crate::metadata::model::AllocateBlockResult { group_name, block },
            )
        } else {
            match self
                .metadata
                .allocate_block(
                    session.path(),
                    session.write_handle(),
                    session.previous_block_id(),
                    deadline.clone(),
                )
                .await
            {
                Ok(allocate_block) => allocate_block,
                Err(err) => {
                    mark_session_after_write_error(session, &err);
                    return Err(self.normalize_outcome_error("AllocateBlock", "metadata", err));
                }
            }
        };
        if let Err(err) = session.validate_target(&allocate_block.block) {
            session.mark_unknown_outcome();
            self.record_metric(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("AllocateBlock", "metadata").with_outcome("unknown"),
            );
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels("AllocateBlock", "metadata").with_outcome("unknown"),
            );
            return Err(side_effect_response_body_mismatch("AllocateBlock", err)
                .with_operation_context(&allocate_block_operation));
        }
        session.record_write_group(allocate_block.group_name.clone())?;
        let operation = worker_write_context(
            self.metadata.client_id(),
            self.metadata.client_name(),
            Operation::WriteBlock,
            session.path(),
            deadline,
        )?;
        let lease_expires_at_ms = session.expires_at_ms()?;
        for attempt_index in 0..self.config.max_attempts() {
            let ctx = self.data_context(&operation, attempt_index as u32);
            match self
                .worker_rpc_with_timeout(
                    &operation,
                    self.worker.open_write_block(
                        ctx,
                        allocate_block.group_name.clone(),
                        allocate_block.block.clone(),
                        lease_expires_at_ms,
                    ),
                )
                .await
            {
                Ok(block) => return Ok(block),
                Err(err) if is_definite_worker_capacity_rejection(&err) => {
                    let has_next = attempt_index + 1 < self.config.max_attempts();
                    if !has_next {
                        return Err(err.with_operation_context(&operation));
                    }
                    self.record_metric(
                        ClientMetric::RetryAttempt,
                        metric_labels("WriteBlock", "worker").with_error_class("server_retry"),
                    );
                    self.sleep_before_retry(attempt_index, &operation).await?;
                }
                Err(err) => {
                    mark_session_after_write_error(session, &err);
                    return Err(self.normalize_outcome_error("WriteBlock", "worker", err));
                }
            }
        }
        unreachable!("client retry configuration requires at least one attempt")
    }

    /// Sends one frame on an acknowledged block RPC under the current public
    /// write call's deadline, then advances the session cursor.
    pub(crate) async fn write_block_frame(
        &self,
        session: &mut WriteSession,
        block: &mut BlockWrite,
        data: Bytes,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        let len = data.len();
        session
            .cursor()
            .checked_add(len as u64)
            .ok_or_else(|| ClientError::invalid_argument("write cursor overflow".to_string()))?;
        match self.worker_write_step_with_timeout(deadline, block.write(data)).await {
            Ok(()) => session.advance_cursor(len),
            Err(err) => {
                mark_session_after_write_error(session, &err);
                Err(self.normalize_outcome_error("WriteBlock", "worker", err))
            }
        }
    }

    /// Half-closes one block request stream and records the block as Ready only
    /// after the Worker response stream ends normally.
    pub(crate) async fn finish_block_write(
        &self,
        session: &mut WriteSession,
        block: BlockWrite,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        match self.worker_write_step_with_timeout(deadline, block.finish()).await {
            Ok((target, written_len)) => {
                if let Err(err) = session.push_ready_block(target, written_len) {
                    session.mark_session_invalid();
                    return Err(err);
                }
                Ok(())
            }
            Err(err) => {
                mark_session_after_write_error(session, &err);
                Err(self.normalize_outcome_error("WriteBlock", "worker", err))
            }
        }
    }

    /// Observes a terminal Worker result that arrived between public writer
    /// calls before sending more bytes on the same block RPC.
    pub(crate) async fn check_block_write(
        &self,
        session: &mut WriteSession,
        block: &mut BlockWrite,
        deadline: &OperationDeadline,
    ) -> ClientResult<()> {
        match self.worker_write_step_with_timeout(deadline, block.check_open()).await {
            Ok(()) => Ok(()),
            Err(err) => {
                mark_session_after_write_error(session, &err);
                Err(self.normalize_outcome_error("WriteBlock", "worker", err))
            }
        }
    }

    /// Waits for local block cancellation only within the current public
    /// operation; the detached completion task retains its lease bound.
    pub(crate) async fn cancel_block_write(&self, block: BlockWrite, deadline: &OperationDeadline) -> ClientResult<()> {
        if block.cancel(deadline.remaining()).await {
            return Ok(());
        }
        self.record_worker_timeout("WriteBlock");
        Err(timeout_error("worker", "WriteBlock cancellation"))
    }

    /// Converts durable Worker blocks into the Metadata visibility-barrier shape.
    pub(crate) fn committed_blocks_for_barrier(&self, session: &WriteSession) -> Vec<beryl_types::CommittedBlock> {
        session.publication_blocks()
    }

    /// Builds a data-plane attempt context under the public operation deadline.
    pub(crate) fn data_context(&self, operation: &OperationContext, attempt: u32) -> AttemptContext {
        AttemptContext::for_data(operation, attempt)
    }

    /// Runs a worker RPC under the shared public operation deadline.
    pub(crate) async fn worker_rpc_with_timeout<T, Fut>(
        &self,
        operation: &OperationContext,
        future: Fut,
    ) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let timeout = operation.deadline().remaining();
        if timeout.is_zero() {
            self.record_worker_timeout(operation.operation_name());
            return Err(timeout_error("worker", operation.operation_name()).with_operation_context(operation));
        }
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result.map_err(|error| error.with_operation_context(operation)),
            Err(_) => {
                self.record_worker_timeout(operation.operation_name());
                Err(timeout_error("worker", operation.operation_name()).with_operation_context(operation))
            }
        }
    }

    /// Bounds one send, status check, or finish step without imposing a fixed
    /// timeout on the multi-call lifetime of the underlying streaming RPC.
    async fn worker_write_step_with_timeout<T, Fut>(&self, deadline: &OperationDeadline, future: Fut) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let timeout = deadline.remaining();
        if timeout.is_zero() {
            self.record_worker_timeout("WriteBlock");
            return Err(timeout_error("worker", "WriteBlock"));
        }
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_worker_timeout("WriteBlock");
                Err(timeout_error("worker", "WriteBlock"))
            }
        }
    }

    /// Sleeps before a worker retry without exceeding the public deadline.
    pub(crate) async fn sleep_before_retry(
        &self,
        retry_index: usize,
        operation: &OperationContext,
    ) -> ClientResult<()> {
        let delay = fixed_backoff_delay(retry_index);
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() || delay >= remaining {
            self.record_worker_timeout(operation.operation_name());
            return Err(timeout_error("worker", operation.operation_name()).with_operation_context(operation));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }

    /// Records metrics for client-recognized protocol and session failures.
    pub(crate) fn record_error_metric(&self, operation: &'static str, target_plane: &'static str, error: &ClientError) {
        let metric = if error.is_outcome_unknown() {
            Some(ClientMetric::UnknownOutcome)
        } else {
            match error.kind() {
                ClientErrorKind::InvalidResponse => Some(ClientMetric::InvalidHeader),
                ClientErrorKind::Fenced => Some(ClientMetric::FencingMismatch),
                ClientErrorKind::SessionInvalid => Some(ClientMetric::SessionInvalid),
                ClientErrorKind::SessionExpired => Some(ClientMetric::SessionExpired),
                ClientErrorKind::Unsupported => Some(ClientMetric::UnsupportedOperation),
                _ => None,
            }
        };
        if let Some(metric) = metric {
            self.record_metric(
                metric,
                metric_labels(operation, target_plane).with_error_class(error.classification_label()),
            );
        }
    }

    /// Maps transport or malformed-response uncertainty into an unknown-outcome client error.
    pub(crate) fn normalize_outcome_error(
        &self,
        operation: &'static str,
        target_plane: &'static str,
        err: ClientError,
    ) -> ClientError {
        if err.is_outcome_unknown() {
            return err;
        }
        self.record_error_metric(operation, target_plane, &err);
        let normalized = map_outcome_error(operation, err);
        if normalized.is_outcome_unknown() {
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels(operation, target_plane)
                    .with_error_class("unknown_outcome")
                    .with_outcome("unknown"),
            );
        }
        normalized
    }

    fn record_worker_timeout(&self, operation: &'static str) {
        self.record_metric(
            ClientMetric::RpcTimeout,
            metric_labels(operation, "worker")
                .with_error_class("retryable_transport")
                .with_outcome("timeout"),
        );
    }

    /// Emits one low-cardinality counter through the process-wide recorder.
    pub(crate) fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        metrics::record(metric, labels);
    }
}

/// Builds the standard metric label set for one client operation.
pub(crate) fn metric_labels(operation: &'static str, target_plane: &'static str) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(operation, target_plane)
}

/// Extracts a structured refresh hint from action errors when one is available.
pub(crate) fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    err.refresh_hint().cloned().unwrap_or_default()
}

/// Returns true when a metadata session barrier has an unknown result.
pub(crate) fn is_unknown_session_barrier_outcome(err: &ClientError) -> bool {
    err.is_outcome_unknown()
}

/// Marks a write session after a metadata session-level failure.
pub(crate) fn mark_session_after_metadata_error(session: &mut WriteSession, err: &ClientError) {
    if err.is_outcome_unknown() {
        session.mark_unknown_outcome();
        return;
    }
    match err.kind() {
        ClientErrorKind::SessionExpired => session.mark_session_expired(),
        ClientErrorKind::Fenced | ClientErrorKind::SessionInvalid => session.mark_session_invalid(),
        _ => {}
    }
    if matches!(
        err.remote_error().map(|error| &error.recovery),
        Some(RecoveryAction::RefreshMetadata { .. })
    ) {
        session.mark_session_invalid();
    }
}

/// Converts a worker timeout into the standard transport-style client error.
fn timeout_error(target_plane: &str, operation: &str) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} exceeded the public operation deadline"
    )))
}

/// Creates the stable operation identity used for worker write attempts.
fn worker_write_context(
    client_id: beryl_types::ClientId,
    client_name: &str,
    operation: Operation,
    path: &str,
    deadline: OperationDeadline,
) -> ClientResult<OperationContext> {
    OperationContext::new_named(client_id, client_name, operation, Some(path.to_string()), deadline)
}

/// Marks a write session after a worker write or add-block failure.
fn mark_session_after_write_error(session: &mut WriteSession, err: &ClientError) {
    if has_uncertain_write_effect(err) {
        session.mark_unknown_outcome();
    } else if is_session_or_fencing_error(err) || is_write_refresh_error(err) {
        mark_session_after_metadata_error(session, err);
    } else {
        session.mark_session_invalid();
    }
}

/// Returns true when a failure leaves worker write side effects uncertain.
fn has_uncertain_write_effect(err: &ClientError) -> bool {
    err.is_outcome_unknown() || err.is_retryable_transport() || err.is_invalid_success_response()
}

/// Returns true when the error invalidates or expires the write session.
fn is_session_or_fencing_error(err: &ClientError) -> bool {
    matches!(
        err.kind(),
        ClientErrorKind::Fenced | ClientErrorKind::SessionInvalid | ClientErrorKind::SessionExpired
    )
}

/// Returns true when a write-path metadata refresh cause invalidates the current session.
fn is_write_refresh_error(err: &ClientError) -> bool {
    err.remote_error().is_some_and(|error| {
        matches!(error.recovery, RecoveryAction::RefreshMetadata { .. })
            && matches!(
                error.kind,
                ErrorKind::Metadata(
                    MetadataErrorKind::RouteEpochMismatch
                        | MetadataErrorKind::OwnerGroupMismatch
                        | MetadataErrorKind::StaleState
                ) | ErrorKind::Worker(WorkerErrorKind::RunMismatch)
            )
    })
}

/// Normalizes uncertain transport and header failures into unknown outcomes.
fn map_outcome_error(operation: &'static str, err: ClientError) -> ClientError {
    if err.is_retryable_transport() {
        let message = format!("{operation} outcome is unknown after transport failure: {err}");
        return err.with_unknown_outcome_name(operation, message);
    }
    if err.is_invalid_success_response() {
        let message = format!("{operation} outcome is unknown after malformed OK response: {err}");
        return err.with_unknown_outcome_name(operation, message);
    }
    err
}

fn fixed_backoff_delay(retry_index: usize) -> Duration {
    const INITIAL_MS: u64 = 100;
    const MAX_MS: u64 = 2_000;
    let shift = retry_index.min(20) as u32;
    Duration::from_millis(INITIAL_MS.saturating_mul(1u64 << shift).min(MAX_MS))
}
