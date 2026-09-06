// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker transport implementation and block-local RPC lifecycle.

use super::channel_pool::GrpcWorkerChannelPool;
use super::protocol::{
    build_read_block_request, build_tonic_request, build_write_block_command, has_structured_worker_error,
    is_transient_worker_transport_status, parse_worker_data_status, read_block_stream_into,
};
use super::{
    duration_until_unix_ms, write_lease_expired_error, BlockWrite, BlockWriteInput, BlockWriteLease, WorkerTransport,
    WorkerWriteTarget,
};
use crate::cache::CacheInvalidationReason;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::planner::{block_location_unavailable_error, PlannedBlockRead};
use crate::runtime::{is_definite_worker_capacity_rejection, AttemptContext};
use async_trait::async_trait;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction, WorkerErrorKind};
use beryl_proto::worker::{WriteBlockRequestProto, WriteBlockResponseProto};
use beryl_types::{GroupName, WorkerEndpointInfo};
use futures::{stream, Stream};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tonic::{Request, Status, Streaming};

/// Executes one block-local operation against Metadata-authorized Worker
/// candidates and owns channel health plus wire validation.
#[derive(Debug)]
pub(super) struct GrpcWorkerTransport {
    channel_pool: Arc<GrpcWorkerChannelPool>,
}

/// Cancels a request whose open future exits before the block-open acknowledgement can
/// transfer request-stream ownership to `BlockWrite`.
struct OpeningWriteCancellation {
    signal: Option<Sender<bool>>,
}

impl OpeningWriteCancellation {
    /// Transfers cancellation ownership to the acknowledged block RPC.
    fn disarm(&mut self) -> Sender<bool> {
        self.signal.take().expect("opening cancellation signal is present")
    }
}

impl Drop for OpeningWriteCancellation {
    fn drop(&mut self) {
        if let Some(signal) = self.signal.take() {
            signal.send_replace(true);
        }
    }
}

impl GrpcWorkerTransport {
    /// Builds the production Worker transport and its bounded channel pool.
    pub(super) fn from_config(config: &ClientConfig) -> Self {
        Self {
            channel_pool: Arc::new(GrpcWorkerChannelPool::from_config(config)),
        }
    }

    /// Orders candidates by observed channel health without inventing targets
    /// beyond Metadata's authorized list.
    fn worker_candidates<'a>(&self, workers: &'a [WorkerEndpointInfo]) -> Vec<&'a WorkerEndpointInfo> {
        let mut active = Vec::with_capacity(workers.len());
        let mut cooling = Vec::new();
        for worker in workers {
            if self.channel_pool.is_worker_cooling_down(worker) {
                cooling.push(worker);
            } else {
                active.push(worker);
            }
        }
        if !active.is_empty() {
            return active;
        }
        for worker in &cooling {
            self.channel_pool.clear_worker_cooldown(worker);
        }
        cooling
    }

    /// Maps a terminal write status. Structured Worker rejection remains
    /// actionable; unstructured transport loss after request initiation is an
    /// unknown outcome and must never be replayed on another worker.
    fn map_write_status(
        channel_pool: &GrpcWorkerChannelPool,
        attempt: &AttemptContext,
        worker: &WorkerEndpointInfo,
        status: Status,
    ) -> ClientError {
        let transport_error = ClientError::from(status.clone());
        if is_definite_worker_capacity_rejection(&transport_error) {
            return transport_error;
        }
        if has_structured_worker_error(&status) {
            let error = parse_worker_data_status(attempt, status);
            channel_pool.invalidate_on_worker_run_mismatch(worker, &error);
            return error;
        }
        if is_transient_worker_transport_status(&status) {
            channel_pool.mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
        }
        let message = format!(
            "worker WriteBlock outcome is unknown after transport status {}: {}",
            status.code(),
            status.message()
        );
        transport_error.with_unknown_outcome(attempt.operation_context(), message)
    }

    /// Opens one block RPC and returns its concrete client-side owner after the
    /// Worker acknowledges block-open checkpoint.
    ///
    /// The RPC intentionally has no fixed tonic timeout after the acknowledgement:
    /// later `write_all`, sync, and close calls apply their own local deadlines,
    /// while the completion task enforces the renewable write-lease expiry.
    async fn open_one_block(
        &self,
        attempt: &AttemptContext,
        target: &WorkerWriteTarget,
        worker: &WorkerEndpointInfo,
        lease_expires_at_ms: u64,
    ) -> ClientResult<BlockWrite> {
        let mut client = self.channel_pool.worker_data_service_client(worker, "WriteBlock")?;
        let command = build_write_block_command(attempt, target, worker)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let (cancellation, cancellation_signal) = watch::channel(false);
        let mut opening_cancellation = OpeningWriteCancellation {
            signal: Some(cancellation),
        };
        let requests = write_block_requests(command, receiver, cancellation_signal);
        let response = tokio::select! {
            biased;
            _ = tokio::time::sleep(duration_until_unix_ms(lease_expires_at_ms)) => {
                return Err(write_lease_expired_error().with_operation_context(attempt.operation_context()));
            }
            response = client.write_block(Request::new(requests)) => response,
        };
        let mut responses = response
            .map_err(|status| Self::map_write_status(self.channel_pool.as_ref(), attempt, worker, status))?
            .into_inner();

        let acknowledgement = tokio::select! {
            biased;
            _ = tokio::time::sleep(duration_until_unix_ms(lease_expires_at_ms)) => {
                return Err(write_lease_expired_error().with_operation_context(attempt.operation_context()));
            }
            acknowledgement = responses.message() => acknowledgement,
        };
        match acknowledgement {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(ClientError::unknown_outcome(
                    "worker WriteBlock ended before block-open acknowledgement".to_string(),
                )
                .with_operation_context(attempt.operation_context()));
            }
            Err(status) => {
                return Err(Self::map_write_status(
                    self.channel_pool.as_ref(),
                    attempt,
                    worker,
                    status,
                ));
            }
        }

        let lease = Arc::new(BlockWriteLease::new(lease_expires_at_ms));
        let cancellation = opening_cancellation.disarm();
        let channel_pool = Arc::clone(&self.channel_pool);
        let attempt = attempt.clone();
        let operation = attempt.operation_context().clone();
        let worker = worker.clone();
        let completion = tokio::spawn(wait_for_write_completion(
            channel_pool,
            attempt,
            worker,
            responses,
            Arc::clone(&lease),
            cancellation.clone(),
        ));
        Ok(BlockWrite::new(
            operation,
            target.target.clone(),
            sender,
            cancellation,
            lease,
            completion,
        ))
    }
}

/// Physical request-stream state for exactly one Worker block RPC.
///
/// The command is emitted once, `Finish` is the only normal EOF, and every
/// abandoned or expired write emits at most one failure-cleanup frame.
struct BlockWriteRequestState {
    command: Option<WriteBlockRequestProto>,
    inputs: Receiver<BlockWriteInput>,
    cancellation: Option<watch::Receiver<bool>>,
    cancellation_sent: bool,
}

/// Emits physical command and data frames. Only explicit `Finish` becomes EOF;
/// cancellation uses Worker's existing invalid-payload cleanup path.
fn write_block_requests(
    command: WriteBlockRequestProto,
    receiver: Receiver<BlockWriteInput>,
    cancellation: watch::Receiver<bool>,
) -> impl Stream<Item = WriteBlockRequestProto> {
    let state = BlockWriteRequestState {
        command: Some(command),
        inputs: receiver,
        cancellation: Some(cancellation),
        cancellation_sent: false,
    };
    stream::unfold(state, next_write_block_request)
}

/// Emits the next command, data, cancellation, or normal-finish event while
/// preserving the one-terminal-event request-stream invariant.
async fn next_write_block_request(
    mut state: BlockWriteRequestState,
) -> Option<(WriteBlockRequestProto, BlockWriteRequestState)> {
    if let Some(command) = state.command.take() {
        return Some((command, state));
    }
    loop {
        if state
            .cancellation
            .as_mut()
            .is_some_and(|cancellation| *cancellation.borrow_and_update())
        {
            if state.cancellation_sent {
                return std::future::pending().await;
            }
            state.cancellation_sent = true;
            return Some((write_block_failure_cleanup_request(), state));
        }
        let Some(cancellation) = state.cancellation.as_mut() else {
            return match state.inputs.recv().await {
                Some(BlockWriteInput::Data(request)) => Some((request, state)),
                Some(BlockWriteInput::Finish) => None,
                None => std::future::pending().await,
            };
        };
        tokio::select! {
            biased;
            changed = cancellation.changed() => {
                if changed.is_err() {
                    state.cancellation = None;
                }
            }
            input = state.inputs.recv() => {
                return match input {
                    Some(BlockWriteInput::Data(request)) => Some((request, state)),
                    Some(BlockWriteInput::Finish) => None,
                    None => std::future::pending().await,
                };
            }
        }
    }
}

/// Builds a terminal missing-payload request. Worker already rejects this
/// shape through its owned failure path, which discards bytes beyond the last durable checkpoint.
fn write_block_failure_cleanup_request() -> WriteBlockRequestProto {
    WriteBlockRequestProto { payload: None }
}

/// Waits for the sole terminal response condition after the block-open acknowledgement.
/// Normal EOF means Worker Ready; every other outcome leaves publication
/// unproven and is returned to the owning `FileWriter`.
async fn wait_for_write_completion(
    channel_pool: Arc<GrpcWorkerChannelPool>,
    attempt: AttemptContext,
    worker: WorkerEndpointInfo,
    mut responses: Streaming<WriteBlockResponseProto>,
    lease: Arc<BlockWriteLease>,
    cancellation: Sender<bool>,
) -> ClientResult<()> {
    let mut lease_updates = lease.subscribe();
    loop {
        if lease.expire_if_due() {
            cancellation.send_replace(true);
            return Err(ClientError::unknown_outcome(
                "worker WriteBlock was cancelled after the write lease expired".to_string(),
            )
            .with_operation_context(attempt.operation_context()));
        }
        let expires_at_ms = lease.expires_at_ms();
        let lease_expiry = tokio::time::sleep(duration_until_unix_ms(expires_at_ms));
        tokio::pin!(lease_expiry);
        tokio::select! {
            biased;
            changed = lease_updates.changed() => {
                if changed.is_err() {
                    return Err(ClientError::unknown_outcome(
                        "worker WriteBlock lost its lease owner before completion".to_string(),
                    )
                    .with_operation_context(attempt.operation_context()));
                }
            }
            _ = &mut lease_expiry => {
                if !lease.expire_if_due() {
                    continue;
                }
                cancellation.send_replace(true);
                return Err(ClientError::unknown_outcome(
                    "worker WriteBlock was cancelled after the write lease expired".to_string(),
                )
                .with_operation_context(attempt.operation_context()));
            }
            response = responses.message() => {
                return match response {
                    Ok(None) => Ok(()),
                    Ok(Some(_)) => Err(ClientError::unknown_outcome(
                        "worker WriteBlock returned more than one acknowledgement".to_string(),
                    )
                    .with_operation_context(attempt.operation_context())),
                    Err(status) => Err(GrpcWorkerTransport::map_write_status(
                        channel_pool.as_ref(),
                        &attempt,
                        &worker,
                        status,
                    )),
                };
            }
        }
    }
}

fn is_stale_read_location_error(error: &ClientError) -> bool {
    error.remote_error().is_some_and(|rpc_error| {
        matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. })
            && matches!(
                rpc_error.kind,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable)
                    | ErrorKind::Worker(WorkerErrorKind::RunMismatch)
                    | ErrorKind::Metadata(MetadataErrorKind::StaleState)
                    | ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch)
                    | ErrorKind::Worker(WorkerErrorKind::FullReportRequired)
                    | ErrorKind::Worker(WorkerErrorKind::NotRegistered)
            )
    })
}

#[async_trait]
impl WorkerTransport for GrpcWorkerTransport {
    async fn read_block_range(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_read: &PlannedBlockRead,
        output: &mut [u8],
    ) -> ClientResult<()> {
        if block_read.workers.is_empty() {
            return Err(block_location_unavailable_error(format!(
                "block location unavailable: no worker candidates for block {} file_offset={} len={}",
                block_read.block_id, block_read.file_offset, block_read.len
            )));
        }
        let mut last_transport_error = None;
        let mut last_location_error = None;
        for worker in self.worker_candidates(&block_read.workers) {
            let mut client = self.channel_pool.worker_data_service_client(worker, "ReadBlock")?;
            let request = build_read_block_request(&attempt, &group_name, block_read, worker)?;
            let mut responses = match client.read_block(build_tonic_request(&attempt, request)).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let error = parse_worker_data_status(&attempt, status);
                    self.channel_pool.invalidate_on_worker_run_mismatch(worker, &error);
                    if is_stale_read_location_error(&error) {
                        last_location_error = Some(error);
                        continue;
                    }
                    if !error.is_retryable_transport() {
                        return Err(error);
                    }
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(error);
                    continue;
                }
            };
            match read_block_stream_into(&attempt, &mut responses, block_read, output).await {
                Ok(()) => {}
                Err(error) if is_stale_read_location_error(&error) => {
                    self.channel_pool.invalidate_on_worker_run_mismatch(worker, &error);
                    last_location_error = Some(error);
                    continue;
                }
                Err(error) if error.is_retryable_transport() => {
                    self.channel_pool
                        .mark_worker_unavailable(worker, CacheInvalidationReason::Unavailable);
                    last_transport_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            }
            return Ok(());
        }
        if let Some(error) = last_transport_error {
            return Err(error);
        }
        Err(last_location_error.unwrap_or_else(|| {
            block_location_unavailable_error(format!(
                "block location unavailable: no reachable worker candidates for block {} file_offset={} len={}",
                block_read.block_id, block_read.file_offset, block_read.len
            ))
        }))
    }

    async fn open_write_block(
        &self,
        attempt: AttemptContext,
        target: WorkerWriteTarget,
        lease_expires_at_ms: u64,
    ) -> ClientResult<BlockWrite> {
        let worker = self
            .worker_candidates(&target.target.worker_endpoints)
            .into_iter()
            .next()
            .ok_or_else(|| ClientError::worker("worker write has no candidates".to_string()))?;
        self.open_one_block(&attempt, &target, worker, lease_expires_at_ms)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientErrorKind;
    use crate::runtime::{retry_decision, Operation, OperationContext, OperationDeadline, RetryDecision, RetrySafety};
    use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind};
    use beryl_common::header::{
        HEADER_WORKER_DATA_ERROR_DETAIL, HEADER_WORKER_DATA_REJECTION, WORKER_DATA_ERROR_DETAIL_V1,
        WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT,
    };
    use beryl_proto::convert::rpc_error_to_proto;
    use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
    use beryl_proto::worker::write_block_request_proto::Payload;
    use beryl_proto::worker::{
        DataRequestHeaderProto, DataResponseHeaderProto, ReadBlockChunkProto, ReadBlockRequestProto,
        WriteBlockRequestProto, WriteBlockResponseProto,
    };
    use beryl_types::lease::FencingToken;
    use beryl_types::{
        BlockFormatId, BlockId, BlockIndex, ClientId, InodeId, LeaseEpoch, LocatedBlock, Tier, WorkerEndpointInfo,
        WorkerId, WorkerNetProtocol, WorkerRunId,
    };
    use bytes::Bytes;
    use prost::Message;
    use std::io::Error;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::metadata::{MetadataMap, MetadataValue};
    use tonic::transport::Server;
    use tonic::{Code, Request, Response, Status};

    #[derive(Clone, Copy)]
    enum ReadFailure {
        None,
        Transport,
        StructuredLocation,
        PartialTransport,
        EmptyChunk,
        ShortRead,
        OversizedRead,
    }

    #[derive(Clone, Copy)]
    enum WriteBehavior {
        Success,
        CapacityRejected,
        AckThenUnavailable,
        DelayedAck,
    }

    struct MockWorkerState {
        read_calls: AtomicUsize,
        write_calls: AtomicUsize,
        write_data_frames: AtomicUsize,
        write_eofs: AtomicUsize,
        write_cancellations: AtomicUsize,
        read_failure: ReadFailure,
        write_behavior: WriteBehavior,
    }

    impl MockWorkerState {
        fn new(read_failure: ReadFailure, write_behavior: WriteBehavior) -> Self {
            Self {
                read_calls: AtomicUsize::new(0),
                write_calls: AtomicUsize::new(0),
                write_data_frames: AtomicUsize::new(0),
                write_eofs: AtomicUsize::new(0),
                write_cancellations: AtomicUsize::new(0),
                read_failure,
                write_behavior,
            }
        }
    }

    #[derive(Clone)]
    struct MockWorkerService {
        state: Arc<MockWorkerState>,
    }

    #[tonic::async_trait]
    impl WorkerDataService for MockWorkerService {
        type ReadBlockStream = Pin<Box<dyn Stream<Item = Result<ReadBlockChunkProto, Status>> + Send>>;
        async fn read_block(
            &self,
            request: Request<ReadBlockRequestProto>,
        ) -> Result<Response<Self::ReadBlockStream>, Status> {
            self.state.read_calls.fetch_add(1, Ordering::SeqCst);
            let request = request.into_inner();
            match self.state.read_failure {
                ReadFailure::Transport => Err(Status::unavailable("read transport unavailable")),
                ReadFailure::StructuredLocation => Err(structured_location_status(request.header.as_ref())),
                ReadFailure::PartialTransport => Ok(Response::new(Box::pin(futures::stream::iter(vec![
                    Ok(ReadBlockChunkProto {
                        data: Bytes::from_static(b"xx"),
                    }),
                    Err(Status::unavailable("partial read transport unavailable")),
                ])))),
                ReadFailure::EmptyChunk => Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    ReadBlockChunkProto { data: Bytes::new() },
                )])))),
                ReadFailure::ShortRead => Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    ReadBlockChunkProto {
                        data: Bytes::from_static(b"da"),
                    },
                )])))),
                ReadFailure::OversizedRead => Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    ReadBlockChunkProto {
                        data: Bytes::from_static(b"datax"),
                    },
                )])))),
                ReadFailure::None => Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
                    ReadBlockChunkProto {
                        data: Bytes::from_static(b"data"),
                    },
                )])))),
            }
        }

        type WriteBlockStream = Pin<Box<dyn Stream<Item = Result<WriteBlockResponseProto, Status>> + Send>>;

        async fn write_block(
            &self,
            request: Request<Streaming<WriteBlockRequestProto>>,
        ) -> Result<Response<Self::WriteBlockStream>, Status> {
            self.state.write_calls.fetch_add(1, Ordering::SeqCst);
            if matches!(self.state.write_behavior, WriteBehavior::CapacityRejected) {
                let mut metadata = MetadataMap::new();
                metadata.insert(
                    HEADER_WORKER_DATA_REJECTION,
                    MetadataValue::from_static(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT),
                );
                return Err(Status::with_metadata(
                    Code::ResourceExhausted,
                    "mock Worker write capacity exhausted",
                    metadata,
                ));
            }
            let mut requests = request.into_inner();
            let first = requests
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("missing command"))?;
            if !matches!(first.payload, Some(Payload::Command(_))) {
                return Err(Status::invalid_argument("first payload must be command"));
            }
            let (responses, response_stream) = tokio::sync::mpsc::channel(2);
            if matches!(self.state.write_behavior, WriteBehavior::DelayedAck) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            responses
                .send(Ok(WriteBlockResponseProto {}))
                .await
                .expect("response receiver is open");
            match self.state.write_behavior {
                WriteBehavior::Success | WriteBehavior::DelayedAck => {
                    let state = Arc::clone(&self.state);
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                biased;
                                _ = responses.closed() => {
                                    state.write_cancellations.fetch_add(1, Ordering::SeqCst);
                                    break;
                                }
                                request = requests.message() => match request {
                                    Ok(Some(request)) => match request.payload {
                                        Some(Payload::Data(_)) => {
                                            state.write_data_frames.fetch_add(1, Ordering::SeqCst);
                                        }
                                        _ => {
                                            state.write_cancellations.fetch_add(1, Ordering::SeqCst);
                                            break;
                                        }
                                    },
                                    Ok(None) => {
                                        state.write_eofs.fetch_add(1, Ordering::SeqCst);
                                        break;
                                    }
                                    Err(_) => {
                                        state.write_cancellations.fetch_add(1, Ordering::SeqCst);
                                        break;
                                    }
                                }
                            }
                        }
                        drop(responses);
                    });
                }
                WriteBehavior::AckThenUnavailable => {
                    responses
                        .send(Err(Status::unavailable("write transport unavailable after ack")))
                        .await
                        .expect("response receiver is open");
                }
                WriteBehavior::CapacityRejected => unreachable!("capacity rejection returned before stream setup"),
            }
            Ok(Response::new(Box::pin(ReceiverStream::new(response_stream))))
        }
    }

    async fn start_mock_worker(
        state: Arc<MockWorkerState>,
        worker_id: u64,
    ) -> (WorkerEndpointInfo, tokio::sync::oneshot::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock Worker");
        let address = listener.local_addr().expect("mock Worker address");
        let incoming = futures::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok::<_, Error>(Some((stream, listener)))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerDataServiceServer::new(MockWorkerService { state }))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("mock Worker server");
        });
        (worker_endpoint(&address.to_string(), worker_id), shutdown_tx)
    }

    fn grpc_client() -> GrpcWorkerTransport {
        let config = ClientConfig::builder()
            .worker_connection_reuse(true)
            .worker_connection_limit(8)
            .build()
            .expect("config");
        GrpcWorkerTransport::from_config(&config)
    }

    fn worker_endpoint(endpoint: &str, worker_id: u64) -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(worker_id),
            worker_run_id: WorkerRunId::new(),
            endpoint: endpoint.to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
        }
    }

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(202), BlockIndex::new(0))
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("group name")
    }

    fn attempt(operation: Operation) -> AttemptContext {
        let operation = OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            operation,
            Some("/alpha".to_string()),
            OperationDeadline::new(5_000),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn lease_expiry_after(delay_ms: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_millis() as u64
            + delay_ms
    }

    fn lease_expiry() -> u64 {
        lease_expiry_after(60_000)
    }

    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("mock Worker observed expected stream event");
    }

    fn planned_read(workers: Vec<WorkerEndpointInfo>) -> PlannedBlockRead {
        PlannedBlockRead {
            file_offset: 0,
            len: 4,
            end_file_offset: 4,
            block_id: block_id(),
            block_offset: 0,

            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
            effective_len: 4,
            workers,
        }
    }

    fn write_target(workers: Vec<WorkerEndpointInfo>) -> WorkerWriteTarget {
        let block_id = block_id();
        WorkerWriteTarget {
            group_name: group_name(),
            target: LocatedBlock {
                write_offset: 0,
                block_id,
                file_offset: 0,
                block_size: 4096,
                worker_endpoints: workers,
                fencing_token: FencingToken::new(block_id, ClientId::new(7), LeaseEpoch::new(1)),

                chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
                block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
                tier: Tier::Mem,
            },
        }
    }

    fn structured_location_status(header: Option<&DataRequestHeaderProto>) -> Status {
        let error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
            RefreshHint::default(),
            "local block is unavailable",
        );
        let response = DataResponseHeaderProto {
            client: header.and_then(|header| header.client.clone()),
            error: Some(rpc_error_to_proto(&error)),
        };
        let mut metadata = MetadataMap::new();
        metadata.insert(
            HEADER_WORKER_DATA_ERROR_DETAIL,
            WORKER_DATA_ERROR_DETAIL_V1.parse().expect("error detail version"),
        );
        Status::with_details_and_metadata(
            Code::FailedPrecondition,
            error.message,
            Bytes::from(response.encode_to_vec()),
            metadata,
        )
    }

    #[tokio::test]
    async fn read_failover_preserves_single_replica_results_and_fails_closed_on_corruption() {
        let cases = [
            (ReadFailure::Transport, true),
            (ReadFailure::StructuredLocation, true),
            (ReadFailure::PartialTransport, true),
            (ReadFailure::EmptyChunk, false),
            (ReadFailure::ShortRead, false),
            (ReadFailure::OversizedRead, false),
        ];
        for (failure, may_fail_over) in cases {
            let first_state = Arc::new(MockWorkerState::new(failure, WriteBehavior::Success));
            let second_state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
            let (first, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
            let (second, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;

            let mut output = [0u8; 4];
            let result = grpc_client()
                .read_block_range(
                    attempt(Operation::Read),
                    group_name(),
                    &planned_read(vec![first, second]),
                    &mut output,
                )
                .await;
            if may_fail_over {
                result.expect("second Worker satisfies read");
                assert_eq!(output, *b"data");
            } else {
                let error = result.expect_err("invalid exact range must fail closed");
                assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
            }
            assert_eq!(first_state.read_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                second_state.read_calls.load(Ordering::SeqCst),
                usize::from(may_fail_over)
            );
            let _ = first_shutdown.send(());
            let _ = second_shutdown.send(());
        }
    }

    #[tokio::test]
    async fn write_failure_after_ack_is_unknown_and_never_tries_another_worker() {
        let first_state = Arc::new(MockWorkerState::new(
            ReadFailure::None,
            WriteBehavior::AckThenUnavailable,
        ));
        let second_state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
        let (first, first_shutdown) = start_mock_worker(Arc::clone(&first_state), 1).await;
        let (second, second_shutdown) = start_mock_worker(Arc::clone(&second_state), 2).await;

        let mut block = grpc_client()
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![first, second]),
                lease_expiry(),
            )
            .await
            .expect("block-open acknowledgement");
        let error = match block.write(Bytes::from_static(b"data")).await {
            Ok(()) => block
                .finish()
                .await
                .expect_err("transport loss after ack has unknown outcome"),
            Err(error) => error,
        };

        assert!(error.is_outcome_unknown());
        assert!(error.message().contains("WriteBlock"));
        assert_eq!(first_state.write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_state.write_calls.load(Ordering::SeqCst), 0);
        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
    }

    #[tokio::test]
    async fn marked_before_side_effect_write_capacity_survives_the_grpc_boundary() {
        let state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::CapacityRejected));
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;

        let error = match grpc_client()
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker]),
                lease_expiry(),
            )
            .await
        {
            Ok(_) => panic!("capacity rejection before Worker side effects must precede acknowledgement"),
            Err(error) => error,
        };

        assert_eq!(
            retry_decision(&error, RetrySafety::NonReplayableMutation),
            RetryDecision::Retry
        );
        assert_eq!(state.write_calls.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn write_ack_cannot_cross_lease_expiry() {
        let state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::DelayedAck));
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;

        let error = match grpc_client()
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker]),
                lease_expiry_after(10),
            )
            .await
        {
            Ok(_) => panic!("delayed acknowledgement cannot outlive the lease"),
            Err(error) => error,
        };

        assert!(error.is_outcome_unknown());
        assert!(error.message().contains("lease expired"));
        tokio::time::sleep(Duration::from_millis(75)).await;
        wait_for_count(&state.write_cancellations, 1).await;
        assert_eq!(state.write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.write_data_frames.load(Ordering::SeqCst), 0);
        assert_eq!(state.write_eofs.load(Ordering::SeqCst), 0);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn lease_renewal_ordering_extends_live_rpc_without_reviving_expired_rpc() {
        let state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;
        let client = grpc_client();
        let mut block = client
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker.clone()]),
                lease_expiry_after(200),
            )
            .await
            .expect("block-open acknowledgement");

        block.write(Bytes::from_static(b"a")).await.expect("first frame");
        block
            .update_lease_expiry(lease_expiry_after(1_000))
            .expect("renew before old expiry");
        // Keep the current-thread runtime from polling the completion task until
        // both the old timer and the queued renewal are ready.
        std::thread::sleep(Duration::from_millis(250));
        block
            .write(Bytes::from_static(b"b"))
            .await
            .expect("frame after renewal");
        block.finish().await.expect("finish renewed block");
        wait_for_count(&state.write_eofs, 1).await;
        assert_eq!(state.write_data_frames.load(Ordering::SeqCst), 2);

        let mut expired = client
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker]),
                lease_expiry_after(30),
            )
            .await
            .expect("open block before its lease expires");
        expired.write(Bytes::from_static(b"c")).await.expect("partial frame");
        std::thread::sleep(Duration::from_millis(50));
        let error = expired
            .update_lease_expiry(lease_expiry_after(1_000))
            .expect_err("renewal observed after old expiry cannot revive the RPC");
        assert!(error.is_outcome_unknown());
        assert!(error.message().contains("lease expired"));
        assert!(expired.cancel(Duration::from_secs(1)).await);
        wait_for_count(&state.write_cancellations, 1).await;

        assert_eq!(state.write_calls.load(Ordering::SeqCst), 2);
        assert_eq!(state.write_eofs.load(Ordering::SeqCst), 1);
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn cancel_drop_and_lease_expiry_never_look_like_normal_finish() {
        let state = Arc::new(MockWorkerState::new(ReadFailure::None, WriteBehavior::Success));
        let (worker, shutdown) = start_mock_worker(Arc::clone(&state), 1).await;
        let client = grpc_client();

        let mut cancelled = client
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker.clone()]),
                lease_expiry(),
            )
            .await
            .expect("open explicitly cancelled block");
        cancelled.write(Bytes::from_static(b"a")).await.expect("partial frame");
        assert!(cancelled.cancel(Duration::from_secs(1)).await);
        wait_for_count(&state.write_cancellations, 1).await;
        assert_eq!(state.write_eofs.load(Ordering::SeqCst), 0);

        let mut dropped = client
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker.clone()]),
                lease_expiry(),
            )
            .await
            .expect("open dropped block");
        dropped.write(Bytes::from_static(b"b")).await.expect("partial frame");
        drop(dropped);
        wait_for_count(&state.write_cancellations, 2).await;
        assert_eq!(state.write_eofs.load(Ordering::SeqCst), 0);

        let mut expired = client
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker.clone()]),
                lease_expiry_after(30),
            )
            .await
            .expect("open lease-expired block");
        expired.write(Bytes::from_static(b"c")).await.expect("partial frame");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let error = expired
            .finish()
            .await
            .expect_err("expired block cannot finish normally");
        assert!(error.is_outcome_unknown());
        assert!(error.message().contains("lease expired"));
        wait_for_count(&state.write_cancellations, 3).await;
        assert_eq!(state.write_eofs.load(Ordering::SeqCst), 0);

        let mut finished = client
            .open_write_block(
                attempt(Operation::WriteBlock),
                write_target(vec![worker]),
                lease_expiry(),
            )
            .await
            .expect("open normally finished block");
        finished.write(Bytes::from_static(b"d")).await.expect("final frame");
        finished.finish().await.expect("normal finish");
        wait_for_count(&state.write_eofs, 1).await;
        assert_eq!(state.write_cancellations.load(Ordering::SeqCst), 3);
        let _ = shutdown.send(());
    }
}
