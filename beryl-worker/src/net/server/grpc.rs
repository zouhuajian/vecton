// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! gRPC WorkerDataService adapter and server entry point.

use crate::config::WorkerRegistrationConfig;
use crate::control::RegistrationSet;
use crate::data::convert::{proto_to_read_block_request, proto_to_write_block_request};
use crate::data::core::{ActiveBlockRead, ActiveBlockWrite, WorkerCore};
use crate::error::WorkerError;
use crate::observe;
use crate::runtime::DataRpcPermit;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RpcErrorDetail, WorkerErrorKind};
use beryl_common::header::{
    HEADER_WORKER_DATA_ERROR_DETAIL, HEADER_WORKER_DATA_REJECTION, WORKER_DATA_ERROR_DETAIL_V1,
    WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT,
};
use beryl_common::observe::propagation::{extract_trace_context, ExtractedContext};
use beryl_proto::common::RequestHeaderProto;
use beryl_proto::common::{ClientInfoProto, ErrorDetailProto, TraceContextProto};
use beryl_proto::convert::require_worker_run_id;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::AuthorizeBlockWriteRequestProto;
use beryl_proto::worker::worker_data_service_server::{WorkerDataService, WorkerDataServiceServer};
use beryl_proto::worker::write_block_request_proto::Payload;
use beryl_proto::worker::{
    DataRequestHeaderProto, DataResponseHeaderProto, ReadBlockChunkProto, ReadBlockRequestProto,
    WriteBlockRequestProto, WriteBlockResponseProto,
};
use beryl_types::{CallId, ClientId, GroupName};
use bytes::Bytes;
use futures::{stream, Stream, StreamExt};
use prost::Message;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::service::Routes;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status, Streaming};
use tracing::Span;

/// Worker data service with independent process-wide read and write admission.
#[derive(Clone)]
pub struct WorkerDataServiceImpl {
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
    read_slots: Arc<Semaphore>,
    write_slots: Arc<Semaphore>,
    metadata: FileSystemServiceProtoClient<Channel>,
    control_client_id: ClientId,
}

impl WorkerDataServiceImpl {
    /// Creates independent process-wide admission pools for the two data modes.
    pub fn new(
        core: Arc<WorkerCore>,
        registration_state: Arc<RegistrationSet>,
        max_concurrent_reads: usize,
        max_concurrent_writes: usize,
        metadata: &WorkerRegistrationConfig,
    ) -> Result<Self, WorkerError> {
        metadata
            .validate()
            .map_err(|e| WorkerError::InvalidArgument(e.message))?;
        let timeout = std::time::Duration::from_millis(metadata.request_timeout_ms);
        let channel = Endpoint::from_shared(metadata.endpoints[0].clone())
            .map_err(|e| WorkerError::InvalidArgument(e.to_string()))?
            .connect_timeout(timeout)
            .timeout(timeout)
            .connect_lazy();
        Ok(Self {
            core,
            registration_state,
            read_slots: Arc::new(Semaphore::new(max_concurrent_reads)),
            write_slots: Arc::new(Semaphore::new(max_concurrent_writes)),
            metadata: FileSystemServiceProtoClient::new(channel),
            control_client_id: ClientId::generate(),
        })
    }

    /// Checks the live session before local IO; transport failure cannot grant authority.
    async fn authorize_write(&self, req: &crate::data::core::WriteBlockRequest) -> Result<u64, WorkerError> {
        let registration = self
            .registration_state
            .registration_for_group(&req.group_name)
            .ok_or_else(|| WorkerError::Unavailable("Worker registration is unavailable".into()))?;
        let header = RequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: CallId::new().to_string(),
                client_id: Some(self.control_client_id.into()),
                client_name: "worker-write-authorization".into(),
            }),
            group_name: req.group_name.to_string(),
            ..Default::default()
        };
        let response = self
            .metadata
            .clone()
            .authorize_block_write(AuthorizeBlockWriteRequestProto {
                header: Some(header),
                group_name: req.group_name.to_string(),
                worker_id: registration.worker_id.as_raw(),
                worker_run_id: req.worker_run_id.to_string(),
                fencing_token: Some(req.fencing_token.into()),
                write_offset: req.write_offset,
                block_size: req.block_size,
                block_format_id: req.block_format_id.as_raw(),
                chunk_size: req.chunk_size,
                tier: beryl_proto::common::TierProto::from(req.tier) as i32,
            })
            .await
            .map_err(|e| WorkerError::Unavailable(format!("Metadata write authorization failed: {e}")))?
            .into_inner();
        let header = response
            .header
            .ok_or_else(|| WorkerError::Corrupt("write authorization response has no header".into()))?;
        if let Some(error) = header.error {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Worker(WorkerErrorKind::Fencing),
                message: error.message,
            });
        }
        if header.group_name != req.group_name.as_str() || response.visible_len > req.write_offset {
            return Err(WorkerError::Corrupt(
                "write authorization response has inconsistent scope".into(),
            ));
        }
        Ok(response.visible_len)
    }

    /// Acquires one read slot without queuing so write saturation cannot block reads.
    fn acquire_read_rpc(&self) -> Result<DataRpcPermit, Status> {
        Self::acquire_data_rpc(Arc::clone(&self.read_slots), "read")
    }

    /// Acquires one write slot without queuing so read saturation cannot block writes.
    fn acquire_write_rpc(&self) -> Result<DataRpcPermit, Status> {
        Self::acquire_data_rpc(Arc::clone(&self.write_slots), "write")
    }

    /// Rejects one mode before Worker write IO or read IO begins when its pool is full.
    fn acquire_data_rpc(slots: Arc<Semaphore>, mode: &'static str) -> Result<DataRpcPermit, Status> {
        match slots.try_acquire_owned() {
            Ok(permit) => Ok(DataRpcPermit::new(permit, mode)),
            Err(_) => {
                observe::record_data_rpc_capacity_rejection(mode);
                let mut metadata = MetadataMap::new();
                metadata.insert(
                    HEADER_WORKER_DATA_REJECTION,
                    MetadataValue::from_static(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT),
                );
                Err(Status::with_metadata(
                    Code::ResourceExhausted,
                    format!("Worker {mode} RPC capacity exhausted"),
                    metadata,
                ))
            }
        }
    }

    fn default_client() -> ClientInfoProto {
        ClientInfoProto {
            call_id: String::new(),
            client_id: None,
            client_name: String::new(),
        }
    }

    fn error_response_header(header: Option<DataRequestHeaderProto>, error: WorkerError) -> DataResponseHeaderProto {
        DataResponseHeaderProto {
            client: Some(
                header
                    .and_then(|value| value.client)
                    .unwrap_or_else(Self::default_client),
            ),
            error: Some(Self::error_detail(&error)),
        }
    }

    fn error_detail(error: &WorkerError) -> ErrorDetailProto {
        let rpc_error: RpcErrorDetail = error.clone().into();
        beryl_proto::convert::rpc_error_to_proto(&rpc_error)
    }

    /// Preserves the structured Worker error contract on streaming status errors.
    fn data_error_status(header: Option<DataRequestHeaderProto>, error: WorkerError) -> Status {
        let status = error.to_status();
        let details = Self::error_response_header(header, error).encode_to_vec();
        let mut metadata = MetadataMap::new();
        metadata.insert(
            HEADER_WORKER_DATA_ERROR_DETAIL,
            MetadataValue::from_static(WORKER_DATA_ERROR_DETAIL_V1),
        );
        Status::with_details_and_metadata(status.code(), status.message(), Bytes::from(details), metadata)
    }

    fn ensure_group_ready_for_run(&self, group_name: &str, worker_run_id: &str) -> Result<(), WorkerError> {
        let group_name = GroupName::parse(group_name)
            .map_err(|error| WorkerError::InvalidArgument(format!("group_name invalid: {error}")))?;
        let requested = require_worker_run_id(worker_run_id, "worker_run_id").map_err(WorkerError::InvalidArgument)?;
        let Some(registration) = self.registration_state.registration_for_group(&group_name) else {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                message: format!("worker is not registered for metadata group {group_name}"),
            });
        };
        if !self.registration_state.is_ready(&group_name) {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Metadata(MetadataErrorKind::StaleState),
                message: format!("worker is not ready for metadata group {group_name}"),
            });
        }
        if !requested.matches(registration.worker_run_id) {
            return Err(WorkerError::RefreshMetadata {
                kind: ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                message: format!(
                    "worker_run_id mismatch: requested={requested}, current={}",
                    registration.worker_run_id
                ),
            });
        }
        Ok(())
    }

    /// Consumes the command before returning the response stream, making the
    /// first empty response an exact acknowledgement of block-open checkpoint.
    async fn begin_write_block<S>(
        &self,
        mut requests: S,
        rpc_permit: DataRpcPermit,
        transport_context: &ExtractedContext,
        started: Instant,
    ) -> Result<WriteBlockState<S>, Status>
    where
        S: Stream<Item = Result<WriteBlockRequestProto, Status>> + Unpin,
    {
        let first = match requests.next().await {
            Some(Ok(request)) => request,
            Some(Err(status)) => {
                observe::record_data_rpc(
                    "write_block",
                    "error",
                    status_error_kind(&status),
                    started.elapsed().as_secs_f64(),
                );
                return Err(status);
            }
            None => {
                let error = WorkerError::InvalidArgument("WriteBlock requires a command payload".to_string());
                observe::record_data_rpc(
                    "write_block",
                    "error",
                    observe::worker_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(Self::data_error_status(None, error));
            }
        };
        let mut command = match first.payload {
            Some(Payload::Command(command)) => command,
            Some(Payload::Data(_)) | None => {
                let error = WorkerError::InvalidArgument("first WriteBlock payload must be command".to_string());
                observe::record_data_rpc(
                    "write_block",
                    "error",
                    observe::worker_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(Self::data_error_status(None, error));
            }
        };
        merge_data_header_transport_context(&mut command.header, transport_context);
        let header = command.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&command.group_name, &command.worker_run_id) {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_stream_open("write", "error", error_kind);
            observe::record_data_rpc("write_block", "error", error_kind, started.elapsed().as_secs_f64());
            return Err(Self::data_error_status(header, error));
        }
        let domain = proto_to_write_block_request(*command).map_err(|error| {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_stream_open("write", "error", error_kind);
            observe::record_data_rpc("write_block", "error", error_kind, started.elapsed().as_secs_f64());
            Self::data_error_status(header.clone(), error)
        })?;
        let block_pin = self
            .core
            .pin_write_authorization(&domain)
            .map_err(|error| Self::data_error_status(header.clone(), error))?;
        let visible_len = self
            .authorize_write(&domain)
            .await
            .map_err(|error| Self::data_error_status(header.clone(), error))?;
        let write = self
            .core
            .begin_block_write(domain, rpc_permit, block_pin, visible_len)
            .await
            .map_err(|error| {
                let error_kind = observe::worker_error_kind(&error);
                observe::record_stream_open("write", "error", error_kind);
                observe::record_data_rpc("write_block", "error", error_kind, started.elapsed().as_secs_f64());
                Self::data_error_status(header.clone(), error)
            })?;
        observe::record_stream_open("write", "ok", "none");
        Ok(WriteBlockState {
            core: Arc::clone(&self.core),
            requests,
            write: Some(write),
            request_header: header,
            started,
            acknowledgement_pending: true,
            outcome: StreamOutcome::Active,
        })
    }
}

#[derive(Clone, Copy)]
enum StreamOutcome {
    Active,
    Success,
    Error(&'static str),
}

/// Owns one read response stream, its pin, and exact-once lifecycle metrics.
struct ReadBlockState {
    core: Arc<WorkerCore>,
    read: ActiveBlockRead,
    request_header: Option<DataRequestHeaderProto>,
    started: Instant,
    outcome: StreamOutcome,
}

impl ReadBlockState {
    async fn next(mut self) -> Option<(Result<ReadBlockChunkProto, Status>, Self)> {
        if !matches!(self.outcome, StreamOutcome::Active) {
            return None;
        }
        match self.core.read_block_chunk(&mut self.read).await {
            Ok(Some(data)) => {
                observe::record_stream_frame("read", "ok", "none", data.len() as u64);
                Some((Ok(ReadBlockChunkProto { data }), self))
            }
            Ok(None) => {
                self.outcome = StreamOutcome::Success;
                None
            }
            Err(error) => {
                let error_kind = observe::worker_error_kind(&error);
                self.outcome = StreamOutcome::Error(error_kind);
                observe::record_stream_frame("read", "error", error_kind, 0);
                let status = WorkerDataServiceImpl::data_error_status(self.request_header.clone(), error);
                Some((Err(status), self))
            }
        }
    }
}

impl Drop for ReadBlockState {
    fn drop(&mut self) {
        let (status, error_kind) = outcome_labels(self.outcome);
        observe::record_data_rpc("read_block", status, error_kind, self.started.elapsed().as_secs_f64());
    }
}

/// Owns the inbound stream and its single block write until durable Ready,
/// explicit failure cleanup, or cancellation-triggered deferred cleanup.
struct WriteBlockState<S> {
    core: Arc<WorkerCore>,
    requests: S,
    write: Option<ActiveBlockWrite>,
    request_header: Option<DataRequestHeaderProto>,
    started: Instant,
    acknowledgement_pending: bool,
    outcome: StreamOutcome,
}

impl<S> WriteBlockState<S>
where
    S: Stream<Item = Result<WriteBlockRequestProto, Status>> + Unpin,
{
    async fn next(mut self) -> Option<(Result<WriteBlockResponseProto, Status>, Self)> {
        if self.acknowledgement_pending {
            self.acknowledgement_pending = false;
            return Some((Ok(WriteBlockResponseProto {}), self));
        }
        if !matches!(self.outcome, StreamOutcome::Active) {
            return None;
        }

        loop {
            let next = tokio::select! {
                biased;
                _ = self.write.as_ref().expect("active write").retired() => {
                    Err(WorkerError::Cancelled("block writer authority was revoked".into()))
                }
                next = self.requests.next() => Ok(next),
            };
            let next = match next {
                Ok(next) => next,
                Err(error) => return Some(self.fail(error).await),
            };
            match next {
                Some(Ok(request)) => match request.payload {
                    Some(Payload::Data(data)) => {
                        let len = data.len() as u64;
                        let write = self.write.as_mut().expect("active response state owns a block write");
                        if let Err(error) = self.core.write_block_data(write, data).await {
                            observe::record_stream_frame("write", "error", observe::worker_error_kind(&error), len);
                            return Some(self.fail(error).await);
                        }
                        observe::record_stream_frame("write", "ok", "none", len);
                    }
                    Some(Payload::Command(_)) | None => {
                        let error = WorkerError::InvalidArgument(
                            "every WriteBlock payload after command must be data".to_string(),
                        );
                        return Some(self.fail(error).await);
                    }
                },
                Some(Err(status)) => {
                    let error_kind = status_error_kind(&status);
                    self.outcome = StreamOutcome::Error(error_kind);
                    self.abort_active().await;
                    return Some((Err(status), self));
                }
                None => {
                    let result = self
                        .core
                        .finish_block_write(self.write.as_mut().expect("active response state owns a block write"))
                        .await;
                    match result {
                        Ok(()) => {
                            self.write.take();
                            self.outcome = StreamOutcome::Success;
                            return None;
                        }
                        Err(error) => return Some(self.fail(error).await),
                    }
                }
            }
        }
    }

    async fn fail(mut self, error: WorkerError) -> (Result<WriteBlockResponseProto, Status>, Self) {
        let error_kind = observe::worker_error_kind(&error);
        self.outcome = StreamOutcome::Error(error_kind);
        self.abort_active().await;
        let status = WorkerDataServiceImpl::data_error_status(self.request_header.clone(), error);
        (Err(status), self)
    }

    async fn abort_active(&mut self) {
        let Some(write) = self.write.take() else {
            return;
        };
        if let Err(error) = self.core.abort_block_write(write).await {
            tracing::warn!(
                target: "worker.state",
                op = "AbortBlockWrite",
                error_code = observe::worker_error_kind(&error),
                error = %error,
                "Failed block write cleanup retained local resources for retry"
            );
        }
    }
}

impl<S> Drop for WriteBlockState<S> {
    fn drop(&mut self) {
        let (status, error_kind) = outcome_labels(self.outcome);
        observe::record_data_rpc("write_block", status, error_kind, self.started.elapsed().as_secs_f64());
    }
}

#[tonic::async_trait]
impl WorkerDataService for WorkerDataServiceImpl {
    type ReadBlockStream = Pin<Box<dyn Stream<Item = Result<ReadBlockChunkProto, Status>> + Send>>;
    async fn read_block(
        &self,
        request: Request<ReadBlockRequestProto>,
    ) -> Result<Response<Self::ReadBlockStream>, Status> {
        let started = Instant::now();
        let rpc_permit = Arc::new(self.acquire_read_rpc().inspect_err(|status| {
            observe::record_data_rpc(
                "read_block",
                "error",
                status_error_kind(status),
                started.elapsed().as_secs_f64(),
            );
        })?);
        let transport_context = extract_trace_context(request.metadata());
        let mut request = request.into_inner();
        merge_data_header_transport_context(&mut request.header, &transport_context);
        let header = request.header.clone();
        if let Err(error) = self.ensure_group_ready_for_run(&request.group_name, &request.worker_run_id) {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
            return Err(Self::data_error_status(header, error));
        }
        let domain = proto_to_read_block_request(request).map_err(|error| {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
            Self::data_error_status(header.clone(), error)
        })?;
        let read = self.core.begin_block_read(domain, rpc_permit).await.map_err(|error| {
            let error_kind = observe::worker_error_kind(&error);
            observe::record_data_rpc("read_block", "error", error_kind, started.elapsed().as_secs_f64());
            Self::data_error_status(header.clone(), error)
        })?;
        let state = ReadBlockState {
            core: Arc::clone(&self.core),
            read,
            request_header: header,
            started,
            outcome: StreamOutcome::Active,
        };
        Ok(Response::new(Box::pin(stream::unfold(state, |state| state.next()))))
    }

    type WriteBlockStream = Pin<Box<dyn Stream<Item = Result<WriteBlockResponseProto, Status>> + Send>>;

    async fn write_block(
        &self,
        request: Request<Streaming<WriteBlockRequestProto>>,
    ) -> Result<Response<Self::WriteBlockStream>, Status> {
        let started = Instant::now();
        let rpc_permit = self.acquire_write_rpc().inspect_err(|status| {
            observe::record_data_rpc(
                "write_block",
                "error",
                status_error_kind(status),
                started.elapsed().as_secs_f64(),
            );
        })?;
        let transport_context = extract_trace_context(request.metadata());
        let state = self
            .begin_write_block(request.into_inner(), rpc_permit, &transport_context, started)
            .await?;
        Ok(Response::new(Box::pin(stream::unfold(state, |state| state.next()))))
    }
}

fn merge_data_header_transport_context(header: &mut Option<DataRequestHeaderProto>, context: &ExtractedContext) {
    record_transport_context(context);
    let Some(header) = header else {
        return;
    };
    if header.trace_context.as_ref().is_some_and(trace_context_proto_is_empty) {
        header.trace_context = None;
    }
    if context.is_empty() {
        return;
    }
    let trace_context = header.trace_context.get_or_insert_with(Default::default);
    if trace_context.traceparent.is_none() {
        trace_context.traceparent = context.traceparent.clone();
    }
    if trace_context.tracestate.is_none() {
        trace_context.tracestate = context.tracestate.clone();
    }
    if trace_context.baggage.is_none() {
        trace_context.baggage = context.baggage.clone();
    }
}

fn trace_context_proto_is_empty(context: &TraceContextProto) -> bool {
    context.traceparent.is_none() && context.tracestate.is_none() && context.baggage.is_none()
}

fn record_transport_context(context: &ExtractedContext) {
    if let Some(traceparent) = &context.traceparent {
        Span::current().record("traceparent", traceparent);
    }
}

fn outcome_labels(outcome: StreamOutcome) -> (&'static str, &'static str) {
    match outcome {
        StreamOutcome::Active => ("cancelled", "cancelled"),
        StreamOutcome::Success => ("ok", "none"),
        StreamOutcome::Error(error_kind) => ("error", error_kind),
    }
}

fn status_error_kind(status: &Status) -> &'static str {
    match status.code() {
        Code::Ok => "none",
        Code::InvalidArgument => "invalid_argument",
        Code::NotFound => "not_found",
        Code::FailedPrecondition => "failed_precondition",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::Unavailable => "unavailable",
        Code::DeadlineExceeded => "timeout",
        Code::Unimplemented => "unimplemented",
        Code::Cancelled => "cancelled",
        Code::Internal => "internal",
        _ => "rpc_status",
    }
}

/// Builds the Worker data-plane routes retained by the process-owned listener.
pub fn worker_data_routes(
    core: Arc<WorkerCore>,
    registration_state: Arc<RegistrationSet>,
    max_concurrent_reads: usize,
    max_concurrent_writes: usize,
    metadata: &WorkerRegistrationConfig,
) -> Result<Routes, WorkerError> {
    let service = WorkerDataServiceImpl::new(
        core,
        registration_state,
        max_concurrent_reads,
        max_concurrent_writes,
        metadata,
    )?;
    Ok(Routes::new(
        WorkerDataServiceServer::new(service)
            .max_decoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE)
            .max_encoding_message_size(beryl_proto::MAX_WORKER_DATA_MESSAGE_SIZE),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{Registration, RegistrationSet};
    use crate::data::core::WorkerCore;
    use crate::store::block::{BlockState, FullBlockFileStore, FullBlockFileStoreConfig};
    use beryl_common::observe::propagation::extract_trace_context;
    use beryl_proto::common::{BlockIdProto, TierProto};
    use beryl_proto::worker::write_block_request_proto::Payload;
    use beryl_proto::worker::{WriteBlockCommandProto, WriteBlockRequestProto};
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::{GroupName, WorkerRunId};
    use bytes::Bytes;
    use futures::stream;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::time::Instant as TokioInstant;
    use tonic::metadata::MetadataMap;
    use tonic::{Code, Status};

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("group name")
    }

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(3))
    }

    fn registered_service() -> (TempDir, Arc<FullBlockFileStore>, WorkerDataServiceImpl, WorkerRunId) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let core = Arc::new(WorkerCore::with_local_store(512, 2048, store.clone()));
        let registrations = Arc::new(RegistrationSet::new());
        let worker_run_id = WorkerRunId::new();
        registrations.record_registered(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(5),
            worker_run_id,
            advertised_endpoint: "http://127.0.0.1:1".to_string(),
        });
        registrations.record_heartbeat_success(&group_name(), Duration::from_secs(30));
        (
            temp,
            store,
            WorkerDataServiceImpl::new(core, registrations, 1, 1, &WorkerRegistrationConfig::default()).unwrap(),
            worker_run_id,
        )
    }

    fn command(worker_run_id: WorkerRunId) -> WriteBlockRequestProto {
        WriteBlockRequestProto {
            payload: Some(Payload::Command(Box::new(WriteBlockCommandProto {
                header: None,
                group_name: group_name().to_string(),
                block_id: Some(BlockIdProto {
                    inode_id: 7,
                    block_index: 3,
                }),
                worker_run_id: worker_run_id.to_string(),
                block_format_id: BlockFormatId::DURABLE_PREFIX.as_raw(),
                block_size: 4096,
                chunk_size: BlockFormatId::DURABLE_PREFIX.spec().unwrap().storage_chunk_size,
                fencing_token: Some(
                    beryl_types::FencingToken::new(block_id(), ClientId::new(9), beryl_types::LeaseEpoch::new(55))
                        .into(),
                ),
                write_offset: 0,
                tier: TierProto::TierHdd as i32,
            }))),
        }
    }

    impl WorkerDataServiceImpl {
        // Protocol tests start after online authorization; real RPC authorization is covered by e2e.
        async fn begin_authorized_test_write<S>(
            &self,
            mut requests: S,
            permit: DataRpcPermit,
            _context: &ExtractedContext,
            started: Instant,
        ) -> Result<WriteBlockState<S>, Status>
        where
            S: Stream<Item = Result<WriteBlockRequestProto, Status>> + Unpin,
        {
            let Some(Payload::Command(command)) = requests.next().await.unwrap()?.payload else {
                panic!("test requires a command")
            };
            let req = proto_to_write_block_request(*command).unwrap();
            let pin = self.core.pin_write_authorization(&req).unwrap();
            let write = self.core.begin_block_write(req, permit, pin, 0).await.unwrap();
            Ok(WriteBlockState {
                core: self.core.clone(),
                requests,
                write: Some(write),
                request_header: None,
                started,
                acknowledgement_pending: true,
                outcome: StreamOutcome::Active,
            })
        }
    }

    #[tokio::test]
    async fn write_block_acknowledges_open_then_checkpoints_on_request_eof() {
        let (_temp, store, service, worker_run_id) = registered_service();
        let requests = stream::iter(vec![
            Ok(command(worker_run_id)),
            Ok(WriteBlockRequestProto {
                payload: Some(Payload::Data(Bytes::from_static(b"abc"))),
            }),
            Ok(WriteBlockRequestProto {
                payload: Some(Payload::Data(Bytes::from_static(b"def"))),
            }),
        ]);
        let context = extract_trace_context(&MetadataMap::new());
        let rpc_permit = service.acquire_write_rpc().expect("write capacity");
        let state = service
            .begin_authorized_test_write(requests, rpc_permit, &context, Instant::now())
            .await
            .expect("begin write");
        let (ack, state) = state.next().await.expect("acknowledgement");
        ack.expect("acknowledgement is successful");
        assert!(state.next().await.is_none());

        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.visibility.block_state, BlockState::Ready);
        assert_eq!(meta.source.durable_len, 6);
    }

    #[tokio::test]
    async fn cancellation_after_ack_releases_write_through_owned_cleanup() {
        let (_temp, _store, service, worker_run_id) = registered_service();
        let requests = stream::iter(vec![Ok(command(worker_run_id))]);
        let context = extract_trace_context(&MetadataMap::new());
        let rpc_permit = service.acquire_write_rpc().expect("write capacity");
        let state = service
            .begin_authorized_test_write(requests, rpc_permit, &context, Instant::now())
            .await
            .expect("begin write");
        let (_ack, state) = state.next().await.expect("acknowledgement");
        drop(state);
        assert!(
            !service
                .core
                .drain_block_writes_until(TokioInstant::now() + Duration::from_secs(1))
                .await
        );

        let replacement = stream::iter(vec![Ok(command(worker_run_id))]);
        let rpc_permit = service.acquire_write_rpc().expect("released write capacity");
        let state = service
            .begin_authorized_test_write(replacement, rpc_permit, &context, Instant::now())
            .await
            .expect("replacement write");
        let (_ack, state) = state.next().await.expect("replacement acknowledgement");
        drop(state);
    }

    #[tokio::test]
    async fn a_second_command_fails_and_cleans_the_owned_block_write() {
        let (_temp, _store, service, worker_run_id) = registered_service();
        let requests = stream::iter(vec![Ok(command(worker_run_id)), Ok(command(worker_run_id))]);
        let context = extract_trace_context(&MetadataMap::new());
        let rpc_permit = service.acquire_write_rpc().expect("write capacity");
        let state = service
            .begin_authorized_test_write(requests, rpc_permit, &context, Instant::now())
            .await
            .expect("begin write");
        let (_ack, state) = state.next().await.expect("acknowledgement");
        let (error, _state) = state.next().await.expect("terminal error");
        assert_eq!(
            error.expect_err("second command must fail").code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn read_and_write_capacity_are_independent() {
        let (_temp, _store, service, _worker_run_id) = registered_service();
        let read = service.acquire_read_rpc().expect("read capacity");
        let write = service.acquire_write_rpc().expect("write capacity");
        let assert_capacity_rejection = |status: Status| {
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert_eq!(
                status
                    .metadata()
                    .get(HEADER_WORKER_DATA_REJECTION)
                    .and_then(|value| value.to_str().ok()),
                Some(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT)
            );
        };

        assert_capacity_rejection(service.acquire_read_rpc().expect_err("read limit"));
        assert_capacity_rejection(service.acquire_write_rpc().expect_err("write limit"));

        drop(read);
        service.acquire_read_rpc().expect("read capacity released");
        assert_capacity_rejection(service.acquire_write_rpc().expect_err("write remains full"));
        drop(write);
    }
    #[tokio::test]
    async fn reclaim_revokes_idle_stream_and_releases_its_pin_without_another_frame() {
        let (_temp, store, service, run) = registered_service();
        let requests = stream::iter(vec![Ok(command(run))]).chain(stream::pending());
        let state = service
            .begin_authorized_test_write(
                requests,
                service.acquire_write_rpc().unwrap(),
                &extract_trace_context(&MetadataMap::new()),
                Instant::now(),
            )
            .await
            .unwrap();
        let (_, state) = state.next().await.unwrap();
        let idle = state.next();
        tokio::pin!(idle);
        assert!(futures::poll!(&mut idle).is_pending());
        let reclaim = service.core.reclaim_block(crate::store::block::ReclaimBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
        });
        tokio::pin!(reclaim);
        assert!(futures::poll!(&mut reclaim).is_pending());
        let (error, state) = tokio::time::timeout(Duration::from_secs(1), idle)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(error.unwrap_err().code(), Code::Cancelled);
        assert!(state.write.is_none());
        assert!(
            !service
                .core
                .drain_block_writes_until(TokioInstant::now() + Duration::from_secs(1))
                .await
        );
        tokio::time::timeout(Duration::from_secs(1), reclaim)
            .await
            .unwrap()
            .unwrap();
        assert!(!store.paths(&group_name(), block_id()).data_path.exists());
        service
            .acquire_write_rpc()
            .expect("retired stream releases its RPC slot");
    }
}
