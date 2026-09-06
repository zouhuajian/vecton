// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! MetadataWorkerService implementation.

use super::manager::{
    worker_net_protocol_label, BlockReportBlock, BlockReportBlockState, BlockReportChange, HealthStatus, WorkerManager,
    WORKER_NET_PROTOCOL_GRPC,
};
use crate::error::{to_rpc_error, MetadataError, MetadataResult};
use crate::maintenance::BlockCleanupCoordinator;
use crate::observe;
use crate::raft::{AppRaftNode, ApplySuccess, Command};
use crate::service::extract_and_inject_context;
use ::beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RpcErrorDetail, WorkerErrorKind};
use ::beryl_common::header::ResponseHeader;
use ::beryl_common::observe::propagation::{extract_trace_context, ExtractedContext};
use beryl_common::header::ClientInfo;
use beryl_proto::common::{
    EndpointProto, ErrorDetailProto, RequestHeaderProto, ResponseHeaderProto, TraceContextProto,
};
use beryl_proto::convert::require_worker_run_id;
use beryl_proto::metadata::block_report_request_proto::Batch;
use beryl_proto::metadata::delta_block_report_entry_proto::Block;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProto;
use beryl_proto::metadata::{
    BlockCleanupCommandProto, BlockReportKindProto, BlockReportRequestProto, BlockReportResponseProto,
    DeltaBlockReportEntryProto, HeartbeatRequestProto, HeartbeatResponseProto, RegisterWorkerRequestProto,
    RegisterWorkerResponseProto, ReportedBlockProto, ReportedBlockStateProto, TierFreeProto,
};
use beryl_types::{BlockId, GroupName, GroupStateWatermark, TierFree, WorkerId, MAX_REPORT_ENTRIES};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::Mutex;
use tonic::{Code, Request, Response, Status};
use tracing::{info, instrument, warn};

fn register_worker_response_with_header(header: ResponseHeaderProto) -> RegisterWorkerResponseProto {
    RegisterWorkerResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn heartbeat_response_with_header(header: ResponseHeaderProto) -> HeartbeatResponseProto {
    HeartbeatResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn block_report_response_with_header(header: ResponseHeaderProto) -> BlockReportResponseProto {
    BlockReportResponseProto {
        header: Some(header),
        ..Default::default()
    }
}

fn register_worker_response_header(response: &RegisterWorkerResponseProto) -> Option<&ResponseHeaderProto> {
    response.header.as_ref()
}

fn heartbeat_response_header(response: &HeartbeatResponseProto) -> Option<&ResponseHeaderProto> {
    response.header.as_ref()
}

fn block_report_response_header(response: &BlockReportResponseProto) -> Option<&ResponseHeaderProto> {
    response.header.as_ref()
}

#[derive(Clone, Copy)]
enum MetadataWorkerMetric {
    Registration,
    Heartbeat,
    BlockReport(&'static str),
}

/// MetadataWorkerService implementation.
pub struct MetadataWorkerServiceImpl {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
    served_group_name: GroupName,
    cleanup: Option<Arc<BlockCleanupCoordinator>>,
    registration_serial: Mutex<()>,
}

impl MetadataWorkerServiceImpl {
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        served_group_name: GroupName,
    ) -> Self {
        Self::build(raft_node, worker_manager, served_group_name, None)
    }

    pub(crate) fn new_with_cleanup(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        served_group_name: GroupName,
        cleanup: Arc<BlockCleanupCoordinator>,
    ) -> Self {
        Self::build(raft_node, worker_manager, served_group_name, Some(cleanup))
    }

    fn build(
        raft_node: Arc<AppRaftNode>,
        worker_manager: Arc<WorkerManager>,
        served_group_name: GroupName,
        cleanup: Option<Arc<BlockCleanupCoordinator>>,
    ) -> Self {
        Self {
            raft_node,
            worker_manager,
            served_group_name,
            cleanup,
            registration_serial: Mutex::new(()),
        }
    }

    /// Helper: create a response header from request header with group name.
    fn create_response_header_from_request(
        &self,
        req_header: &Option<RequestHeaderProto>,
        group_name: Option<&GroupName>,
    ) -> ResponseHeaderProto {
        let mut header: ResponseHeaderProto = req_header
            .as_ref()
            .and_then(|h| h.client.as_ref())
            .and_then(|c| ClientInfo::try_from(c.clone()).ok())
            .map(|client| (&ResponseHeader::ok(client)).into())
            .unwrap_or_default();
        if let Some(group_name) = group_name {
            header.group_name = group_name.to_string();
        }
        if self.raft_node.is_leader() {
            if let (Some(group_name), Some(sid)) = (group_name, self.raft_node.get_last_applied_state_id()) {
                header.state = vec![(&GroupStateWatermark::new(group_name.clone(), sid)).into()];
            }
        }
        header
    }

    fn group_name_from_request_header(req_header: &Option<RequestHeaderProto>) -> Option<GroupName> {
        req_header
            .as_ref()
            .and_then(|header| GroupName::parse_optional(&header.group_name).ok().flatten())
    }

    fn error_response_header_from_request(
        &self,
        req_header: &Option<RequestHeaderProto>,
        error: RpcErrorDetail,
    ) -> ResponseHeaderProto {
        let mut header = self
            .create_response_header_from_request(req_header, Self::group_name_from_request_header(req_header).as_ref());
        header.error = Some(beryl_proto::convert::rpc_error_to_proto(&error));
        header
    }

    fn response_with_error<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        error: RpcErrorDetail,
        make_response: fn(ResponseHeaderProto) -> T,
    ) -> Result<Response<T>, Status> {
        Ok(Response::new(make_response(
            self.error_response_header_from_request(req_header, error),
        )))
    }

    fn invalid_request_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            to_rpc_error(MetadataError::InvalidArgument(message.into())),
            make_response,
        )
    }

    fn metadata_error_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        error: MetadataError,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(req_header, to_rpc_error(error), make_response)
    }

    fn group_mismatch_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::fail(ErrorKind::Metadata(MetadataErrorKind::GroupMismatch), message),
            make_response,
        )
    }

    fn need_register_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::NotRegistered), message),
            make_response,
        )
    }

    fn worker_run_mismatch_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), message),
            make_response,
        )
    }

    fn worker_descriptor_mismatch_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch), message),
            make_response,
        )
    }

    fn liveness_timeout_ms(&self) -> u32 {
        self.worker_manager.heartbeat_timeout_ms()
    }

    fn full_report_required_response<T>(
        &self,
        req_header: &Option<RequestHeaderProto>,
        make_response: fn(ResponseHeaderProto) -> T,
        message: impl Into<String>,
    ) -> Result<Response<T>, Status> {
        self.response_with_error(
            req_header,
            RpcErrorDetail::send_full_block_report(ErrorKind::Worker(WorkerErrorKind::FullReportRequired), message),
            make_response,
        )
    }

    fn proto_to_report_block(block: ReportedBlockProto) -> MetadataResult<BlockReportBlock> {
        let block_id_proto = block
            .block_id
            .ok_or_else(|| MetadataError::InvalidArgument("block report entry missing block_id".to_string()))?;
        let block_id = BlockId::try_from(block_id_proto)
            .unwrap_or_else(|error| panic!("validated BlockIdProto must be valid: {error}"));
        let block_state = match block.state() {
            ReportedBlockStateProto::ReportedBlockStateReady => BlockReportBlockState::Ready,
            ReportedBlockStateProto::ReportedBlockStateCorrupt => BlockReportBlockState::Corrupt,
            ReportedBlockStateProto::ReportedBlockStateDeleting => BlockReportBlockState::Deleting,
            ReportedBlockStateProto::ReportedBlockStateUnspecified => {
                return Err(MetadataError::InvalidArgument(
                    "block report entry state must be specified".to_string(),
                ));
            }
        };
        if block_state == BlockReportBlockState::Ready && block.lease_epoch == 0 {
            return Err(MetadataError::InvalidArgument(
                "Ready block report entry lease_epoch must be non-zero".to_string(),
            ));
        }
        let tier = if block_state == BlockReportBlockState::Ready {
            Some(beryl_proto::convert::parse_known_tier(block.tier).map_err(MetadataError::InvalidArgument)?)
        } else {
            None
        };
        Ok(BlockReportBlock {
            tier,
            block_id,
            lease_epoch: block.lease_epoch,
            block_state,
            effective_len: block.effective_len,
        })
    }

    fn proto_to_delta_entry(entry: DeltaBlockReportEntryProto) -> MetadataResult<BlockReportChange> {
        match entry.block {
            Some(Block::Present(block)) => Self::proto_to_report_block(block).map(BlockReportChange::Upsert),
            Some(Block::Absent(block_id)) => BlockId::try_from(block_id)
                .map(BlockReportChange::Remove)
                .map_err(|error| MetadataError::InvalidArgument(format!("invalid absent block_id: {error}"))),
            None => Err(MetadataError::InvalidArgument(
                "delta block report entry block must be specified".to_string(),
            )),
        }
    }

    fn record_worker_rpc_outcome<T>(
        method: &'static str,
        metric: MetadataWorkerMetric,
        started: Instant,
        outcome: &Result<Response<T>, Status>,
        response_header: fn(&T) -> Option<&ResponseHeaderProto>,
    ) {
        let duration = started.elapsed().as_secs_f64();
        let (status, error_kind) = metadata_worker_outcome_labels(outcome, response_header);

        observe::record_rpc_request("metadata_worker", method, status, error_kind, duration);
        match metric {
            MetadataWorkerMetric::Registration => observe::record_worker_registration(status, error_kind, duration),
            MetadataWorkerMetric::Heartbeat => observe::record_worker_heartbeat(status, error_kind, duration),
            MetadataWorkerMetric::BlockReport(kind) => {
                observe::record_worker_block_report(kind, status, error_kind, duration)
            }
        }
    }
}

fn metadata_worker_outcome_labels<T>(
    outcome: &Result<Response<T>, Status>,
    response_header: fn(&T) -> Option<&ResponseHeaderProto>,
) -> (&'static str, &'static str) {
    match outcome {
        Ok(response) => match response_header(response.get_ref()).and_then(|header| header.error.as_ref()) {
            Some(error) => ("error", metadata_worker_error_detail_kind(error)),
            None => ("ok", "none"),
        },
        Err(status) => ("error", tonic_status_error_kind(status)),
    }
}

fn metadata_worker_error_detail_kind(error: &ErrorDetailProto) -> &'static str {
    let rpc_error = beryl_proto::convert::rpc_error_from_proto(error);
    observe::rpc_error_kind(&rpc_error)
}

fn tonic_status_error_kind(status: &Status) -> &'static str {
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

fn block_report_kind(req: &BlockReportRequestProto) -> &'static str {
    match &req.batch {
        Some(Batch::FullReport(_)) => "full",
        Some(Batch::DeltaReport(_)) => "delta",
        None => "unknown",
    }
}

fn merge_request_header_transport_context(header: &mut Option<RequestHeaderProto>, context: &ExtractedContext) {
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

fn validate_advertised_endpoint(endpoint: EndpointProto) -> Result<String, String> {
    if endpoint.host.trim().is_empty() {
        return Err("advertised_endpoint host must not be empty".to_string());
    }
    if endpoint.port == 0 || endpoint.port > u32::from(u16::MAX) {
        return Err("advertised_endpoint port must be between 1 and 65535".to_string());
    }
    if endpoint
        .host
        .parse::<IpAddr>()
        .is_ok_and(|address| address.is_unspecified())
    {
        return Err("advertised_endpoint must not use a wildcard host".to_string());
    }
    match endpoint.host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => Ok(format!("[{}]:{}", endpoint.host, endpoint.port)),
        _ => Ok(format!("{}:{}", endpoint.host, endpoint.port)),
    }
}

fn parse_tier_free(entries: &[TierFreeProto]) -> Result<Vec<TierFree>, String> {
    entries
        .iter()
        .map(|entry| {
            let tier = beryl_proto::convert::parse_known_tier(entry.tier)
                .map_err(|err| format!("capacity.tier_free tier invalid: {err}"))?;
            Ok(TierFree {
                tier,
                free_bytes: entry.free_bytes,
            })
        })
        .collect()
}

fn parse_worker_request_group_name(req_header: &Option<RequestHeaderProto>) -> Result<GroupName, String> {
    let header = req_header
        .as_ref()
        .ok_or_else(|| "request header is required".to_string())?;
    GroupName::parse(&header.group_name).map_err(|error| format!("header group_name is invalid: {error}"))
}

#[tonic::async_trait]
impl MetadataWorkerServiceProto for MetadataWorkerServiceImpl {
    #[instrument(skip_all)]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequestProto>,
    ) -> Result<Response<RegisterWorkerResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let outcome = async {
            let mut req = request.into_inner();
            merge_request_header_transport_context(&mut req.header, &transport_context);
            let _caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => {
                    return self.response_with_error(&req.header, error, register_worker_response_with_header)
                }
            };

            if !self.raft_node.is_leader() {
                return self.metadata_error_response(
                    &req.header,
                    register_worker_response_with_header,
                    MetadataError::LeaderChanged(
                        "worker registration must be sent to the metadata group leader".into(),
                    ),
                );
            }

            let group_name = match parse_worker_request_group_name(&req.header) {
                Ok(group_name) => group_name,
                Err(error) => {
                    return self.invalid_request_response(&req.header, register_worker_response_with_header, error)
                }
            };
            if group_name != self.served_group_name {
                return self.invalid_request_response(
                    &req.header,
                    register_worker_response_with_header,
                    format!(
                        "register group_name {} does not match served metadata group {}",
                        group_name, self.served_group_name
                    ),
                );
            }
            let worker_id = WorkerId::new(req.worker_id);
            if worker_id.as_raw() == 0 {
                return self.invalid_request_response(
                    &req.header,
                    register_worker_response_with_header,
                    "worker_id must be non-zero",
                );
            }
            let worker_run_id = match require_worker_run_id(&req.worker_run_id, "RegisterWorkerRequest.worker_run_id") {
                Ok(worker_run_id) => worker_run_id,
                Err(error) => {
                    return self.invalid_request_response(&req.header, register_worker_response_with_header, error)
                }
            };
            let worker_net_protocol = WORKER_NET_PROTOCOL_GRPC;
            let endpoint = match req.advertised_endpoint {
                Some(endpoint) => endpoint,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        register_worker_response_with_header,
                        "Missing advertised_endpoint",
                    );
                }
            };
            let address = match validate_advertised_endpoint(endpoint) {
                Ok(address) => address,
                Err(message) => {
                    return self.invalid_request_response(&req.header, register_worker_response_with_header, message)
                }
            };
            let _registration_guard = self.registration_serial.lock().await;
            if let Err(error) = self.worker_manager.validate_worker_registration_preflight(
                &group_name,
                worker_id,
                worker_run_id,
                &address,
                worker_net_protocol,
            ) {
                return self.metadata_error_response(&req.header, register_worker_response_with_header, error);
            }

            let command = Command::RegisterWorkerDescriptor {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                group_name: group_name.clone(),
                worker_id,
                address: address.clone(),
                worker_net_protocol,
                fault_domain: None,
            };

            let accepted_worker_id = match self.raft_node.propose(command).await {
                Ok(ApplySuccess::WorkerUpserted(worker_id)) => worker_id,
                Ok(other) => {
                    return self.metadata_error_response(
                        &req.header,
                        register_worker_response_with_header,
                        MetadataError::Internal(format!("RegisterWorker returned unexpected Raft response: {other:?}")),
                    );
                }
                Err(error) => {
                    return self.metadata_error_response(&req.header, register_worker_response_with_header, error)
                }
            };
            if accepted_worker_id != worker_id {
                return self.metadata_error_response(
                    &req.header,
                    register_worker_response_with_header,
                    MetadataError::Internal(format!(
                        "RegisterWorker returned worker_id {}, expected {}",
                        accepted_worker_id.as_raw(),
                        worker_id.as_raw()
                    )),
                );
            }
            if let Err(error) = self.worker_manager.register_worker_run(
                &group_name,
                accepted_worker_id,
                address.clone(),
                worker_net_protocol,
                worker_run_id,
                None,
            ) {
                return self.metadata_error_response(&req.header, register_worker_response_with_header, error);
            }

            info!(
                target: "metadata.worker",
                op = "RegisterWorker",
                result = "accepted",
                error_code = "none",
                event = "worker_registered",
                group_name = %group_name,
                worker_id = accepted_worker_id.as_raw(),
                worker_run_id = %worker_run_id,
                endpoint = %address,
                protocol = worker_net_protocol_label(worker_net_protocol),
                "Worker registered"
            );

            Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                worker_id: accepted_worker_id.as_raw(),
                accepted_worker_run_id: worker_run_id.to_string(),
            }))
        }
        .await;
        Self::record_worker_rpc_outcome(
            "register_worker",
            MetadataWorkerMetric::Registration,
            started,
            &outcome,
            register_worker_response_header,
        );
        outcome
    }

    #[instrument(skip_all)]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let started = Instant::now();
        let transport_context = extract_trace_context(request.metadata());
        let outcome = async {
            let mut req = request.into_inner();
            merge_request_header_transport_context(&mut req.header, &transport_context);
            let _caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => return self.response_with_error(&req.header, error, heartbeat_response_with_header),
            };

            let group_name = match parse_worker_request_group_name(&req.header) {
                Ok(group_name) => group_name,
                Err(error) => return self.invalid_request_response(&req.header, heartbeat_response_with_header, error),
            };
            if group_name != self.served_group_name {
                return self.group_mismatch_response(
                    &req.header,
                    heartbeat_response_with_header,
                    format!(
                        "heartbeat group_name {} does not match served metadata group {}",
                        group_name, self.served_group_name
                    ),
                );
            }
            let worker_id = WorkerId::new(req.worker_id);
            if worker_id.as_raw() == 0 {
                return self.invalid_request_response(
                    &req.header,
                    heartbeat_response_with_header,
                    "worker_id must be non-zero",
                );
            }
            let worker_run_id = match require_worker_run_id(&req.worker_run_id, "HeartbeatRequest.worker_run_id") {
                Ok(worker_run_id) => worker_run_id,
                Err(error) => return self.invalid_request_response(&req.header, heartbeat_response_with_header, error),
            };

            let capacity = match req.capacity.as_ref() {
                Some(capacity) => capacity,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        heartbeat_response_with_header,
                        "Missing capacity",
                    )
                }
            };
            let tier_free = match parse_tier_free(&capacity.tier_free) {
                Ok(tier_free) => tier_free,
                Err(message) => {
                    return self.invalid_request_response(&req.header, heartbeat_response_with_header, message)
                }
            };

            let _load = match req.load {
                Some(load) => load,
                None => {
                    return self.invalid_request_response(&req.header, heartbeat_response_with_header, "Missing load")
                }
            };

            let _health_status = HealthStatus::from(req.health() as i32);
            let worker_net_protocol = WORKER_NET_PROTOCOL_GRPC;
            let endpoint = match req.advertised_endpoint {
                Some(endpoint) => endpoint,
                None => {
                    return self.invalid_request_response(
                        &req.header,
                        heartbeat_response_with_header,
                        "Missing advertised_endpoint",
                    );
                }
            };
            let advertised_endpoint = match validate_advertised_endpoint(endpoint) {
                Ok(address) => address,
                Err(message) => {
                    return self.invalid_request_response(&req.header, heartbeat_response_with_header, message)
                }
            };
            self.worker_manager.expire_liveness();

            let descriptor = match self.worker_manager.get_descriptor(&group_name, worker_id) {
                Some(descriptor) => descriptor,
                None => {
                    if self.worker_manager.mark_heartbeat_need_register_if_changed(
                        &group_name,
                        worker_id,
                        worker_run_id,
                    ) {
                        warn!(
                            target: "metadata.worker",
                            op = "Heartbeat",
                            result = "rejected",
                            error_code = "need_register",
                            group_name = %group_name,
                            worker_id = worker_id.as_raw(),
                            worker_run_id = %worker_run_id,
                            "Heartbeat rejected"
                        );
                    }
                    return self.need_register_response(
                        &req.header,
                        heartbeat_response_with_header,
                        format!(
                            "worker descriptor not found for group_name={}, worker_id={}",
                            group_name,
                            worker_id.as_raw()
                        ),
                    );
                }
            };
            let registration = match self.worker_manager.get_registration(&group_name, worker_id) {
                Some(registration) => registration,
                None => {
                    if self.worker_manager.mark_heartbeat_need_register_if_changed(
                        &group_name,
                        worker_id,
                        worker_run_id,
                    ) {
                        warn!(
                            target: "metadata.worker",
                            op = "Heartbeat",
                            result = "rejected",
                            error_code = "need_register",
                            group_name = %group_name,
                            worker_id = worker_id.as_raw(),
                            worker_run_id = %worker_run_id,
                            "Heartbeat rejected"
                        );
                    }
                    return self.need_register_response(
                        &req.header,
                        heartbeat_response_with_header,
                        format!(
                            "live worker registration not found for group_name={}, worker_id={}",
                            group_name,
                            worker_id.as_raw()
                        ),
                    );
                }
            };
            if !registration.worker_run_id.matches(worker_run_id) {
                if self
                    .worker_manager
                    .mark_heartbeat_run_mismatch_if_changed(&group_name, worker_id, worker_run_id)
                {
                    warn!(
                        target: "metadata.worker",
                        op = "Heartbeat",
                        result = "rejected",
                        error_code = "worker_run_mismatch",
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        expected_worker_run_id = %registration.worker_run_id,
                        "Heartbeat rejected"
                    );
                }
                return self.worker_run_mismatch_response(
                    &req.header,
                    heartbeat_response_with_header,
                    format!(
                        "worker_run_id mismatch for group_name={}, worker_id={}",
                        group_name,
                        worker_id.as_raw()
                    ),
                );
            }
            if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
                return self.worker_descriptor_mismatch_response(
                    &req.header,
                    heartbeat_response_with_header,
                    format!(
                        "advertised endpoint or protocol does not match registration for group_name={}, worker_id={}",
                        group_name,
                        worker_id.as_raw()
                    ),
                );
            }

            let live_state = match self.worker_manager.record_heartbeat_with_tier_free(
                &group_name,
                worker_id,
                worker_run_id,
                req.heartbeat_seq,
                &advertised_endpoint,
                worker_net_protocol,
                tier_free,
            ) {
                Ok(live_state) => live_state,
                Err(MetadataError::NotFound(message)) => {
                    if self.worker_manager.mark_heartbeat_need_register_if_changed(
                        &group_name,
                        worker_id,
                        worker_run_id,
                    ) {
                        warn!(
                            target: "metadata.worker",
                            op = "Heartbeat",
                            result = "rejected",
                            error_code = "need_register",
                            group_name = %group_name,
                            worker_id = worker_id.as_raw(),
                            worker_run_id = %worker_run_id,
                            "Heartbeat rejected"
                        );
                    }
                    return self.need_register_response(&req.header, heartbeat_response_with_header, message);
                }
                Err(MetadataError::StaleState(message)) => {
                    if self
                        .worker_manager
                        .mark_heartbeat_run_mismatch_if_changed(&group_name, worker_id, worker_run_id)
                    {
                        warn!(
                            target: "metadata.worker",
                            op = "Heartbeat",
                            result = "rejected",
                            error_code = "worker_run_mismatch",
                            group_name = %group_name,
                            worker_id = worker_id.as_raw(),
                            worker_run_id = %worker_run_id,
                            "Heartbeat rejected"
                        );
                    }
                    return self.worker_run_mismatch_response(&req.header, heartbeat_response_with_header, message);
                }
                Err(MetadataError::InvalidArgument(message)) => {
                    return self.worker_descriptor_mismatch_response(
                        &req.header,
                        heartbeat_response_with_header,
                        message,
                    );
                }
                Err(error) => return self.metadata_error_response(&req.header, heartbeat_response_with_header, error),
            };

            let live_count = self.worker_manager.list_live_workers().len();
            observe::set_worker_live(live_count);
            let now_ms = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(live_state.last_seen_ms);
            observe::record_worker_heartbeat_lag(now_ms.saturating_sub(live_state.last_seen_ms) as f64 / 1000.0);

            let cleanup_commands = self
                .cleanup
                .as_ref()
                .map(|cleanup| {
                    cleanup.commands_for_heartbeat(
                        &group_name,
                        live_state.worker_id,
                        live_state.worker_run_id,
                        Instant::now(),
                    )
                })
                .unwrap_or_default()
                .into_iter()
                .map(|command| BlockCleanupCommandProto {
                    block_id: Some(command.block_id.into()),
                })
                .collect();

            Ok(Response::new(HeartbeatResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                worker_id: live_state.worker_id.as_raw(),
                accepted_worker_run_id: live_state.worker_run_id.to_string(),
                liveness_timeout_ms: self.liveness_timeout_ms(),
                cleanup_commands,
            }))
        }
        .await;
        Self::record_worker_rpc_outcome(
            "heartbeat",
            MetadataWorkerMetric::Heartbeat,
            started,
            &outcome,
            heartbeat_response_header,
        );
        outcome
    }

    #[instrument(skip_all)]
    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let started = Instant::now();
        let metric_kind = block_report_kind(request.get_ref());
        let transport_context = extract_trace_context(request.metadata());
        let outcome = async {
            let mut req = request.into_inner();
            merge_request_header_transport_context(&mut req.header, &transport_context);
            let _caller_ctx = match extract_and_inject_context(&req.header) {
                Ok(ctx) => ctx,
                Err(error) => return self.response_with_error(&req.header, error, block_report_response_with_header),
            };

            let group_name = match parse_worker_request_group_name(&req.header) {
                Ok(group_name) => group_name,
                Err(error) => {
                    return self.invalid_request_response(&req.header, block_report_response_with_header, error)
                }
            };
            if group_name != self.served_group_name {
                return self.group_mismatch_response(
                    &req.header,
                    block_report_response_with_header,
                    format!(
                        "block report group_name {} does not match served metadata group {}",
                        group_name, self.served_group_name
                    ),
                );
            }
            let worker_id = WorkerId::new(req.worker_id);
            if worker_id.as_raw() == 0 {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "worker_id must be non-zero",
                );
            }
            let worker_run_id = match require_worker_run_id(&req.worker_run_id, "BlockReportRequest.worker_run_id") {
                Ok(worker_run_id) => worker_run_id,
                Err(error) => {
                    return self.invalid_request_response(&req.header, block_report_response_with_header, error)
                }
            };
            let baseline_seq = req.baseline_seq;
            if baseline_seq == 0 {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "block report baseline_seq must be non-zero",
                );
            }
            let Some(batch) = req.batch else {
                return self.invalid_request_response(
                    &req.header,
                    block_report_response_with_header,
                    "block report batch is required",
                );
            };

            let (report_kind, report_kind_proto, batch_seq, final_batch, apply_result) = match batch {
                Batch::FullReport(full) => {
                    let batch_seq = full.batch_seq;
                    let final_batch = full.final_batch;
                    if full.blocks.len() > MAX_REPORT_ENTRIES {
                        return self.metadata_error_response(
                            &req.header,
                            block_report_response_with_header,
                            MetadataError::ResourceExhausted(format!(
                                "full block report entry count {} exceeds maximum {}",
                                full.blocks.len(),
                                MAX_REPORT_ENTRIES
                            )),
                        );
                    }
                    let mut blocks = Vec::with_capacity(full.blocks.len());
                    for block in full.blocks {
                        match Self::proto_to_report_block(block) {
                            Ok(block) => blocks.push(block),
                            Err(error) => {
                                return self.metadata_error_response(
                                    &req.header,
                                    block_report_response_with_header,
                                    error,
                                );
                            }
                        }
                    }
                    let result = self.worker_manager.receive_full_block_report(
                        &group_name,
                        worker_id,
                        worker_run_id,
                        baseline_seq,
                        full.batch_seq,
                        full.final_batch,
                        blocks,
                    );
                    (
                        "full",
                        BlockReportKindProto::BlockReportKindFull,
                        batch_seq,
                        Some(final_batch),
                        result,
                    )
                }
                Batch::DeltaReport(delta) => {
                    let batch_seq = delta.batch_seq;
                    if delta.entries.is_empty() {
                        return self.invalid_request_response(
                            &req.header,
                            block_report_response_with_header,
                            "delta block report entries must be non-empty",
                        );
                    }
                    if delta.entries.len() > MAX_REPORT_ENTRIES {
                        return self.metadata_error_response(
                            &req.header,
                            block_report_response_with_header,
                            MetadataError::ResourceExhausted(format!(
                                "delta block report entry count {} exceeds maximum {}",
                                delta.entries.len(),
                                MAX_REPORT_ENTRIES
                            )),
                        );
                    }
                    let mut changes = Vec::with_capacity(delta.entries.len());
                    for entry in delta.entries {
                        match Self::proto_to_delta_entry(entry) {
                            Ok(change) => changes.push(change),
                            Err(error) => {
                                return self.metadata_error_response(
                                    &req.header,
                                    block_report_response_with_header,
                                    error,
                                );
                            }
                        }
                    }
                    let result = self.worker_manager.apply_delta_block_report(
                        &group_name,
                        worker_id,
                        worker_run_id,
                        baseline_seq,
                        batch_seq,
                        changes,
                    );
                    (
                        "delta",
                        BlockReportKindProto::BlockReportKindDelta,
                        batch_seq,
                        None,
                        result,
                    )
                }
            };

            let result = match apply_result {
                Ok(result) => result,
                Err(MetadataError::NotFound(message)) => {
                    warn!(
                        target: "metadata.worker",
                        op = "BlockReport",
                        result = "rejected",
                        error_code = "need_register",
                        report_kind,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        "Block report rejected"
                    );
                    return self.need_register_response(&req.header, block_report_response_with_header, message);
                }
                Err(MetadataError::StaleState(message)) => {
                    warn!(
                        target: "metadata.worker",
                        op = "BlockReport",
                        result = "rejected",
                        error_code = "worker_run_mismatch",
                        report_kind,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        "Block report rejected"
                    );
                    return self.worker_run_mismatch_response(&req.header, block_report_response_with_header, message);
                }
                Err(MetadataError::FullReportRequired(message)) => {
                    warn!(
                        target: "metadata.worker",
                        op = "BlockReport",
                        result = "rejected",
                        error_code = "full_report_required",
                        report_kind,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        "Block report rejected"
                    );
                    return self.full_report_required_response(&req.header, block_report_response_with_header, message);
                }
                Err(error) => {
                    return self.metadata_error_response(&req.header, block_report_response_with_header, error)
                }
            };

            observe::record_worker_block_report_blocks("added", result.added_blocks.len());
            observe::record_worker_block_report_blocks("removed", result.removed_blocks.len());

            let changed_block_count = result.added_blocks.len() + result.removed_blocks.len();
            if changed_block_count > 0 || result.baseline_published {
                if let Some(final_batch) = final_batch {
                    info!(
                        target: "metadata.block",
                        op = "FullBlockReport",
                        result = "processed",
                        error_code = "none",
                        report_kind,
                        client_id = %_caller_ctx.client.client_id.as_raw(),
                        call_id = %_caller_ctx.client.call_id,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        final_batch,
                        next_batch_seq = result.next_batch_seq,
                        added_blocks = result.added_blocks.len(),
                        removed_blocks = result.removed_blocks.len(),
                        changed_block_count,
                        "Full block report processed"
                    );
                } else {
                    info!(
                        target: "metadata.block",
                        op = "DeltaBlockReport",
                        result = "processed",
                        error_code = "none",
                        report_kind,
                        client_id = %_caller_ctx.client.client_id.as_raw(),
                        call_id = %_caller_ctx.client.call_id,
                        group_name = %group_name,
                        worker_id = worker_id.as_raw(),
                        worker_run_id = %worker_run_id,
                        baseline_seq,
                        batch_seq,
                        next_batch_seq = result.next_batch_seq,
                        added_blocks = result.added_blocks.len(),
                        removed_blocks = result.removed_blocks.len(),
                        changed_block_count,
                        "Delta block report processed"
                    );
                }
            }

            let baseline_published = match report_kind_proto {
                BlockReportKindProto::BlockReportKindFull => result.baseline_published,
                BlockReportKindProto::BlockReportKindDelta => true,
                BlockReportKindProto::BlockReportKindUnspecified => {
                    unreachable!("validated block report batch always has a concrete kind")
                }
            };
            Ok(Response::new(BlockReportResponseProto {
                header: Some(self.create_response_header_from_request(&req.header, Some(&group_name))),
                report_kind: report_kind_proto as i32,
                baseline_seq,
                next_batch_seq: result.next_batch_seq,
                baseline_published,
            }))
        }
        .await;
        Self::record_worker_rpc_outcome(
            "block_report",
            MetadataWorkerMetric::BlockReport(metric_kind),
            started,
            &outcome,
            block_report_response_header,
        );
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BlockCleanupConfig, RaftConfig};
    use crate::raft::{AppRaftStateMachine, RocksDBStorage};
    use crate::session_registry::SessionRegistry;
    use crate::MountTable;
    use ::beryl_common::error::rpc::RecoveryAction;
    use beryl_common::header::RequestHeader;
    use beryl_proto::common::{BlockIdProto, TierProto};
    use beryl_proto::convert::rpc_error_from_proto;
    use beryl_proto::metadata::{CapacityInfoProto, DeltaBlockReportBatchProto, HealthStatusProto, LoadInfoProto};
    use beryl_types::{BlockIndex, ClientId, InodeId, Tier, WorkerRunId};
    use std::time::Duration;
    use tempfile::TempDir;

    fn assert_error_kind(error: &ErrorDetailProto, expected_kind: ErrorKind) -> RpcErrorDetail {
        let rpc_error = rpc_error_from_proto(error);
        assert_eq!(rpc_error.kind, expected_kind, "{rpc_error:?}");
        rpc_error
    }

    fn assert_error_register_worker(error: &ErrorDetailProto, expected_kind: ErrorKind) -> RpcErrorDetail {
        let rpc_error = assert_error_kind(error, expected_kind);
        assert!(
            matches!(rpc_error.recovery, RecoveryAction::RegisterWorker),
            "{rpc_error:?}"
        );
        rpc_error
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    async fn leader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        leader_raft_with_storage(dir).await.0
    }

    async fn leader_raft_with_storage(dir: &TempDir) -> (Arc<AppRaftNode>, Arc<RocksDBStorage>) {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), state_machine, mount_table, &raft_config)
                .await
                .unwrap(),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();
        for _ in 0..100 {
            if raft_node.is_leader() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(raft_node.is_leader());
        (raft_node, storage)
    }

    async fn nonleader_raft(dir: &TempDir) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, state_machine, mount_table, &raft_config)
                .await
                .unwrap(),
        );
        assert!(!raft_node.is_leader());
        raft_node
    }

    fn block_proto(block_id: BlockId) -> BlockIdProto {
        block_id.into()
    }

    fn absent_delta_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        baseline_seq: u64,
        batch_seq: u64,
        block_id: BlockId,
    ) -> BlockReportRequestProto {
        BlockReportRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(72))),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            baseline_seq,
            batch: Some(Batch::DeltaReport(DeltaBlockReportBatchProto {
                batch_seq,
                entries: vec![DeltaBlockReportEntryProto {
                    block: Some(Block::Absent(block_proto(block_id))),
                }],
            })),
        }
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn second_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440001".parse().unwrap()
    }

    fn worker_run_id_for(group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
        let group_component = group_name
            .as_str()
            .bytes()
            .fold(0u64, |acc, byte| acc.saturating_add(u64::from(byte)));
        let suffix = group_component
            .saturating_mul(1_000_000)
            .saturating_add(worker_id.as_raw());
        format!("550e8400-e29b-41d4-a716-{suffix:012x}")
            .parse()
            .expect("valid test WorkerRunId")
    }

    fn record_heartbeat(worker_manager: &WorkerManager, group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
        let descriptor = worker_manager
            .get_descriptor(group_name, worker_id)
            .expect("worker descriptor should be registered");
        let worker_run_id = worker_manager
            .get_registration(group_name, worker_id)
            .map(|registration| registration.worker_run_id)
            .unwrap_or_else(|| {
                let worker_run_id = worker_run_id_for(group_name, worker_id);
                worker_manager
                    .register_worker_run(
                        group_name,
                        worker_id,
                        descriptor.address.clone(),
                        descriptor.worker_net_protocol,
                        worker_run_id,
                        descriptor.fault_domain.clone(),
                    )
                    .expect("worker run should register");
                worker_run_id
            });
        worker_manager
            .record_heartbeat_with_tier_free(
                group_name,
                worker_id,
                worker_run_id,
                1,
                &descriptor.address,
                descriptor.worker_net_protocol,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes: 500,
                }],
            )
            .expect("heartbeat should be accepted");
        worker_manager
            .upsert_descriptor(descriptor)
            .expect("descriptor should be restored");
        worker_run_id
    }

    fn heartbeat_request(
        group_name: GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        endpoint_port: u32,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(valid_request_header(&group_name, ClientId::new(73))),
            worker_id: worker_id.as_raw(),
            worker_run_id: worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(EndpointProto {
                host: "127.0.0.1".to_string(),
                port: endpoint_port,
            }),
            capacity: Some(CapacityInfoProto {
                total_bytes: 1_000,
                used_bytes: 100,
                available_bytes: 900,
                tier_free: vec![TierFreeProto {
                    tier: TierProto::TierHdd as i32,
                    free_bytes: 900,
                }],
            }),
            load: Some(LoadInfoProto {
                active_reads: 0,
                active_writes: 0,
            }),
            health: HealthStatusProto::HealthStatusHealthy as i32,
        }
    }

    fn valid_request_header(group_name: &GroupName, client_id: ClientId) -> RequestHeaderProto {
        (&RequestHeader::new(client_id).with_group_name(group_name.clone())).into()
    }

    #[tokio::test]
    async fn follower_block_report_updates_local_view() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let worker_id = WorkerId::new(8);
        let block_id = BlockId::from_u64_u32(80, 0);
        worker_manager
            .register_worker_run(
                &group_name("root"),
                worker_id,
                "127.0.0.1:9091".to_string(),
                1,
                worker_run_id_for(&group_name("root"), worker_id),
                None,
            )
            .unwrap();
        let worker_run_id = record_heartbeat(&worker_manager, &group_name("root"), worker_id);
        worker_manager
            .receive_full_block_report(
                &group_name("root"),
                worker_id,
                worker_run_id,
                3,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 100,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(raft_node, Arc::clone(&worker_manager), group_name("root"));

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::block_report(
            &service,
            Request::new(absent_delta_request(
                group_name("root"),
                worker_id,
                worker_run_id,
                3,
                0,
                block_id,
            )),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(response.report_kind(), BlockReportKindProto::BlockReportKindDelta);
        assert_eq!(response.baseline_seq, 3);
        assert_eq!(response.next_batch_seq, 1);
        assert!(response.baseline_published);
        assert!(worker_manager
            .get_block_locations(&group_name("root"), block_id)
            .is_empty());
    }

    #[tokio::test]
    async fn register_worker_publishes_live_run_only_after_raft_success() {
        let dir = TempDir::new().unwrap();
        let raft_node = leader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let worker_run_id = test_worker_run_id();
        let service = MetadataWorkerServiceImpl::new(raft_node, Arc::clone(&worker_manager), group_name("root"));

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::register_worker(
            &service,
            Request::new(RegisterWorkerRequestProto {
                header: Some(valid_request_header(&group_name("root"), ClientId::new(84))),
                worker_id: 124,
                worker_run_id: worker_run_id.to_string(),
                advertised_endpoint: Some(EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9091,
                }),
            }),
        )
        .await
        .expect("register worker response")
        .into_inner();

        assert!(response.header.as_ref().expect("header").error.is_none());
        assert_eq!(
            worker_manager
                .get_descriptor(&group_name("root"), WorkerId::new(124))
                .expect("published descriptor")
                .address,
            "127.0.0.1:9091"
        );
        assert_eq!(
            worker_manager
                .get_registration(&group_name("root"), WorkerId::new(124))
                .expect("published live run")
                .worker_run_id,
            worker_run_id
        );
    }

    #[tokio::test]
    async fn heartbeat_maps_registration_mismatches_to_recovery_headers() {
        let dir = TempDir::new().unwrap();
        let raft_node = nonleader_raft(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let group_name = group_name("root");
        worker_manager
            .register_worker_run(
                &group_name,
                WorkerId::new(11),
                "127.0.0.1:9090".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        worker_manager
            .register_worker_run(
                &group_name,
                WorkerId::new(9),
                "127.0.0.1:9099".to_string(),
                1,
                test_worker_run_id(),
                None,
            )
            .unwrap();
        let service = MetadataWorkerServiceImpl::new(raft_node, worker_manager, group_name.clone());

        let run_mismatch = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name.clone(),
                WorkerId::new(11),
                second_worker_run_id(),
                1,
                9090,
            )),
        )
        .await
        .expect("run mismatch returns gRPC OK")
        .into_inner();
        let error = run_mismatch.header.expect("header").error.expect("header error");
        assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::RunMismatch));

        let descriptor_mismatch = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(
                group_name,
                WorkerId::new(9),
                test_worker_run_id(),
                1,
                9098,
            )),
        )
        .await
        .expect("descriptor mismatch returns gRPC OK")
        .into_inner();
        let error = descriptor_mismatch.header.expect("header").error.expect("header error");
        assert_error_register_worker(&error, ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch));
    }

    #[tokio::test]
    async fn heartbeat_accepts_liveness_without_raft_propose_for_leader_and_follower() {
        for (worker_id, leader, heartbeat_seq) in [(WorkerId::new(12), false, 7), (WorkerId::new(13), true, 1)] {
            let dir = TempDir::new().unwrap();
            let raft_node = if leader {
                leader_raft(&dir).await
            } else {
                nonleader_raft(&dir).await
            };
            let before_state_id = raft_node.get_last_applied_state_id();
            let worker_manager = Arc::new(WorkerManager::new(1_500));
            worker_manager
                .register_worker_run(
                    &group_name("root"),
                    worker_id,
                    "127.0.0.1:9090".to_string(),
                    1,
                    test_worker_run_id(),
                    None,
                )
                .unwrap();
            let service =
                MetadataWorkerServiceImpl::new(Arc::clone(&raft_node), Arc::clone(&worker_manager), group_name("root"));

            let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
                &service,
                Request::new(heartbeat_request(
                    group_name("root"),
                    worker_id,
                    test_worker_run_id(),
                    heartbeat_seq,
                    9090,
                )),
            )
            .await
            .expect("heartbeat succeeds")
            .into_inner();

            assert!(response.header.as_ref().expect("header").error.is_none());
            assert_eq!(response.header.as_ref().expect("header").group_name, "root");
            assert_eq!(response.worker_id, worker_id.as_raw());
            assert_eq!(response.accepted_worker_run_id, test_worker_run_id().to_string());
            assert_eq!(response.liveness_timeout_ms, 1_500);
            assert!(worker_manager.is_worker_live(&group_name("root"), worker_id));
            if leader {
                assert_eq!(raft_node.get_last_applied_state_id(), before_state_id);
            }
        }
    }

    #[tokio::test]
    async fn heartbeat_returns_due_cleanup_command_for_the_accepted_ready_replica() {
        let dir = TempDir::new().unwrap();
        let (raft_node, storage) = leader_raft_with_storage(&dir).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let group = group_name("root");
        let worker_id = WorkerId::new(21);
        let worker_run_id = test_worker_run_id();
        worker_manager
            .register_worker_run(&group, worker_id, "127.0.0.1:9090".to_string(), 1, worker_run_id, None)
            .unwrap();

        let cleanup_config = BlockCleanupConfig {
            enabled: true,
            reclaim_grace_ms: 1,
            ..BlockCleanupConfig::default()
        };
        let cleanup = Arc::new(BlockCleanupCoordinator::new(
            Arc::clone(&raft_node),
            storage,
            Arc::clone(&worker_manager),
            Arc::new(SessionRegistry::default()),
            group.clone(),
            &cleanup_config,
        ));
        let service = MetadataWorkerServiceImpl::new_with_cleanup(
            raft_node,
            Arc::clone(&worker_manager),
            group.clone(),
            Arc::clone(&cleanup),
        );

        <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(group.clone(), worker_id, worker_run_id, 1, 9090)),
        )
        .await
        .expect("initial heartbeat succeeds");

        let block_id = BlockId::new(InodeId::new(700), BlockIndex::new(0));
        let lease_epoch = 991;
        worker_manager
            .receive_full_block_report(
                &group,
                worker_id,
                worker_run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        cleanup.scan_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        cleanup.scan_once().await.unwrap();

        let response = <MetadataWorkerServiceImpl as MetadataWorkerServiceProto>::heartbeat(
            &service,
            Request::new(heartbeat_request(group, worker_id, worker_run_id, 2, 9090)),
        )
        .await
        .expect("heartbeat succeeds")
        .into_inner();

        assert_eq!(response.cleanup_commands.len(), 1);
        let command = &response.cleanup_commands[0];
        assert_eq!(command.block_id, Some(block_id.into()));
    }
}
