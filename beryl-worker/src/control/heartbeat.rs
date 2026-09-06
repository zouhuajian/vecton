// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-to-metadata heartbeat reporting.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beryl_common::error::rpc::{ErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind};
use beryl_common::header::RequestHeader;
use beryl_proto::common::{EndpointProto, RequestHeaderProto};
use beryl_proto::convert::{require_worker_run_id, required_block_id, rpc_error_from_proto};
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{
    CapacityInfoProto, HealthStatusProto, HeartbeatRequestProto, HeartbeatResponseProto, LoadInfoProto, TierFreeProto,
};
use beryl_types::{GroupName, TierFree, WorkerRunId};
use thiserror::Error;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, info, warn};

use crate::config::WorkerRegistrationConfig;
use crate::control::{
    metadata_tonic_request, BlockCleanupCommand, BlockCleanupExecutor, ControlIdentity, ControlOp, MetadataRegistrar,
    Registration, RegistrationDescriptor, RegistrationSet,
};
use crate::observe;
use crate::store::dirs::{StoreDirs, StoreReport};

/// Lightweight local resource snapshot sent on heartbeat.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatSnapshot {
    pub capacity_total_bytes: u64,
    pub capacity_used_bytes: u64,
    pub capacity_available_bytes: u64,
    pub tier_free: Vec<TierFree>,
    pub active_reads: u32,
    pub active_writes: u32,
}

#[derive(Debug, Error)]
pub enum HeartbeatError {
    #[error("invalid worker metadata heartbeat config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata heartbeat error: {0}")]
    Retryable(String),
    #[error("fatal metadata heartbeat error: {0}")]
    Fatal(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatRound {
    pub attempted_peers: usize,
    pub accepted_peers: usize,
    pub needs_register: bool,
    pub worker_run_mismatch: bool,
}

/// Heartbeat sender for one registered metadata group.
pub struct MetadataHeartbeatLoop {
    config: WorkerRegistrationConfig,
    descriptor: RegistrationDescriptor,
    state: Arc<RegistrationSet>,
    endpoint: Endpoint,
    control_identity: ControlIdentity,
    heartbeat_seq: Mutex<HashMap<(GroupName, WorkerRunId), u64>>,
    cleanup: BlockCleanupExecutor,
    interval: Duration,
}

impl MetadataHeartbeatLoop {
    /// Builds a heartbeat loop whose accepted cleanup commands use `cleanup`.
    ///
    /// Endpoint and registration configuration is validated before any
    /// background task or RPC is started.
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        cleanup: BlockCleanupExecutor,
    ) -> Result<Self, HeartbeatError> {
        Self::with_interval(config, descriptor, state, cleanup, Duration::from_secs(1))
    }

    pub fn with_interval(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        cleanup: BlockCleanupExecutor,
        interval: Duration,
    ) -> Result<Self, HeartbeatError> {
        if interval.is_zero() {
            return Err(HeartbeatError::InvalidConfig(
                "heartbeat interval must be greater than zero".to_string(),
            ));
        }
        config
            .validate()
            .map_err(|err| HeartbeatError::InvalidConfig(err.message))?;
        let endpoint = Endpoint::from_shared(config.endpoints[0].clone())
            .map_err(|err| HeartbeatError::InvalidConfig(format!("beryl.worker.metadata.addresses: {err}")))?;
        Ok(Self {
            config,
            descriptor,
            state,
            endpoint,
            control_identity: ControlIdentity::new_local(),
            heartbeat_seq: Mutex::new(HashMap::new()),
            cleanup,
            interval,
        })
    }

    pub fn spawn_with_registrar(self, registrar: Arc<MetadataRegistrar>) -> tokio::task::JoinHandle<()> {
        self.spawn_with_registrar_until_shutdown(registrar, CancellationToken::new())
    }

    /// Starts the heartbeat loop under the process shutdown token.
    pub fn spawn_with_registrar_until_shutdown(
        self,
        registrar: Arc<MetadataRegistrar>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(registrar, None, shutdown).await })
    }

    pub fn spawn_with_registrar_and_store(
        self,
        registrar: Arc<MetadataRegistrar>,
        store: Arc<StoreDirs>,
    ) -> tokio::task::JoinHandle<()> {
        self.spawn_with_registrar_and_store_until_shutdown(registrar, store, CancellationToken::new())
    }

    /// Starts store-backed heartbeat reporting under the process shutdown token.
    pub fn spawn_with_registrar_and_store_until_shutdown(
        self,
        registrar: Arc<MetadataRegistrar>,
        store: Arc<StoreDirs>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(registrar, Some(store), shutdown).await })
    }

    /// Sends one heartbeat round and enqueues commands from accepted responses.
    ///
    /// A response must confirm the requested group, worker, and worker run.
    /// Cleanup commands are parsed as one batch, so a malformed command rejects
    /// that peer response without partially enqueueing destructive work.
    pub async fn send_once(&self, snapshot: HeartbeatSnapshot) -> Result<HeartbeatRound, HeartbeatError> {
        let Some(registration) = self.state.registration(&self.config.group_name) else {
            return Ok(HeartbeatRound::default());
        };
        let seq = self.next_heartbeat_seq(&registration);
        let op = self.control_identity.new_op();
        let request = self.build_request(&registration, &op, seq, &snapshot);
        let mut round = HeartbeatRound {
            attempted_peers: 1,
            ..HeartbeatRound::default()
        };
        let started = Instant::now();
        match self.send_to_peer(self.endpoint.clone(), request).await {
            Ok(HeartbeatPeerOutcome::Accepted {
                liveness_timeout,
                cleanup_commands,
            }) => {
                let duration = started.elapsed().as_secs_f64();
                observe::record_metadata_rpc("heartbeat", "ok", "none", duration);
                observe::record_heartbeat_sent("ok", "none");
                round.accepted_peers = 1;
                self.state
                    .record_heartbeat_success(&registration.group_name, liveness_timeout);
                self.cleanup.enqueue(&registration, cleanup_commands);
            }
            Ok(HeartbeatPeerOutcome::NeedRegister) => {
                observe::record_metadata_rpc("heartbeat", "error", "need_register", started.elapsed().as_secs_f64());
                round.needs_register = true;
                self.state.mark_needs_register(&registration.group_name);
            }
            Ok(HeartbeatPeerOutcome::WorkerRunMismatch) => {
                observe::record_metadata_rpc(
                    "heartbeat",
                    "error",
                    "worker_run_mismatch",
                    started.elapsed().as_secs_f64(),
                );
                round.worker_run_mismatch = true;
                self.state.mark_needs_register(&registration.group_name);
            }
            Err(error) => {
                observe::record_metadata_rpc(
                    "heartbeat",
                    "error",
                    heartbeat_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                debug!(%error, "Worker heartbeat endpoint attempt failed");
                return Err(error);
            }
        }

        Ok(round)
    }

    fn build_request(
        &self,
        registration: &Registration,
        op: &ControlOp,
        heartbeat_seq: u64,
        snapshot: &HeartbeatSnapshot,
    ) -> HeartbeatRequestProto {
        HeartbeatRequestProto {
            header: Some(heartbeat_request_header(&registration.group_name, op)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            heartbeat_seq,
            advertised_endpoint: Some(EndpointProto {
                host: self.descriptor.endpoint_host.clone(),
                port: self.descriptor.endpoint_port,
            }),
            capacity: Some(CapacityInfoProto {
                total_bytes: snapshot.capacity_total_bytes,
                used_bytes: snapshot.capacity_used_bytes,
                available_bytes: snapshot.capacity_available_bytes,
                tier_free: snapshot
                    .tier_free
                    .iter()
                    .map(|entry| TierFreeProto {
                        tier: beryl_proto::common::TierProto::from(entry.tier) as i32,
                        free_bytes: entry.free_bytes,
                    })
                    .collect(),
            }),
            load: Some(LoadInfoProto {
                active_reads: snapshot.active_reads,
                active_writes: snapshot.active_writes,
            }),
            health: HealthStatusProto::HealthStatusHealthy as i32,
        }
    }

    fn next_heartbeat_seq(&self, registration: &Registration) -> u64 {
        let mut seqs = self.heartbeat_seq.lock().expect("heartbeat seq state poisoned");
        let entry = seqs
            .entry((registration.group_name.clone(), registration.worker_run_id))
            .or_insert(0);
        *entry = entry.saturating_add(1);
        *entry
    }

    async fn send_to_peer(
        &self,
        endpoint: Endpoint,
        request: HeartbeatRequestProto,
    ) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, endpoint.connect())
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat connect timed out".to_string()))?
            .map_err(|err| HeartbeatError::Retryable(format!("metadata heartbeat endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let tonic_request = metadata_tonic_request(request.clone(), request.header.as_ref());
        let response = time::timeout(timeout, client.heartbeat(tonic_request))
            .await
            .map_err(|_| HeartbeatError::Retryable("metadata heartbeat request timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_heartbeat_response(&request, response)
    }

    async fn run(self, registrar: Arc<MetadataRegistrar>, store: Option<Arc<StoreDirs>>, shutdown: CancellationToken) {
        let mut interval = time::interval(self.interval);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {}
            }
            if self.state.registration(&self.config.group_name).is_none() {
                match registrar.register_with_retry(shutdown.clone().cancelled_owned()).await {
                    Ok(registration) => {
                        info!(
                            group_name = %registration.group_name,
                            worker_id = registration.worker_id.as_raw(),
                            worker_run_id = %registration.worker_run_id,
                            "Worker re-registered after heartbeat requested registration"
                        );
                    }
                    Err(error) => {
                        if shutdown.is_cancelled() {
                            return;
                        }
                        warn!(%error, "Worker metadata re-registration failed in heartbeat loop");
                        continue;
                    }
                }
            }

            let snapshot = match store.as_ref() {
                Some(store) => match store.report() {
                    Ok(report) => {
                        observe::record_store_report(&report);
                        HeartbeatSnapshot::from(report)
                    }
                    Err(error) => {
                        warn!(%error, "Worker store report failed before heartbeat");
                        continue;
                    }
                },
                None => HeartbeatSnapshot::default(),
            };

            let heartbeat = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                result = self.send_once(snapshot) => result,
            };
            match heartbeat {
                Ok(round) if round.needs_register => {
                    warn!("Metadata heartbeat requested worker registration");
                }
                Ok(round) if round.worker_run_mismatch => {
                    warn!("Metadata heartbeat reported worker_run_id mismatch");
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "Worker heartbeat round failed"),
            }
        }
    }
}

impl From<StoreReport> for HeartbeatSnapshot {
    fn from(report: StoreReport) -> Self {
        Self {
            capacity_total_bytes: report.total_bytes,
            capacity_used_bytes: report.used_bytes,
            capacity_available_bytes: report.free_bytes,
            tier_free: report.tier_free,
            ..Self::default()
        }
    }
}

enum HeartbeatPeerOutcome {
    Accepted {
        liveness_timeout: Duration,
        cleanup_commands: Vec<BlockCleanupCommand>,
    },
    NeedRegister,
    WorkerRunMismatch,
}

fn heartbeat_error_kind(error: &HeartbeatError) -> &'static str {
    match error {
        HeartbeatError::InvalidConfig(_) => "invalid_config",
        HeartbeatError::Retryable(_) => "retryable",
        HeartbeatError::Fatal(_) => "fatal",
    }
}

/// Authenticates heartbeat response identity and decodes its command batch.
///
/// All commands are validated before an accepted outcome is returned. Callers
/// therefore never execute a valid prefix from an otherwise malformed batch.
fn classify_heartbeat_response(
    request: &HeartbeatRequestProto,
    response: HeartbeatResponseProto,
) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
    let response_group_name = response
        .header
        .as_ref()
        .map(|header| header.group_name.as_str())
        .ok_or_else(|| HeartbeatError::Fatal("metadata heartbeat response missing ResponseHeader".to_string()))?;
    let request_group_name = request
        .header
        .as_ref()
        .map(|header| header.group_name.as_str())
        .ok_or_else(|| HeartbeatError::Fatal("metadata heartbeat request missing RequestHeader".to_string()))?;
    if response_group_name != request_group_name {
        return Err(HeartbeatError::Fatal(format!(
            "metadata heartbeat response confirmed group_name {response_group_name}, expected {request_group_name}"
        )));
    }
    if let Some(outcome) = classify_header(response.header.as_ref())? {
        return Ok(outcome);
    }
    if response.worker_id != request.worker_id {
        return Err(HeartbeatError::Fatal(
            "metadata heartbeat response did not confirm worker_id".to_string(),
        ));
    }
    let accepted_worker_run_id = require_worker_run_id(
        &response.accepted_worker_run_id,
        "HeartbeatResponse.accepted_worker_run_id",
    )
    .map_err(HeartbeatError::Fatal)?;
    let expected_worker_run_id = require_worker_run_id(&request.worker_run_id, "HeartbeatRequest.worker_run_id")
        .map_err(HeartbeatError::Fatal)?;
    if !accepted_worker_run_id.matches(expected_worker_run_id) {
        return Err(HeartbeatError::Fatal(
            "metadata heartbeat response did not confirm worker_run_id".to_string(),
        ));
    }
    let cleanup_commands = response
        .cleanup_commands
        .into_iter()
        .map(|command| {
            let block_id = required_block_id(command.block_id, "HeartbeatResponse.cleanup_commands.block_id")
                .map_err(HeartbeatError::Fatal)?;
            if block_id.inode_id.as_raw() == 0 {
                return Err(HeartbeatError::Fatal(
                    "HeartbeatResponse.cleanup_commands.block_id.inode_id must be non-zero".to_string(),
                ));
            }
            Ok(BlockCleanupCommand { block_id })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let liveness_timeout = Duration::from_millis(u64::from(response.liveness_timeout_ms.max(1)));
    Ok(HeartbeatPeerOutcome::Accepted {
        liveness_timeout,
        cleanup_commands,
    })
}

fn classify_header(
    header: Option<&beryl_proto::common::ResponseHeaderProto>,
) -> Result<Option<HeartbeatPeerOutcome>, HeartbeatError> {
    let header = header
        .ok_or_else(|| HeartbeatError::Fatal("metadata heartbeat response missing ResponseHeader".to_string()))?;
    let Some(error) = header.error.as_ref() else {
        return Ok(None);
    };
    classify_rpc_error(rpc_error_from_proto(error)).map(Some)
}

fn classify_rpc_error(error: RpcErrorDetail) -> Result<HeartbeatPeerOutcome, HeartbeatError> {
    match error.recovery {
        RecoveryAction::RegisterWorker if error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {
            Ok(HeartbeatPeerOutcome::WorkerRunMismatch)
        }
        RecoveryAction::RegisterWorker => Ok(HeartbeatPeerOutcome::NeedRegister),
        RecoveryAction::Retry { .. } | RecoveryAction::RefreshMetadata { .. } | RecoveryAction::SendFullBlockReport => {
            Err(HeartbeatError::Retryable(error.message))
        }
        RecoveryAction::Fail | RecoveryAction::ReopenWriteSession { .. } => Err(HeartbeatError::Fatal(format!(
            "fatal metadata heartbeat error: {}",
            error.message
        ))),
    }
}

fn classify_status(status: tonic::Status) -> HeartbeatError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            HeartbeatError::Retryable(status.to_string())
        }
        _ => HeartbeatError::Fatal(format!("metadata heartbeat RPC failed: {status}")),
    }
}

fn heartbeat_request_header(group_name: &GroupName, op: &ControlOp) -> RequestHeaderProto {
    let mut header = RequestHeader::new(op.client_id).with_group_name(group_name.clone());
    header.client.call_id = op.call_id;
    (&header).into()
}
