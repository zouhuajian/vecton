// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-to-metadata full and incremental block reporting.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beryl_common::error::rpc::{ErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind};
use beryl_common::header::RequestHeader;
use beryl_proto::common::RequestHeaderProto;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{
    block_report_request_proto, delta_block_report_entry_proto, BlockReportKindProto, BlockReportRequestProto,
    BlockReportResponseProto, DeltaBlockReportBatchProto, DeltaBlockReportEntryProto, FullBlockReportBatchProto,
    ReportedBlockProto, ReportedBlockStateProto,
};
use beryl_types::{BlockId, GroupName, MAX_REPORT_ENTRIES};
use thiserror::Error;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;
use tonic::Code;
use tracing::{debug, warn};

use crate::config::WorkerRegistrationConfig;
use crate::control::{
    metadata_tonic_request, ControlIdentity, ControlOp, Registration, RegistrationDescriptor, RegistrationSet,
};
use crate::error::WorkerError;
use crate::observe;
use crate::report::DirtyBlock;
use crate::store::block::{BlockMetaPayload, BlockState};
use crate::store::dirs::StoreDirs;
use crate::WorkerCore;

/// Worker-side batching policy constrained by the shared report protocol cap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportOptions {
    /// Maximum block entries sent in one Full batch.
    pub full_max_blocks_per_batch: usize,
    /// Maximum changed identities sent in one Delta batch.
    pub delta_max_entries_per_batch: usize,
}

impl Default for BlockReportOptions {
    fn default() -> Self {
        Self {
            full_max_blocks_per_batch: MAX_REPORT_ENTRIES,
            delta_max_entries_per_batch: MAX_REPORT_ENTRIES,
        }
    }
}

/// Configuration, retryable transport, and fatal protocol failures from reporting.
#[derive(Debug, Error)]
pub enum BlockReportError {
    #[error("invalid worker block report config: {0}")]
    InvalidConfig(String),
    #[error("retryable metadata block report error: {0}")]
    Retryable(String),
    #[error("fatal metadata block report error: {0}")]
    Fatal(String),
}

/// Outcome summary for one Full or Delta submission attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockReportRound {
    pub attempted_peers: usize,
    pub accepted_peers: usize,
    pub full_report_required: bool,
    pub needs_register: bool,
    pub worker_run_mismatch: bool,
}

/// Immutable Full snapshot retained across every result-unknown retry.
#[derive(Debug)]
struct FullReportInFlight {
    registration_epoch: u64,
    baseline_seq: u64,
    store_snapshot_revision: u64,
    runtime_snapshot_revision: u64,
    blocks: Vec<ReportedBlockProto>,
    batch_ops: Vec<ControlOp>,
}

/// Dirty revisions covered by one immutable Delta entry.
#[derive(Clone, Copy, Debug)]
struct TrackedBlockChange {
    block_id: BlockId,
    store: Option<DirtyBlock>,
    runtime: Option<DirtyBlock>,
}

/// Immutable Delta request retained until Metadata acknowledges its sequence.
#[derive(Debug)]
struct DeltaReportInFlight {
    registration_epoch: u64,
    baseline_seq: u64,
    batch_seq: u64,
    op: ControlOp,
    entries: Vec<DeltaBlockReportEntryProto>,
    tracked: Vec<TrackedBlockChange>,
}

/// Worker-side synchronization state for one configured metadata group.
///
/// The state stores only report identity and one in-flight request. The local
/// stores remain physical authority, so a long-lived copy of the entire block
/// inventory is neither required nor allowed on the Delta path.
#[derive(Debug, Default)]
struct ReportRuntime {
    registration_epoch: u64,
    next_baseline_seq: u64,
    active_baseline_seq: Option<u64>,
    next_delta_batch_seq: u64,
    full_inflight: Option<Arc<FullReportInFlight>>,
    delta_inflight: Option<Arc<DeltaReportInFlight>>,
}

/// Sends Full reports only to establish or recover a baseline and uses retained
/// dirty identities for the steady-state Delta path.
///
/// The interval is a bounded flush and retry cadence. It never schedules a
/// periodic Full report while the current baseline remains continuous.
pub struct MetadataBlockReportLoop {
    config: WorkerRegistrationConfig,
    _descriptor: RegistrationDescriptor,
    state: Arc<RegistrationSet>,
    endpoint: Endpoint,
    store: Arc<StoreDirs>,
    core: Arc<WorkerCore>,
    options: BlockReportOptions,
    delta_flush_interval: Duration,
    control_identity: ControlIdentity,
    reports: Mutex<HashMap<GroupName, ReportRuntime>>,
}

impl MetadataBlockReportLoop {
    pub fn new(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        core: Arc<WorkerCore>,
    ) -> Result<Self, BlockReportError> {
        Self::with_options(config, descriptor, state, store, core, BlockReportOptions::default())
    }

    pub fn with_options(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        core: Arc<WorkerCore>,
        options: BlockReportOptions,
    ) -> Result<Self, BlockReportError> {
        Self::with_options_and_delta_flush_interval(
            config,
            descriptor,
            state,
            store,
            core,
            options,
            Duration::from_secs(1),
        )
    }

    /// Builds a reporter with an explicit retry and Delta flush cadence.
    pub fn with_options_and_delta_flush_interval(
        config: WorkerRegistrationConfig,
        descriptor: RegistrationDescriptor,
        state: Arc<RegistrationSet>,
        store: Arc<StoreDirs>,
        core: Arc<WorkerCore>,
        options: BlockReportOptions,
        delta_flush_interval: Duration,
    ) -> Result<Self, BlockReportError> {
        config
            .validate()
            .map_err(|err| BlockReportError::InvalidConfig(err.message))?;
        if delta_flush_interval.is_zero() {
            return Err(BlockReportError::InvalidConfig(
                "block report Delta flush interval must be greater than zero".to_string(),
            ));
        }
        validate_batch_limit("full_max_blocks_per_batch", options.full_max_blocks_per_batch)?;
        validate_batch_limit("delta_max_entries_per_batch", options.delta_max_entries_per_batch)?;

        let endpoint = Endpoint::from_shared(config.endpoints[0].clone())
            .map_err(|err| BlockReportError::InvalidConfig(format!("beryl.worker.metadata.addresses: {err}")))?;

        Ok(Self {
            config,
            _descriptor: descriptor,
            state,
            endpoint,
            store,
            core,
            options,
            delta_flush_interval,
            control_identity: ControlIdentity::new_local(),
            reports: Mutex::new(HashMap::new()),
        })
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        self.spawn_until_shutdown(CancellationToken::new())
    }

    /// Starts block reporting under the process shutdown token.
    pub fn spawn_until_shutdown(self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run(shutdown).await })
    }

    /// Returns whether the current registration owns an accepted Full baseline.
    pub fn has_delta_baseline(&self, group_name: &GroupName) -> bool {
        let Some((_, registration_epoch)) = self.state.ready_registration(group_name) else {
            return false;
        };
        self.reports
            .lock()
            .expect("block report state poisoned")
            .get(group_name)
            .is_some_and(|report| {
                report.registration_epoch == registration_epoch && report.active_baseline_seq.is_some()
            })
    }

    /// Sends or exactly retries one Full snapshot for the current registration.
    pub async fn send_full_once(&self) -> Result<BlockReportRound, BlockReportError> {
        let Some((registration, registration_epoch)) = self.ready_registration() else {
            return Ok(BlockReportRound::default());
        };
        let full = self.prepare_full_report(&registration.group_name, registration_epoch)?;
        let mut round = BlockReportRound {
            attempted_peers: 1,
            ..BlockReportRound::default()
        };
        let started = Instant::now();
        match self
            .send_full_to_peer(self.endpoint.clone(), &registration, &full)
            .await
        {
            Ok(BlockReportPeerOutcome::FullAccepted { .. }) => {
                let duration = started.elapsed().as_secs_f64();
                observe::record_metadata_rpc("block_report", "ok", "none", duration);
                observe::record_block_report_sent("full", "ok", "none", duration);
                round.accepted_peers = 1;
                self.accept_full_report(&registration.group_name, registration_epoch, full.baseline_seq);
            }
            Ok(outcome) => {
                self.record_structured_outcome(&registration.group_name, outcome, &mut round, "full", started);
            }
            Err(error) => {
                observe::record_metadata_rpc(
                    "block_report",
                    "error",
                    block_report_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                debug!(%error, "Worker full block report endpoint attempt failed");
                return Err(error);
            }
        }
        Ok(round)
    }

    /// Sends or exactly retries one bounded Delta batch for the current baseline.
    pub async fn send_delta_once(&self) -> Result<BlockReportRound, BlockReportError> {
        let Some((registration, registration_epoch)) = self.ready_registration() else {
            return Ok(BlockReportRound::default());
        };
        let delta = match self.prepare_delta_report(&registration.group_name, registration_epoch)? {
            DeltaPreparation::NoChanges => return Ok(BlockReportRound::default()),
            DeltaPreparation::FullRequired => {
                return Ok(BlockReportRound {
                    full_report_required: true,
                    ..BlockReportRound::default()
                });
            }
            DeltaPreparation::Ready(delta) => delta,
        };

        let mut round = BlockReportRound {
            attempted_peers: 1,
            ..BlockReportRound::default()
        };
        let started = Instant::now();
        match self
            .send_delta_to_peer(self.endpoint.clone(), &registration, &delta)
            .await
        {
            Ok(BlockReportPeerOutcome::DeltaAccepted { next_batch_seq }) => {
                let duration = started.elapsed().as_secs_f64();
                observe::record_metadata_rpc("block_report", "ok", "none", duration);
                observe::record_block_report_sent("delta", "ok", "none", duration);
                round.accepted_peers = 1;
                self.accept_delta_report(
                    &registration.group_name,
                    registration_epoch,
                    delta.batch_seq,
                    next_batch_seq,
                )?;
            }
            Ok(outcome) => {
                self.record_structured_outcome(&registration.group_name, outcome, &mut round, "delta", started);
            }
            Err(error) => {
                observe::record_metadata_rpc(
                    "block_report",
                    "error",
                    block_report_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                debug!(%error, "Worker delta block report endpoint attempt failed");
                return Err(error);
            }
        }
        Ok(round)
    }

    fn ready_registration(&self) -> Option<(Registration, u64)> {
        self.state.ready_registration(&self.config.group_name)
    }

    /// Builds a Full snapshot once and retains it unchanged until acknowledgement.
    fn prepare_full_report(
        &self,
        group_name: &GroupName,
        registration_epoch: u64,
    ) -> Result<Arc<FullReportInFlight>, BlockReportError> {
        let mut reports = self.reports.lock().expect("block report state poisoned");
        let report = reports.entry(group_name.clone()).or_default();
        bind_registration(report, registration_epoch);
        if let Some(full) = &report.full_inflight {
            return Ok(Arc::clone(full));
        }

        let store_snapshot_revision = self.store.block_report_changes().begin_full_snapshot(group_name);
        let runtime_snapshot_revision = self.core.block_report_changes().begin_full_snapshot(group_name);
        let blocks = self.scan_report_blocks()?;
        report.next_baseline_seq = report
            .next_baseline_seq
            .checked_add(1)
            .ok_or_else(|| BlockReportError::Fatal("block report baseline sequence overflow".to_string()))?;
        let batch_count = blocks.len().max(1).div_ceil(self.options.full_max_blocks_per_batch);
        let full = Arc::new(FullReportInFlight {
            registration_epoch,
            baseline_seq: report.next_baseline_seq,
            store_snapshot_revision,
            runtime_snapshot_revision,
            blocks,
            batch_ops: (0..batch_count).map(|_| self.control_identity.new_op()).collect(),
        });
        report.active_baseline_seq = None;
        report.next_delta_batch_seq = 0;
        report.delta_inflight = None;
        report.full_inflight = Some(Arc::clone(&full));
        Ok(full)
    }

    /// Builds one Delta from retained dirty identities without scanning inventory.
    fn prepare_delta_report(
        &self,
        group_name: &GroupName,
        registration_epoch: u64,
    ) -> Result<DeltaPreparation, BlockReportError> {
        let mut reports = self.reports.lock().expect("block report state poisoned");
        let report = reports.entry(group_name.clone()).or_default();
        bind_registration(report, registration_epoch);
        if let Some(delta) = &report.delta_inflight {
            return Ok(DeltaPreparation::Ready(Arc::clone(delta)));
        }
        let Some(baseline_seq) = report.active_baseline_seq else {
            return Ok(DeltaPreparation::FullRequired);
        };

        let store_dirty = match self.store.block_report_changes().snapshot(group_name) {
            Ok(dirty) => dirty,
            Err(()) => {
                reset_baseline(report);
                return Ok(DeltaPreparation::FullRequired);
            }
        };
        let runtime_dirty = match self.core.block_report_changes().snapshot(group_name) {
            Ok(dirty) => dirty,
            Err(()) => {
                reset_baseline(report);
                return Ok(DeltaPreparation::FullRequired);
            }
        };
        let tracked = merge_dirty_changes(store_dirty, runtime_dirty, self.options.delta_max_entries_per_batch);
        if tracked.is_empty() {
            return Ok(DeltaPreparation::NoChanges);
        }

        let mut entries = Vec::with_capacity(tracked.len());
        for entry in &tracked {
            entries.push(self.resolve_delta_entry(group_name, entry.block_id)?);
        }
        let delta = Arc::new(DeltaReportInFlight {
            registration_epoch,
            baseline_seq,
            batch_seq: report.next_delta_batch_seq,
            op: self.control_identity.new_op(),
            entries,
            tracked,
        });
        report.delta_inflight = Some(Arc::clone(&delta));
        Ok(DeltaPreparation::Ready(delta))
    }

    /// Resolves one dirty identity against the current store and reclaim fence.
    fn resolve_delta_entry(
        &self,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> Result<DeltaBlockReportEntryProto, BlockReportError> {
        if self.core.reclaiming_block(group_name, block_id).is_some() {
            return Ok(present_entry(ReportedBlockProto {
                block_id: Some(block_id.into()),
                lease_epoch: 0,
                tier: 0,
                state: ReportedBlockStateProto::ReportedBlockStateDeleting as i32,
                effective_len: 0,
            }));
        }
        match self.store.load_report_meta(group_name, block_id) {
            Ok(meta) => meta_to_report_block(meta).map(present_entry),
            Err(WorkerError::NotFound(_)) => Ok(DeltaBlockReportEntryProto {
                block: Some(delta_block_report_entry_proto::Block::Absent(block_id.into())),
            }),
            Err(error) => Err(BlockReportError::Retryable(format!(
                "load changed local block for report failed: {error}"
            ))),
        }
    }

    /// Builds the authoritative local view used only by Full recovery.
    fn scan_report_blocks(&self) -> Result<Vec<ReportedBlockProto>, BlockReportError> {
        let metas = self
            .store
            .scan_group_blocks(&self.config.group_name)
            .map_err(|err| BlockReportError::Retryable(format!("scan local block report group failed: {err}")))?;
        let mut blocks = HashMap::with_capacity(metas.len());
        for meta in metas {
            let block = meta_to_report_block(meta)?;
            let id = block_id(&block).expect("local block report entry has an id");
            blocks.insert(id, block);
        }
        for reclaiming in self.core.reclaiming_blocks(&self.config.group_name) {
            blocks.insert(
                reclaiming.block_id,
                ReportedBlockProto {
                    block_id: Some(reclaiming.block_id.into()),
                    lease_epoch: 0,
                    tier: 0,
                    state: ReportedBlockStateProto::ReportedBlockStateDeleting as i32,
                    effective_len: 0,
                },
            );
        }
        let mut blocks = blocks.into_values().collect::<Vec<_>>();
        blocks.sort_by_key(|block| block_id(block).expect("local block report entry has an id"));
        Ok(blocks)
    }

    /// Commits a Full acknowledgement only if it still names the in-flight snapshot.
    fn accept_full_report(&self, group_name: &GroupName, registration_epoch: u64, baseline_seq: u64) {
        let mut reports = self.reports.lock().expect("block report state poisoned");
        let Some(report) = reports.get_mut(group_name) else {
            return;
        };
        let Some(full) = report.full_inflight.as_ref() else {
            return;
        };
        if full.registration_epoch != registration_epoch || full.baseline_seq != baseline_seq {
            return;
        }
        let store_continuous = self
            .store
            .block_report_changes()
            .acknowledge_full(group_name, full.store_snapshot_revision);
        let runtime_continuous = self
            .core
            .block_report_changes()
            .acknowledge_full(group_name, full.runtime_snapshot_revision);
        report.full_inflight = None;
        report.delta_inflight = None;
        if store_continuous && runtime_continuous {
            report.active_baseline_seq = Some(baseline_seq);
            report.next_delta_batch_seq = 0;
        } else {
            reset_baseline(report);
        }
    }

    /// Advances Delta state only after the exact in-flight batch is acknowledged.
    fn accept_delta_report(
        &self,
        group_name: &GroupName,
        registration_epoch: u64,
        batch_seq: u64,
        next_batch_seq: u64,
    ) -> Result<(), BlockReportError> {
        let expected_next = batch_seq
            .checked_add(1)
            .ok_or_else(|| BlockReportError::Fatal("delta batch sequence overflow".to_string()))?;
        if next_batch_seq != expected_next {
            return Err(BlockReportError::Fatal(format!(
                "metadata acknowledged next delta batch {next_batch_seq}, expected {expected_next}"
            )));
        }
        let mut reports = self.reports.lock().expect("block report state poisoned");
        let Some(report) = reports.get_mut(group_name) else {
            return Ok(());
        };
        let Some(delta) = report.delta_inflight.as_ref() else {
            return Ok(());
        };
        if delta.registration_epoch != registration_epoch || delta.batch_seq != batch_seq {
            return Ok(());
        }

        let store_ack = delta.tracked.iter().filter_map(|entry| entry.store).collect::<Vec<_>>();
        let runtime_ack = delta
            .tracked
            .iter()
            .filter_map(|entry| entry.runtime)
            .collect::<Vec<_>>();
        self.store.block_report_changes().acknowledge(group_name, &store_ack);
        self.core.block_report_changes().acknowledge(group_name, &runtime_ack);
        report.delta_inflight = None;
        report.next_delta_batch_seq = next_batch_seq;
        Ok(())
    }

    fn reset_baseline(&self, group_name: &GroupName) {
        let mut reports = self.reports.lock().expect("block report state poisoned");
        if let Some(report) = reports.get_mut(group_name) {
            reset_baseline(report);
        }
    }

    fn record_structured_outcome(
        &self,
        group_name: &GroupName,
        outcome: BlockReportPeerOutcome,
        round: &mut BlockReportRound,
        report_kind: &'static str,
        started: Instant,
    ) {
        let error_kind = match outcome {
            BlockReportPeerOutcome::FullReportRequired => {
                round.full_report_required = true;
                self.reset_baseline(group_name);
                "full_report_required"
            }
            BlockReportPeerOutcome::NeedRegister => {
                round.needs_register = true;
                self.state.mark_needs_register(group_name);
                self.reset_baseline(group_name);
                "need_register"
            }
            BlockReportPeerOutcome::WorkerRunMismatch => {
                round.worker_run_mismatch = true;
                self.state.mark_needs_register(group_name);
                self.reset_baseline(group_name);
                "worker_run_mismatch"
            }
            BlockReportPeerOutcome::FullAccepted { .. } | BlockReportPeerOutcome::DeltaAccepted { .. } => {
                unreachable!("accepted outcomes are handled by the caller")
            }
        };
        observe::record_metadata_rpc("block_report", "error", error_kind, started.elapsed().as_secs_f64());
        observe::record_block_report_sent(report_kind, "error", error_kind, started.elapsed().as_secs_f64());
    }

    async fn send_full_to_peer(
        &self,
        endpoint: Endpoint,
        registration: &Registration,
        full: &FullReportInFlight,
    ) -> Result<BlockReportPeerOutcome, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, endpoint.connect())
            .await
            .map_err(|_| BlockReportError::Retryable("metadata block report connect timed out".to_string()))?
            .map_err(|err| BlockReportError::Retryable(format!("metadata block report endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);

        let batch_count = full.batch_ops.len();
        let mut batch_seq = 0usize;
        loop {
            let start = batch_seq
                .checked_mul(self.options.full_max_blocks_per_batch)
                .ok_or_else(|| BlockReportError::Fatal("full report batch offset overflow".to_string()))?;
            let end = start
                .saturating_add(self.options.full_max_blocks_per_batch)
                .min(full.blocks.len());
            let blocks = full.blocks.get(start..end).ok_or_else(|| {
                BlockReportError::Fatal("full report acknowledgement selected an invalid batch".to_string())
            })?;
            let final_batch = batch_seq + 1 == batch_count;
            let outcome = self
                .send_full_batch(&mut client, registration, full, batch_seq, blocks, final_batch)
                .await?;
            match outcome {
                BlockReportPeerOutcome::FullAccepted {
                    baseline_published: true,
                    ..
                } => return Ok(outcome),
                BlockReportPeerOutcome::FullAccepted {
                    next_batch_seq,
                    baseline_published: false,
                } => {
                    let next_batch_seq = usize::try_from(next_batch_seq).map_err(|_| {
                        BlockReportError::Fatal(
                            "metadata full report acknowledgement exceeds local batch range".to_string(),
                        )
                    })?;
                    if next_batch_seq <= batch_seq || next_batch_seq >= batch_count {
                        return Err(BlockReportError::Fatal(format!(
                            "metadata full report acknowledgement selected invalid next_batch_seq {next_batch_seq} after batch {batch_seq} of {batch_count}"
                        )));
                    }
                    batch_seq = next_batch_seq;
                }
                _ => return Ok(outcome),
            }
        }
    }

    async fn send_full_batch(
        &self,
        client: &mut MetadataWorkerServiceProtoClient<tonic::transport::Channel>,
        registration: &Registration,
        full: &FullReportInFlight,
        batch_seq: usize,
        blocks: &[ReportedBlockProto],
        final_batch: bool,
    ) -> Result<BlockReportPeerOutcome, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let op = full
            .batch_ops
            .get(batch_seq)
            .ok_or_else(|| BlockReportError::Fatal("full report batch identity is missing".to_string()))?;
        let request = BlockReportRequestProto {
            header: Some(block_report_request_header(&registration.group_name, op)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            baseline_seq: full.baseline_seq,
            batch: Some(block_report_request_proto::Batch::FullReport(
                FullBlockReportBatchProto {
                    batch_seq: u64::try_from(batch_seq)
                        .map_err(|_| BlockReportError::Fatal("full report batch index overflow".to_string()))?,
                    final_batch,
                    blocks: blocks.to_vec(),
                },
            )),
        };
        let tonic_request = metadata_tonic_request(request.clone(), request.header.as_ref());
        let response = time::timeout(timeout, client.block_report(tonic_request))
            .await
            .map_err(|_| BlockReportError::Retryable("metadata full block report timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_block_report_response(&request, response)
    }

    async fn send_delta_to_peer(
        &self,
        endpoint: Endpoint,
        registration: &Registration,
        delta: &DeltaReportInFlight,
    ) -> Result<BlockReportPeerOutcome, BlockReportError> {
        let timeout = Duration::from_millis(self.config.request_timeout_ms);
        let channel = time::timeout(timeout, endpoint.connect())
            .await
            .map_err(|_| BlockReportError::Retryable("metadata delta report connect timed out".to_string()))?
            .map_err(|err| BlockReportError::Retryable(format!("metadata delta report endpoint unavailable: {err}")))?;
        let mut client = MetadataWorkerServiceProtoClient::new(channel);
        let request = BlockReportRequestProto {
            header: Some(block_report_request_header(&registration.group_name, &delta.op)),
            worker_id: registration.worker_id.as_raw(),
            worker_run_id: registration.worker_run_id.to_string(),
            baseline_seq: delta.baseline_seq,
            batch: Some(block_report_request_proto::Batch::DeltaReport(
                DeltaBlockReportBatchProto {
                    batch_seq: delta.batch_seq,
                    entries: delta.entries.clone(),
                },
            )),
        };
        let tonic_request = metadata_tonic_request(request.clone(), request.header.as_ref());
        let response = time::timeout(timeout, client.block_report(tonic_request))
            .await
            .map_err(|_| BlockReportError::Retryable("metadata delta block report timed out".to_string()))?
            .map_err(classify_status)?
            .into_inner();
        classify_block_report_response(&request, response)
    }

    /// Flushes retained changes and retries in-flight requests until shutdown.
    async fn run(self, shutdown: CancellationToken) {
        let mut interval = time::interval(self.delta_flush_interval);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = interval.tick() => {}
                _ = self.store.wait_for_block_report_change() => {}
                _ = self.core.wait_for_block_report_change() => {}
            }
            let report = async {
                if self.has_delta_baseline(&self.config.group_name) {
                    match self.send_delta_once().await {
                        Ok(round) if round.full_report_required => {
                            if let Err(error) = self.send_full_once().await {
                                warn!(%error, "Worker full block report recovery failed");
                            }
                        }
                        Ok(_) => {}
                        Err(error) => warn!(%error, "Worker delta block report round failed"),
                    }
                } else if let Err(error) = self.send_full_once().await {
                    warn!(%error, "Worker full block report round failed");
                }
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = report => {}
            }
        }
    }
}

enum DeltaPreparation {
    NoChanges,
    FullRequired,
    Ready(Arc<DeltaReportInFlight>),
}

enum BlockReportPeerOutcome {
    FullAccepted {
        next_batch_seq: u64,
        baseline_published: bool,
    },
    DeltaAccepted {
        next_batch_seq: u64,
    },
    FullReportRequired,
    NeedRegister,
    WorkerRunMismatch,
}

fn validate_batch_limit(name: &str, value: usize) -> Result<(), BlockReportError> {
    if value == 0 {
        return Err(BlockReportError::InvalidConfig(format!(
            "{name} must be greater than zero"
        )));
    }
    if value > MAX_REPORT_ENTRIES {
        return Err(BlockReportError::InvalidConfig(format!(
            "{name} {value} exceeds maximum {MAX_REPORT_ENTRIES}"
        )));
    }
    Ok(())
}

/// Fences report identity and in-flight work to the current registration epoch.
fn bind_registration(report: &mut ReportRuntime, registration_epoch: u64) {
    if report.registration_epoch == registration_epoch {
        return;
    }
    report.registration_epoch = registration_epoch;
    reset_baseline(report);
}

/// Drops only synchronization state; retained dirty identities remain pending.
fn reset_baseline(report: &mut ReportRuntime) {
    report.active_baseline_seq = None;
    report.next_delta_batch_seq = 0;
    report.full_inflight = None;
    report.delta_inflight = None;
}

/// Coalesces store and reclaim changes by identity while preserving both revisions.
fn merge_dirty_changes(store: Vec<DirtyBlock>, runtime: Vec<DirtyBlock>, limit: usize) -> Vec<TrackedBlockChange> {
    let mut merged = BTreeMap::<BlockId, TrackedBlockChange>::new();
    for entry in store {
        merged
            .entry(entry.block_id)
            .or_insert(TrackedBlockChange {
                block_id: entry.block_id,
                store: None,
                runtime: None,
            })
            .store = Some(entry);
    }
    for entry in runtime {
        merged
            .entry(entry.block_id)
            .or_insert(TrackedBlockChange {
                block_id: entry.block_id,
                store: None,
                runtime: None,
            })
            .runtime = Some(entry);
    }
    merged.into_values().take(limit).collect()
}

fn present_entry(block: ReportedBlockProto) -> DeltaBlockReportEntryProto {
    DeltaBlockReportEntryProto {
        block: Some(delta_block_report_entry_proto::Block::Present(block)),
    }
}

fn meta_to_report_block(meta: BlockMetaPayload) -> Result<ReportedBlockProto, BlockReportError> {
    let block_state = match meta.visibility.block_state {
        BlockState::Ready => ReportedBlockStateProto::ReportedBlockStateReady,
        BlockState::Corrupt => ReportedBlockStateProto::ReportedBlockStateCorrupt,
        BlockState::Deleting => ReportedBlockStateProto::ReportedBlockStateDeleting,
    };
    let block_id = meta.identity.block_id;
    Ok(ReportedBlockProto {
        block_id: Some(block_id.into()),
        lease_epoch: meta.visibility.fencing_token.epoch.as_raw(),
        tier: beryl_proto::common::TierProto::from(meta.tier) as i32,
        state: block_state as i32,
        effective_len: meta.source.durable_len,
    })
}

fn block_id(block: &ReportedBlockProto) -> Option<BlockId> {
    block.block_id.map(|block_id| {
        BlockId::try_from(block_id).unwrap_or_else(|error| panic!("stored BlockId must be valid: {error}"))
    })
}

fn block_report_error_kind(error: &BlockReportError) -> &'static str {
    match error {
        BlockReportError::InvalidConfig(_) => "invalid_config",
        BlockReportError::Retryable(_) => "retryable",
        BlockReportError::Fatal(_) => "fatal",
    }
}

/// Validates that Metadata confirmed the exact report kind and baseline progress.
fn classify_block_report_response(
    request: &BlockReportRequestProto,
    response: BlockReportResponseProto,
) -> Result<BlockReportPeerOutcome, BlockReportError> {
    let response_group_name = response
        .header
        .as_ref()
        .map(|header| header.group_name.as_str())
        .ok_or_else(|| BlockReportError::Fatal("metadata block report response missing ResponseHeader".to_string()))?;
    let request_group_name = request
        .header
        .as_ref()
        .map(|header| header.group_name.as_str())
        .ok_or_else(|| BlockReportError::Fatal("metadata block report request missing RequestHeader".to_string()))?;
    if response_group_name != request_group_name {
        return Err(BlockReportError::Fatal(format!(
            "metadata block report response confirmed group_name {response_group_name}, expected {request_group_name}"
        )));
    }
    if let Some(outcome) = classify_header(response.header.as_ref())? {
        return Ok(outcome);
    }
    let report_kind = BlockReportKindProto::try_from(response.report_kind).map_err(|_| {
        BlockReportError::Fatal(format!(
            "metadata block report response returned unknown report_kind {}",
            response.report_kind
        ))
    })?;
    if response.baseline_seq != request.baseline_seq {
        return Err(BlockReportError::Fatal(format!(
            "metadata block report response confirmed baseline_seq {}, expected {}",
            response.baseline_seq, request.baseline_seq
        )));
    }
    match (request.batch.as_ref(), report_kind) {
        (Some(block_report_request_proto::Batch::FullReport(full)), BlockReportKindProto::BlockReportKindFull) => {
            let expected_next = full
                .batch_seq
                .checked_add(1)
                .ok_or_else(|| BlockReportError::Fatal("full report batch sequence overflow".to_string()))?;
            if response.baseline_published || (!full.final_batch && response.next_batch_seq >= expected_next) {
                Ok(BlockReportPeerOutcome::FullAccepted {
                    next_batch_seq: response.next_batch_seq,
                    baseline_published: response.baseline_published,
                })
            } else {
                Err(BlockReportError::Fatal(format!(
                    "metadata acknowledged full batch with next_batch_seq={} and baseline_published={}, expected next_batch_seq>={}{}",
                    response.next_batch_seq,
                    response.baseline_published,
                    expected_next,
                    if full.final_batch { " and a published baseline" } else { "" }
                )))
            }
        }
        (Some(block_report_request_proto::Batch::DeltaReport(_)), BlockReportKindProto::BlockReportKindDelta)
            if response.baseline_published =>
        {
            Ok(BlockReportPeerOutcome::DeltaAccepted {
                next_batch_seq: response.next_batch_seq,
            })
        }
        _ => Err(BlockReportError::Fatal(
            "metadata block report response did not confirm the requested report kind or a published Delta baseline"
                .to_string(),
        )),
    }
}

fn classify_header(
    header: Option<&beryl_proto::common::ResponseHeaderProto>,
) -> Result<Option<BlockReportPeerOutcome>, BlockReportError> {
    let header = header
        .ok_or_else(|| BlockReportError::Fatal("metadata block report response missing ResponseHeader".to_string()))?;
    let Some(error) = header.error.as_ref() else {
        return Ok(None);
    };
    classify_rpc_error(rpc_error_from_proto(error)).map(Some)
}

fn classify_rpc_error(error: RpcErrorDetail) -> Result<BlockReportPeerOutcome, BlockReportError> {
    match error.recovery {
        RecoveryAction::SendFullBlockReport => Ok(BlockReportPeerOutcome::FullReportRequired),
        RecoveryAction::RegisterWorker if error.kind == ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {
            Ok(BlockReportPeerOutcome::WorkerRunMismatch)
        }
        RecoveryAction::RegisterWorker => Ok(BlockReportPeerOutcome::NeedRegister),
        RecoveryAction::Retry { .. } | RecoveryAction::RefreshMetadata { .. } => {
            Err(BlockReportError::Retryable(error.message))
        }
        RecoveryAction::Fail | RecoveryAction::ReopenWriteSession { .. } => Err(BlockReportError::Fatal(format!(
            "fatal metadata block report error: {}",
            error.message
        ))),
    }
}

fn classify_status(status: tonic::Status) -> BlockReportError {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted => {
            BlockReportError::Retryable(status.to_string())
        }
        _ => BlockReportError::Fatal(format!("metadata block report RPC failed: {status}")),
    }
}

fn block_report_request_header(group_name: &GroupName, op: &ControlOp) -> RequestHeaderProto {
    let mut header = RequestHeader::new(op.client_id).with_group_name(group_name.clone());
    header.client.call_id = op.call_id;
    (&header).into()
}
