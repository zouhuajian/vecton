// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker manager: tracks worker registration, heartbeat liveness, and block report locations.

use crate::error::{MetadataError, MetadataResult};
use crate::placement::{ReportedBlockLocation, WorkerPlacementView};
use beryl_types::ids::{BlockId, WorkerId};
use beryl_types::layout::BlockFormatId;
use beryl_types::{GroupName, LocatedBlock, TierFree, WorkerNetProtocol, WorkerRunId};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};

pub(super) const WORKER_NET_PROTOCOL_GRPC: i32 = 1;

pub(super) fn worker_net_protocol_label(worker_net_protocol: i32) -> &'static str {
    if worker_net_protocol == WORKER_NET_PROTOCOL_GRPC {
        "grpc"
    } else {
        "unknown"
    }
}

/// Worker descriptor (low-frequency, authoritative, persisted in Raft).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerDescriptor {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub address: String,
    /// Worker network protocol wire value. Current runtime accepts gRPC only.
    pub worker_net_protocol: i32,
    pub fault_domain: Option<String>,
}

/// Worker runtime (high-frequency, soft-state, memory-only with TTL).
#[derive(Clone, Debug)]
pub struct WorkerRuntime {
    pub worker_run_id: WorkerRunId,
    pub heartbeat_seq: u64,
    pub last_seen_at: Instant,
    pub last_seen_ms: u64, // Unix timestamp in milliseconds
    pub tier_free: Vec<TierFree>,
}

/// Worker information persisted by RocksDB storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub address: String,
    /// Worker network protocol wire value. Current runtime accepts gRPC only.
    pub worker_net_protocol: i32,
    pub capacity_total: u64,
    pub capacity_used: u64,
    pub capacity_available: u64,
    pub active_reads: u32,
    pub active_writes: u32,
    pub health: HealthStatus,
    pub last_heartbeat: u64, // Unix timestamp in seconds
    pub fault_domain: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

impl From<i32> for HealthStatus {
    fn from(v: i32) -> Self {
        match v {
            1 => HealthStatus::Healthy,
            2 => HealthStatus::Degraded,
            3 => HealthStatus::Unhealthy,
            _ => HealthStatus::Healthy,
        }
    }
}

/// Block locations keyed by metadata group and block identity.
pub type BlockLocations = HashMap<BlockLocationKey, BTreeSet<WorkerRegistrationKey>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockLocationKey {
    pub group_name: GroupName,
    pub block_id: BlockId,
}

impl BlockLocationKey {
    pub fn new(group_name: &GroupName, block_id: BlockId) -> Self {
        Self {
            group_name: group_name.clone(),
            block_id,
        }
    }
}

/// Group-scoped key for worker registration and liveness state.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerRegistrationKey {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
}

impl WorkerRegistrationKey {
    pub fn new(group_name: &GroupName, worker_id: WorkerId) -> Self {
        Self {
            group_name: group_name.clone(),
            worker_id,
        }
    }
}

/// Exact identity of one ready physical replica reported by a worker run.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaKey {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub block_id: BlockId,
}

/// Stable exclusive position after one worker or one Ready block.
///
/// `block_id = None` means this worker is fully consumed and the next page
/// starts from its successor. `Some(block_id)` resumes strictly after that
/// block and preserves the inclusive block high watermark captured when this
/// worker was first entered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadyReplicaCursor {
    /// Last worker visited in group-scoped worker-key order.
    pub worker_id: WorkerId,
    /// Last Ready block visited for that worker, if one was emitted.
    pub block_id: Option<BlockId>,
    /// Inclusive block upper bound captured on first entry into this worker.
    pub worker_end_block_id: Option<BlockId>,
}

/// One bounded page from the current published Ready reports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadyReplicaPage {
    /// Exact current-run Ready replicas encountered in this page.
    pub replicas: Vec<ReplicaKey>,
    /// Exclusive continuation position, or `None` when the traversal reached EOF.
    pub next_cursor: Option<ReadyReplicaCursor>,
}

/// Live startup registration state for the current metadata process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRegistrationState {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub address: String,
    pub worker_net_protocol: i32,
    pub fault_domain: Option<String>,
}

/// Worker liveness view updated only by group-scoped heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLiveState {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub heartbeat_seq: u64,
    pub last_seen_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeartbeatRejectionReason {
    NeedRegister,
    WorkerRunMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HeartbeatRejectionState {
    worker_run_id: WorkerRunId,
    reason: HeartbeatRejectionReason,
}

/// Metadata's accepted worker-local lifecycle state for one block version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockReportBlockState {
    Ready,
    Corrupt,
    Deleting,
}

/// Worker-reported block-location entry.
///
/// The entry is block-level only. Chunk presence and range routing are not part
/// of this report view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockReportBlock {
    /// Persisted replica tier, required for Ready checkpoints.
    pub tier: Option<beryl_types::Tier>,
    pub block_id: BlockId,
    pub lease_epoch: u64,
    pub block_state: BlockReportBlockState,
    /// Worker-persisted valid byte length. Ready reports must carry a non-zero value.
    pub effective_len: u64,
}

/// Latest reportable state for one block in an ordered Delta batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockReportChange {
    Upsert(BlockReportBlock),
    Remove(BlockId),
}

/// Observable index changes and acknowledgement state from one accepted batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockReportApplyResult {
    pub added_blocks: Vec<BlockId>,
    pub removed_blocks: Vec<BlockId>,
    pub next_batch_seq: u64,
    pub baseline_published: bool,
}

/// Metadata-visible baseline accepted from one exact Worker process run.
#[derive(Clone, Debug)]
struct ActiveBlockReport {
    baseline_seq: u64,
    blocks: HashMap<BlockId, BlockReportBlock>,
    /// Ordered Ready identities used by cleanup pagination and index removal.
    ready_blocks: BTreeSet<BlockId>,
    next_delta_batch_seq: u64,
}

/// Incomplete Full report that is never visible through location lookups.
#[derive(Clone, Debug)]
struct StagingFullBlockReport {
    baseline_seq: u64,
    next_batch_seq: u64,
    blocks: HashMap<BlockId, BlockReportBlock>,
}

/// Full/Delta soft state for one registered Worker run.
#[derive(Clone, Debug)]
struct WorkerBlockReportRuntime {
    worker_run_id: WorkerRunId,
    /// Greatest Full baseline ever started for this run. Unlike `active`, this
    /// survives continuity loss so a delayed Full cannot republish stale data.
    baseline_high_watermark: Option<u64>,
    active: Option<ActiveBlockReport>,
    staging: Option<StagingFullBlockReport>,
}

/// Atomically couples published reports with their derived reverse index.
#[derive(Debug, Default)]
struct BlockReportObservationState {
    reports: BTreeMap<WorkerRegistrationKey, WorkerBlockReportRuntime>,
    locations: BlockLocations,
}

/// Result of checking whether one publication batch has readable worker evidence.
///
/// `Pending` is reserved for observations that may still converge without
/// replacing the active write session. Deterministic identity or local block
/// state conflicts are returned separately so publication can fail closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishReadyStatus {
    Ready,
    Pending { block_id: BlockId },
    Conflict(PublishReadyConflict),
}

/// One metadata-issued target paired with the exact length requested for publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PublishReadyTarget {
    pub(crate) target: LocatedBlock,
    pub(crate) effective_len: u64,
}

/// Deterministic worker evidence that cannot authorize file publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishReadyConflict {
    MissingWriteEndpoint {
        block_id: BlockId,
    },
    WorkerRunMismatch {
        block_id: BlockId,
        worker_id: WorkerId,
        expected: WorkerRunId,
        current: Option<WorkerRunId>,
    },
    EndpointMismatch {
        block_id: BlockId,
        worker_id: WorkerId,
    },
    LeaseEpochMismatch {
        block_id: BlockId,
        worker_id: WorkerId,
        expected: u64,
        reported: u64,
    },
    UnreadableBlock {
        block_id: BlockId,
        worker_id: WorkerId,
        state: BlockReportBlockState,
    },
}

fn ready_block_ids<'a>(blocks: impl Iterator<Item = &'a BlockReportBlock>) -> BTreeSet<BlockId> {
    blocks
        .filter(|block| block.block_state == BlockReportBlockState::Ready)
        .map(|block| block.block_id)
        .collect()
}

/// Adds only the supplied Ready identities to the reverse location index.
fn add_worker_ready_locations(
    locations: &mut BlockLocations,
    key: &WorkerRegistrationKey,
    block_ids: impl Iterator<Item = BlockId>,
) {
    for block_id in block_ids {
        locations
            .entry(BlockLocationKey::new(&key.group_name, block_id))
            .or_default()
            .insert(key.clone());
    }
}

/// Removes only the supplied Ready identities without scanning global locations.
fn remove_worker_ready_locations(
    locations: &mut BlockLocations,
    key: &WorkerRegistrationKey,
    block_ids: impl Iterator<Item = BlockId>,
) {
    for block_id in block_ids {
        let location_key = BlockLocationKey::new(&key.group_name, block_id);
        let remove_entry = locations.get_mut(&location_key).is_some_and(|workers| {
            workers.remove(key);
            workers.is_empty()
        });
        if remove_entry {
            locations.remove(&location_key);
        }
    }
}

/// Removes one Worker report and every index entry derived from its active baseline.
fn remove_worker_report(
    observations: &mut BlockReportObservationState,
    key: &WorkerRegistrationKey,
) -> Option<WorkerBlockReportRuntime> {
    let report = observations.reports.remove(key)?;
    if let Some(active) = &report.active {
        remove_worker_ready_locations(&mut observations.locations, key, active.ready_blocks.iter().copied());
    }
    Some(report)
}

fn validate_same_run_descriptor(
    group_name: &GroupName,
    worker_id: WorkerId,
    existing: &WorkerRegistrationState,
    address: &str,
    worker_net_protocol: i32,
) -> MetadataResult<()> {
    if existing.address == address && existing.worker_net_protocol == worker_net_protocol {
        return Ok(());
    }
    Err(MetadataError::InvalidArgument(format!(
        "worker descriptor mismatch for group_name={}, worker_id={}, worker_run_id={}: registered endpoint {} protocol {}, requested endpoint {} protocol {}",
        group_name,
        worker_id.as_raw(),
        existing.worker_run_id,
        existing.address,
        worker_net_protocol_label(existing.worker_net_protocol),
        address,
        worker_net_protocol_label(worker_net_protocol)
    )))
}

fn endpoint_host(endpoint: &str) -> Option<String> {
    let without_scheme = endpoint.rsplit_once("://").map(|(_, rest)| rest).unwrap_or(endpoint);
    let host = without_scheme
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(without_scheme)
        .trim_matches(['[', ']'])
        .trim();
    (!host.is_empty()).then(|| host.to_string())
}

/// Worker manager.
pub struct WorkerManager {
    /// Worker descriptors (authoritative, from Raft state).
    descriptors: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerDescriptor>>>,
    /// Accepted worker process runs for this metadata process, learned through Raft apply.
    registrations: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerRegistrationState>>>,
    /// Worker runtime (soft-state, memory-only, updated by heartbeat).
    runtime: Arc<RwLock<HashMap<WorkerRegistrationKey, WorkerRuntime>>>,
    /// Last heartbeat rejection state per worker, used only to suppress repeated unchanged warn logs.
    heartbeat_rejections: Arc<RwLock<HashMap<WorkerRegistrationKey, HeartbeatRejectionState>>>,
    /// Worker reports and their derived location index, published atomically.
    block_report_observations: Arc<RwLock<BlockReportObservationState>>,
    /// Coalesced revision for publication-relevant worker observations.
    ///
    /// Ready evidence is leader-local and reconstructable. The revision only
    /// wakes waiters so they can rebuild and revalidate a complete snapshot.
    publication_observation: Sender<u64>,
    /// Heartbeat timeout shared by RPC responses and all soft-state checks.
    heartbeat_timeout_ms: u32,
}

impl WorkerManager {
    pub fn new(heartbeat_timeout_ms: u32) -> Self {
        let (publication_observation, _) = watch::channel(0);
        Self {
            descriptors: Arc::new(RwLock::new(HashMap::new())),
            registrations: Arc::new(RwLock::new(HashMap::new())),
            runtime: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_rejections: Arc::new(RwLock::new(HashMap::new())),
            block_report_observations: Arc::new(RwLock::new(BlockReportObservationState::default())),
            publication_observation,
            heartbeat_timeout_ms,
        }
    }

    fn notify_publication_observation_changed(&self) {
        self.publication_observation
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    /// Returns the exact timeout carried by heartbeat responses.
    pub fn heartbeat_timeout_ms(&self) -> u32 {
        self.heartbeat_timeout_ms
    }

    fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.heartbeat_timeout_ms))
    }

    /// Drops live registration and reconstructable report state on metadata restart.
    pub fn reset_worker_soft_state(&self) {
        let mut registrations = self.registrations.write();
        let mut observations = self.block_report_observations.write();
        registrations.clear();
        observations.reports.clear();
        observations.locations.clear();
        drop(observations);
        drop(registrations);
        self.runtime.write().clear();
        self.heartbeat_rejections.write().clear();
        self.notify_publication_observation_changed();
    }

    /// Upsert worker descriptor (called from Raft apply).
    pub fn upsert_descriptor(&self, descriptor: WorkerDescriptor) -> MetadataResult<()> {
        let mut descriptors = self.descriptors.write();
        descriptors.insert(
            WorkerRegistrationKey::new(&descriptor.group_name, descriptor.worker_id),
            descriptor,
        );
        drop(descriptors);
        self.notify_publication_observation_changed();
        Ok(())
    }

    /// Load persisted descriptors from replicated storage.
    ///
    /// WorkerRunId is intentionally not reconstructed here. Startup
    /// registration state is live-only, so reload/snapshot recovery fails closed
    /// until the worker registers again through Raft apply.
    pub fn load_registered_workers(&self, workers: Vec<WorkerInfo>) -> MetadataResult<()> {
        let mut descriptors = self.descriptors.write();
        let mut registrations = self.registrations.write();
        let mut runtime = self.runtime.write();
        let mut heartbeat_rejections = self.heartbeat_rejections.write();
        let mut observations = self.block_report_observations.write();
        descriptors.clear();
        registrations.clear();
        observations.reports.clear();
        observations.locations.clear();
        runtime.clear();
        heartbeat_rejections.clear();
        for worker in workers {
            let descriptor = WorkerDescriptor {
                group_name: worker.group_name,
                worker_id: worker.worker_id,
                address: worker.address,
                worker_net_protocol: worker.worker_net_protocol,
                fault_domain: worker.fault_domain,
            };
            descriptors.insert(
                WorkerRegistrationKey::new(&descriptor.group_name, descriptor.worker_id),
                descriptor,
            );
        }
        drop(observations);
        drop(heartbeat_rejections);
        drop(runtime);
        drop(registrations);
        drop(descriptors);
        self.notify_publication_observation_changed();
        Ok(())
    }

    /// Get a worker descriptor scoped to one metadata group.
    pub fn get_descriptor(&self, group_name: &GroupName, worker_id: WorkerId) -> Option<WorkerDescriptor> {
        let descriptors = self.descriptors.read();
        descriptors
            .get(&WorkerRegistrationKey::new(group_name, worker_id))
            .cloned()
    }

    /// Get live startup registration state scoped to one metadata group.
    pub fn get_registration(&self, group_name: &GroupName, worker_id: WorkerId) -> Option<WorkerRegistrationState> {
        let registrations = self.registrations.read();
        registrations
            .get(&WorkerRegistrationKey::new(group_name, worker_id))
            .cloned()
    }

    /// Runtime preflight rejects a live different-run endpoint conflict before Raft proposal.
    pub fn validate_worker_registration_preflight(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        address: &str,
        worker_net_protocol: i32,
    ) -> MetadataResult<()> {
        self.expire_liveness();
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let existing = {
            let registrations = self.registrations.read();
            registrations.get(&key).cloned()
        };
        if let Some(existing) = existing {
            let same_run = existing.worker_run_id.matches(worker_run_id);
            let endpoint_changed = existing.address != address || existing.worker_net_protocol != worker_net_protocol;
            if same_run {
                validate_same_run_descriptor(group_name, worker_id, &existing, address, worker_net_protocol)?;
            }
            if !same_run && endpoint_changed && self.is_worker_live(group_name, worker_id) {
                return Err(MetadataError::ActiveWorkerConflict(format!(
                    "worker_id {} in group_name {} is live at {} protocol {} with worker_run_id {}, rejected registration from {} protocol {} with worker_run_id {}",
                    worker_id.as_raw(),
                    group_name,
                    existing.address,
                    worker_net_protocol_label(existing.worker_net_protocol),
                    existing.worker_run_id,
                    address,
                    worker_net_protocol_label(worker_net_protocol),
                    worker_run_id
                )));
            }
        }
        Ok(())
    }

    /// Deterministic apply validation for a registration command already in the Raft log.
    pub fn validate_worker_registration_for_apply(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        address: &str,
        worker_net_protocol: i32,
    ) -> MetadataResult<()> {
        if worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        if let Some(existing) = self.registrations.read().get(&key) {
            if &existing.group_name != group_name || existing.worker_id != worker_id {
                return Err(MetadataError::Internal(format!(
                    "worker registration key mismatch for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                )));
            }
            if existing.worker_run_id.matches(worker_run_id) {
                validate_same_run_descriptor(group_name, worker_id, existing, address, worker_net_protocol)?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Register or update live startup-registration state after Raft apply succeeds.
    pub fn register_worker_run(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        worker_run_id: WorkerRunId,
        fault_domain: Option<String>,
    ) -> MetadataResult<()> {
        self.validate_worker_registration_for_apply(
            group_name,
            worker_id,
            worker_run_id,
            &address,
            worker_net_protocol,
        )?;
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let descriptor_address = address.clone();
        let descriptor_fault_domain = fault_domain.clone();
        let descriptor = WorkerDescriptor {
            group_name: group_name.clone(),
            worker_id,
            address: descriptor_address,
            worker_net_protocol,
            fault_domain: descriptor_fault_domain,
        };
        self.upsert_descriptor(descriptor)?;

        let mut registrations = self.registrations.write();
        let mut observations = self.block_report_observations.write();
        let same_registered_run = registrations
            .get(&key)
            .map(|registration| registration.worker_run_id.matches(worker_run_id))
            .unwrap_or(false);
        registrations.insert(
            key.clone(),
            WorkerRegistrationState {
                group_name: group_name.clone(),
                worker_id,
                worker_run_id,
                address,
                worker_net_protocol,
                fault_domain,
            },
        );
        if !same_registered_run {
            remove_worker_report(&mut observations, &key);
        }
        drop(observations);
        drop(registrations);
        self.heartbeat_rejections.write().remove(&key);
        if !same_registered_run {
            self.runtime.write().remove(&key);
        }
        self.notify_publication_observation_changed();
        Ok(())
    }

    /// Stages one ordered Full batch and atomically publishes the final baseline.
    ///
    /// Starting a newer baseline first invalidates the previous report and its
    /// locations because Full is currently a recovery operation, not a periodic
    /// refresh. Replayed batches never reset already accepted staging state.
    #[allow(clippy::too_many_arguments)]
    pub fn receive_full_block_report(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        baseline_seq: u64,
        batch_seq: u64,
        final_batch: bool,
        blocks: Vec<BlockReportBlock>,
    ) -> MetadataResult<BlockReportApplyResult> {
        self.validate_report_source(group_name, worker_id, worker_run_id)?;
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        let registrations = self.registrations.read();
        if !registrations
            .get(&key)
            .is_some_and(|registration| registration.worker_run_id.matches(worker_run_id))
        {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        let mut observations = self.block_report_observations.write();
        let BlockReportObservationState { reports, locations } = &mut *observations;
        let report = reports.entry(key.clone()).or_insert_with(|| WorkerBlockReportRuntime {
            worker_run_id,
            baseline_high_watermark: None,
            active: None,
            staging: None,
        });
        if !report.worker_run_id.matches(worker_run_id) {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }

        if report
            .active
            .as_ref()
            .is_some_and(|active| active.baseline_seq == baseline_seq)
        {
            return Ok(BlockReportApplyResult {
                baseline_published: true,
                ..BlockReportApplyResult::default()
            });
        }

        let continuing_staging = report
            .staging
            .as_ref()
            .is_some_and(|staging| staging.baseline_seq == baseline_seq);
        if !continuing_staging
            && report
                .baseline_high_watermark
                .is_some_and(|high_watermark| baseline_seq <= high_watermark)
        {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required: stale baseline_seq {} for group_name={}, worker_id={}, high watermark {}",
                baseline_seq,
                group_name,
                worker_id.as_raw(),
                report
                    .baseline_high_watermark
                    .expect("checked block report baseline high watermark")
            )));
        }

        let starting_new_baseline = !continuing_staging;
        let mut removed_blocks = Vec::new();
        if starting_new_baseline {
            if batch_seq != 0 {
                return Err(MetadataError::FullReportRequired(format!(
                    "full report required from batch_seq 0 for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                )));
            }
            if let Some(active) = report.active.take() {
                removed_blocks = active.ready_blocks.iter().copied().collect();
                remove_worker_ready_locations(locations, &key, active.ready_blocks.iter().copied());
            }
            report.staging = Some(StagingFullBlockReport {
                baseline_seq,
                next_batch_seq: 0,
                blocks: HashMap::new(),
            });
            report.baseline_high_watermark = Some(baseline_seq);
        }

        let staging = report.staging.as_mut().expect("full staging must exist");
        if batch_seq < staging.next_batch_seq {
            return Ok(BlockReportApplyResult {
                removed_blocks,
                next_batch_seq: staging.next_batch_seq,
                ..BlockReportApplyResult::default()
            });
        }
        if batch_seq > staging.next_batch_seq {
            let expected_batch_seq = staging.next_batch_seq;
            report.staging = None;
            return Err(MetadataError::FullReportRequired(format!(
                "full report required after batch gap: expected {}, got {} for group_name={}, worker_id={}",
                expected_batch_seq,
                batch_seq,
                group_name,
                worker_id.as_raw()
            )));
        }

        let mut batch_ids = HashSet::with_capacity(blocks.len());
        for block in &blocks {
            if !batch_ids.insert(block.block_id) || staging.blocks.contains_key(&block.block_id) {
                return Err(MetadataError::InvalidArgument(format!(
                    "full block report contains duplicate block_id {}",
                    block.block_id
                )));
            }
        }
        for block in blocks {
            staging.blocks.insert(block.block_id, block);
        }
        staging.next_batch_seq = batch_seq
            .checked_add(1)
            .ok_or_else(|| MetadataError::InvalidArgument("full block report batch_seq overflow".to_string()))?;

        if !final_batch {
            let next_batch_seq = staging.next_batch_seq;
            drop(observations);
            drop(registrations);
            if !removed_blocks.is_empty() {
                self.notify_publication_observation_changed();
            }
            return Ok(BlockReportApplyResult {
                removed_blocks,
                next_batch_seq,
                ..BlockReportApplyResult::default()
            });
        }

        let staging = report.staging.take().expect("full staging must exist");
        let ready_blocks = ready_block_ids(staging.blocks.values());
        add_worker_ready_locations(locations, &key, ready_blocks.iter().copied());
        report.active = Some(ActiveBlockReport {
            baseline_seq,
            blocks: staging.blocks,
            ready_blocks: ready_blocks.clone(),
            next_delta_batch_seq: 0,
        });
        drop(observations);
        drop(registrations);
        self.notify_publication_observation_changed();
        tracing::debug!(
            group_name = %group_name,
            worker_id = worker_id.as_raw(),
            worker_run_id = %worker_run_id,
            baseline_seq,
            "Worker full block report converged"
        );
        Ok(BlockReportApplyResult {
            added_blocks: ready_blocks.into_iter().collect(),
            removed_blocks,
            baseline_published: true,
            ..BlockReportApplyResult::default()
        })
    }

    /// Applies one ordered Delta batch and its location changes atomically.
    ///
    /// A sequence gap invalidates the affected Worker baseline before requiring
    /// Full recovery. Older batches are idempotent retries because the Worker
    /// retains one immutable in-flight request until Metadata acknowledges it.
    pub fn apply_delta_block_report(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        baseline_seq: u64,
        batch_seq: u64,
        changes: Vec<BlockReportChange>,
    ) -> MetadataResult<BlockReportApplyResult> {
        self.validate_report_source(group_name, worker_id, worker_run_id)?;
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        let registrations = self.registrations.read();
        if !registrations
            .get(&key)
            .is_some_and(|registration| registration.worker_run_id.matches(worker_run_id))
        {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        let mut observations = self.block_report_observations.write();
        let BlockReportObservationState { reports, locations } = &mut *observations;
        let report = reports.get_mut(&key).ok_or_else(|| {
            MetadataError::FullReportRequired(format!(
                "full report required before delta for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            ))
        })?;
        if !report.worker_run_id.matches(worker_run_id) {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        let Some(current_baseline_seq) = report.active.as_ref().map(|active| active.baseline_seq) else {
            return Err(MetadataError::FullReportRequired(format!(
                "full report required for current baseline: group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        };
        if current_baseline_seq != baseline_seq {
            if baseline_seq > current_baseline_seq {
                let active = report.active.take().expect("active block report must exist");
                remove_worker_ready_locations(locations, &key, active.ready_blocks.iter().copied());
                drop(observations);
                drop(registrations);
                self.notify_publication_observation_changed();
            }
            return Err(MetadataError::FullReportRequired(format!(
                "full report required for baseline_seq {}: group_name={}, worker_id={}, current {}",
                baseline_seq,
                group_name,
                worker_id.as_raw(),
                current_baseline_seq
            )));
        }
        let active = report.active.as_mut().expect("active block report must exist");
        if batch_seq < active.next_delta_batch_seq {
            return Ok(BlockReportApplyResult {
                next_batch_seq: active.next_delta_batch_seq,
                ..BlockReportApplyResult::default()
            });
        }
        if batch_seq > active.next_delta_batch_seq {
            let active = report.active.take().expect("active block report must exist");
            remove_worker_ready_locations(locations, &key, active.ready_blocks.iter().copied());
            drop(observations);
            drop(registrations);
            self.notify_publication_observation_changed();
            return Err(MetadataError::FullReportRequired(format!(
                "full report required after delta gap: expected batch_seq {}, got {}",
                active.next_delta_batch_seq, batch_seq
            )));
        }
        let Some(next_batch_seq) = batch_seq.checked_add(1) else {
            let active = report.active.take().expect("active block report must exist");
            remove_worker_ready_locations(locations, &key, active.ready_blocks.iter().copied());
            drop(observations);
            drop(registrations);
            self.notify_publication_observation_changed();
            return Err(MetadataError::FullReportRequired(
                "full report required after delta batch sequence overflow".to_string(),
            ));
        };

        let mut changed_ids = HashSet::with_capacity(changes.len());
        for change in &changes {
            let block_id = match change {
                BlockReportChange::Upsert(block) => block.block_id,
                BlockReportChange::Remove(block_id) => *block_id,
            };
            if !changed_ids.insert(block_id) {
                return Err(MetadataError::InvalidArgument(format!(
                    "delta block report contains duplicate block_id {block_id}"
                )));
            }
        }

        let mut added_blocks = Vec::new();
        let mut removed_blocks = Vec::new();
        for change in changes {
            let block_id = match &change {
                BlockReportChange::Upsert(block) => block.block_id,
                BlockReportChange::Remove(block_id) => *block_id,
            };
            let was_ready = active.ready_blocks.contains(&block_id);
            match change {
                BlockReportChange::Upsert(block) => {
                    active.blocks.insert(block_id, block);
                }
                BlockReportChange::Remove(block_id) => {
                    active.blocks.remove(&block_id);
                }
            }
            let is_ready = active
                .blocks
                .get(&block_id)
                .is_some_and(|block| block.block_state == BlockReportBlockState::Ready);
            match (was_ready, is_ready) {
                (false, true) => {
                    active.ready_blocks.insert(block_id);
                    add_worker_ready_locations(locations, &key, std::iter::once(block_id));
                    added_blocks.push(block_id);
                }
                (true, false) => {
                    active.ready_blocks.remove(&block_id);
                    remove_worker_ready_locations(locations, &key, std::iter::once(block_id));
                    removed_blocks.push(block_id);
                }
                _ => {}
            }
        }
        active.next_delta_batch_seq = next_batch_seq;
        drop(observations);
        drop(registrations);
        self.notify_publication_observation_changed();
        Ok(BlockReportApplyResult {
            added_blocks,
            removed_blocks,
            next_batch_seq,
            ..BlockReportApplyResult::default()
        })
    }

    fn validate_report_source(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> MetadataResult<()> {
        self.expire_liveness();
        let registration = self.get_registration(group_name, worker_id).ok_or_else(|| {
            MetadataError::NotFound(format!(
                "worker not registered for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            ))
        })?;
        if !registration.worker_run_id.matches(worker_run_id) {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        if !self.is_worker_live(group_name, worker_id) {
            return Err(MetadataError::NotFound(format!(
                "worker heartbeat readiness lease not found for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        Ok(())
    }

    pub fn mark_heartbeat_need_register_if_changed(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> bool {
        self.mark_heartbeat_rejection_if_changed(
            group_name,
            worker_id,
            worker_run_id,
            HeartbeatRejectionReason::NeedRegister,
        )
    }

    pub fn mark_heartbeat_run_mismatch_if_changed(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
    ) -> bool {
        self.mark_heartbeat_rejection_if_changed(
            group_name,
            worker_id,
            worker_run_id,
            HeartbeatRejectionReason::WorkerRunMismatch,
        )
    }

    fn mark_heartbeat_rejection_if_changed(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        reason: HeartbeatRejectionReason,
    ) -> bool {
        let current = HeartbeatRejectionState { worker_run_id, reason };
        let previous = self
            .heartbeat_rejections
            .write()
            .insert(WorkerRegistrationKey::new(group_name, worker_id), current);
        previous != Some(current)
    }

    fn clear_heartbeat_rejection(&self, key: &WorkerRegistrationKey) {
        self.heartbeat_rejections.write().remove(key);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_heartbeat_with_tier_free(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        advertised_endpoint: &str,
        worker_net_protocol: i32,
        tier_free: Vec<TierFree>,
    ) -> MetadataResult<WorkerLiveState> {
        self.expire_liveness();
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let descriptor = {
            let descriptors = self.descriptors.read();
            descriptors.get(&key).cloned().ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "worker descriptor not found for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                ))
            })?
        };
        let registration = {
            let registrations = self.registrations.read();
            registrations.get(&key).cloned().ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "live worker registration not found for group_name={}, worker_id={}",
                    group_name,
                    worker_id.as_raw()
                ))
            })?
        };

        if !registration.worker_run_id.matches(worker_run_id) {
            return Err(MetadataError::StaleState(format!(
                "worker_run_id mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }
        if descriptor.address != advertised_endpoint || descriptor.worker_net_protocol != worker_net_protocol {
            return Err(MetadataError::InvalidArgument(format!(
                "worker descriptor mismatch for group_name={}, worker_id={}",
                group_name,
                worker_id.as_raw()
            )));
        }

        let now = Instant::now();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut runtime = self.runtime.write();
        let live_state = match runtime.get_mut(&key) {
            Some(existing) if heartbeat_seq <= existing.heartbeat_seq => {
                existing.last_seen_at = now;
                existing.last_seen_ms = now_ms;
                existing.worker_run_id = worker_run_id;
                WorkerLiveState {
                    group_name: group_name.clone(),
                    worker_id,
                    worker_run_id,
                    heartbeat_seq: existing.heartbeat_seq,
                    last_seen_ms: existing.last_seen_ms,
                }
            }
            existing => {
                let worker_runtime = WorkerRuntime {
                    worker_run_id,
                    heartbeat_seq,
                    last_seen_at: now,
                    last_seen_ms: now_ms,
                    tier_free,
                };
                match existing {
                    Some(slot) => *slot = worker_runtime,
                    None => {
                        runtime.insert(key.clone(), worker_runtime);
                    }
                }
                WorkerLiveState {
                    group_name: group_name.clone(),
                    worker_id,
                    worker_run_id,
                    heartbeat_seq,
                    last_seen_ms: now_ms,
                }
            }
        };
        drop(runtime);
        self.clear_heartbeat_rejection(&key);
        self.notify_publication_observation_changed();

        Ok(live_state)
    }

    /// Expire heartbeat liveness.
    pub fn expire_liveness(&self) -> Vec<(GroupName, WorkerId)> {
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();
        let mut expired = Vec::new();

        {
            let mut runtime = self.runtime.write();
            runtime.retain(|key, runtime| {
                let is_live = now.duration_since(runtime.last_seen_at) < timeout;
                if !is_live {
                    expired.push((key.group_name.clone(), key.worker_id));
                }
                is_live
            });
        }

        if !expired.is_empty() {
            self.notify_publication_observation_changed();
        }
        expired
    }

    /// Remove dead-worker runtime state and keep the persisted descriptor.
    pub fn remove_dead_worker(&self, group_name: &GroupName, worker_id: WorkerId) -> (bool, Vec<BlockId>) {
        let key = WorkerRegistrationKey::new(group_name, worker_id);
        let mut removed = false;
        let mut affected_blocks = HashSet::new();

        let mut registrations = self.registrations.write();
        let mut observations = self.block_report_observations.write();
        let registration_removed = registrations.remove(&key).is_some();
        let removed_report = remove_worker_report(&mut observations, &key);
        if registration_removed || removed_report.is_some() {
            removed = true;
        }
        if let Some(report) = &removed_report {
            if let Some(active) = &report.active {
                affected_blocks.extend(active.ready_blocks.iter().copied());
            }
        }
        drop(observations);
        drop(registrations);
        if self.runtime.write().remove(&key).is_some() {
            removed = true;
        }

        let mut affected_blocks: Vec<_> = affected_blocks.into_iter().collect();
        affected_blocks.sort_by_key(|block_id| (block_id.inode_id.as_raw(), block_id.index.as_raw()));
        if removed {
            self.notify_publication_observation_changed();
        }
        (removed, affected_blocks)
    }

    /// List all live workers (based on runtime last_seen_ms), preserving group identity.
    pub fn list_live_workers(&self) -> Vec<WorkerRegistrationKey> {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        runtime
            .iter()
            .filter(|(_, r)| now.duration_since(r.last_seen_at) < timeout)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// List current in-memory worker run registrations for runtime scans.
    pub fn list_registered_workers(&self) -> Vec<WorkerRegistrationKey> {
        let registrations = self.registrations.read();
        registrations.keys().cloned().collect()
    }

    /// List live workers scoped to one metadata group.
    pub fn list_live_workers_in_group(&self, group_name: &GroupName) -> Vec<WorkerId> {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        runtime
            .iter()
            .filter(|(key, r)| &key.group_name == group_name && now.duration_since(r.last_seen_at) < timeout)
            .map(|(key, _)| key.worker_id)
            .collect()
    }

    /// Check if worker is live (based on runtime last_seen_ms).
    pub fn is_worker_live(&self, group_name: &GroupName, worker_id: WorkerId) -> bool {
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();
        let key = WorkerRegistrationKey::new(group_name, worker_id);

        runtime
            .get(&key)
            .map(|r| now.duration_since(r.last_seen_at) < timeout)
            .unwrap_or(false)
    }

    /// Build the placement worker view from group-scoped registration and heartbeat state.
    pub fn collect_worker_placement_views(&self, group_name: &GroupName) -> Vec<WorkerPlacementView> {
        let descriptors = self.descriptors.read();
        let registrations = self.registrations.read();
        let runtime = self.runtime.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        let mut views = Vec::new();
        for (key, descriptor) in descriptors.iter().filter(|(key, _)| &key.group_name == group_name) {
            let registration = registrations.get(key);
            let live = runtime.get(key);
            let registered = registration.is_some();
            let lease_valid = registered
                && live
                    .map(|runtime| now.duration_since(runtime.last_seen_at) < timeout)
                    .unwrap_or(false);
            views.push(WorkerPlacementView {
                group_name: key.group_name.clone(),
                worker_id: key.worker_id,
                worker_run_id: registration.map(|registration| registration.worker_run_id),
                endpoint: descriptor.address.clone(),
                worker_net_protocol: descriptor.worker_net_protocol,
                registered,
                lease_valid,
                ip: endpoint_host(&descriptor.address),
                host: endpoint_host(&descriptor.address),
                az: None,
                rack: descriptor.fault_domain.clone(),
                region: None,
                tier_free: live.map(|runtime| runtime.tier_free.clone()).unwrap_or_default(),
                supported_block_formats: vec![BlockFormatId::CURRENT_FOR_NEW_FILE],
            });
        }
        views.sort_by_key(|view| view.worker_id.as_raw());
        views
    }

    /// Get block locations for one metadata group (only live workers in that group).
    pub fn get_block_locations(&self, group_name: &GroupName, block_id: BlockId) -> Vec<WorkerId> {
        let live_workers = self.list_live_workers_in_group(group_name);
        let live_set: HashSet<WorkerId> = live_workers.into_iter().collect();
        let observations = self.block_report_observations.read();

        observations
            .locations
            .get(&BlockLocationKey::new(group_name, block_id))
            .map(|workers| {
                workers
                    .iter()
                    .filter(|key| &key.group_name == group_name && live_set.contains(&key.worker_id))
                    .map(|key| key.worker_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Return ready block-report locations with the report's worker run id.
    pub fn reported_block_locations(&self, group_name: &GroupName, block_id: BlockId) -> Vec<ReportedBlockLocation> {
        let observations = self.block_report_observations.read();
        let Some(worker_keys) = observations.locations.get(&BlockLocationKey::new(group_name, block_id)) else {
            return Vec::new();
        };

        let mut reported = Vec::with_capacity(worker_keys.len());
        for key in worker_keys {
            if &key.group_name != group_name {
                continue;
            }
            let Some(report) = observations.reports.get(key) else {
                continue;
            };
            let worker_run_id = report.worker_run_id;
            let Some(active) = &report.active else {
                continue;
            };
            let Some(block) = active.blocks.get(&block_id) else {
                continue;
            };
            if block.block_state != BlockReportBlockState::Ready {
                continue;
            }
            let Some(tier) = block.tier else {
                continue;
            };
            reported.push(ReportedBlockLocation {
                tier,
                group_name: group_name.clone(),
                block_id,
                durable_len: block.effective_len,
                worker_id: key.worker_id,
                worker_run_id,
            });
        }
        reported.sort_by_key(|location| location.worker_id.as_raw());
        reported
    }

    /// Captures the inclusive Worker end of the group's current report keyspace.
    ///
    /// Later workers are deferred to the next scan cycle. The paginator also
    /// captures an inclusive block end when it first enters each worker, so
    /// appends cannot indefinitely delay progress to the next worker.
    pub(crate) fn ready_replica_scan_end(&self, group_name: &GroupName) -> Option<WorkerId> {
        let observations = self.block_report_observations.read();
        let end_key = WorkerRegistrationKey::new(group_name, WorkerId::new(u64::MAX));
        let (worker_key, _) = observations.reports.range(..=end_key).next_back()?;
        if &worker_key.group_name != group_name {
            return None;
        }
        Some(worker_key.worker_id)
    }

    /// Copies one bounded, stably ordered page from published Ready reports.
    ///
    /// Report changes may defer keys inserted at or before the cursor until the
    /// next complete cycle. Registration and report guards are held together so
    /// one page never pairs different Worker runs. The work budget counts each
    /// emitted block and each visited worker that cannot emit a block, keeping
    /// scans bounded even when reports are not Ready.
    pub(crate) fn list_ready_replica_page(
        &self,
        group_name: &GroupName,
        cursor: Option<ReadyReplicaCursor>,
        scan_end_worker_id: WorkerId,
        limit: usize,
    ) -> MetadataResult<ReadyReplicaPage> {
        if limit == 0 {
            return Err(MetadataError::InvalidArgument(
                "ready replica page limit must be greater than zero".to_string(),
            ));
        }

        if cursor.is_some_and(|cursor| {
            cursor.worker_id > scan_end_worker_id
                || (cursor.worker_id == scan_end_worker_id && cursor.block_id.is_none())
        }) {
            return Ok(ReadyReplicaPage {
                replicas: Vec::new(),
                next_cursor: None,
            });
        }
        if cursor.is_some_and(|cursor| cursor.block_id.is_some() && cursor.worker_end_block_id.is_none()) {
            return Err(MetadataError::Internal(
                "ready replica cursor is missing its worker block end".to_string(),
            ));
        }

        let registrations = self.registrations.read();
        let observations = self.block_report_observations.read();
        let reports = &observations.reports;
        let start_worker_id = cursor
            .map(|cursor| cursor.worker_id)
            .unwrap_or_else(|| WorkerId::new(0));
        let start_key = WorkerRegistrationKey::new(group_name, start_worker_id);
        let mut replicas = Vec::new();
        let mut visited = 0;

        for (worker_key, report) in reports.range(start_key..) {
            if &worker_key.group_name != group_name {
                break;
            }
            if worker_key.worker_id > scan_end_worker_id {
                break;
            }
            if cursor.is_some_and(|cursor| cursor.worker_id == worker_key.worker_id && cursor.block_id.is_none()) {
                continue;
            }
            let is_end_worker = worker_key.worker_id == scan_end_worker_id;

            let report_run_id = report.worker_run_id;
            let current_run = registrations
                .get(worker_key)
                .map(|registration| registration.worker_run_id);
            let active = report.active.as_ref();
            if !current_run.is_some_and(|run_id| run_id.matches(report_run_id))
                || active.is_none_or(|active| active.ready_blocks.is_empty())
            {
                visited += 1;
                if is_end_worker {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: None,
                    });
                }
                let next_cursor = ReadyReplicaCursor {
                    worker_id: worker_key.worker_id,
                    block_id: None,
                    worker_end_block_id: None,
                };
                if visited == limit {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: Some(next_cursor),
                    });
                }
                continue;
            }
            let active = active.expect("active report checked above");

            let worker_cursor = cursor.filter(|cursor| cursor.worker_id == worker_key.worker_id);
            let after_block = worker_cursor.and_then(|cursor| cursor.block_id);
            let worker_end_block_id = worker_cursor
                .and_then(|cursor| cursor.worker_end_block_id)
                .or_else(|| active.ready_blocks.last().copied())
                .expect("non-empty Ready report has a last block");
            let lower_bound = after_block.map(Excluded).unwrap_or(Unbounded);
            let replicas_before_worker = replicas.len();
            for block_id in active.ready_blocks.range((lower_bound, Included(worker_end_block_id))) {
                let Some(block) = active.blocks.get(block_id) else {
                    return Err(MetadataError::Internal(format!(
                        "Ready block index is missing report state for group_name={}, worker_id={}, block_id={}",
                        group_name,
                        worker_key.worker_id.as_raw(),
                        block_id
                    )));
                };
                if block.block_state != BlockReportBlockState::Ready {
                    return Err(MetadataError::Internal(format!(
                        "Ready block index contains non-Ready report state for group_name={}, worker_id={}, block_id={}",
                        group_name,
                        worker_key.worker_id.as_raw(),
                        block_id
                    )));
                }
                replicas.push(ReplicaKey {
                    group_name: group_name.clone(),
                    worker_id: worker_key.worker_id,
                    worker_run_id: report_run_id,
                    block_id: *block_id,
                });
                visited += 1;
                if *block_id == worker_end_block_id {
                    if is_end_worker {
                        return Ok(ReadyReplicaPage {
                            replicas,
                            next_cursor: None,
                        });
                    }
                    if visited == limit {
                        return Ok(ReadyReplicaPage {
                            replicas,
                            next_cursor: Some(ReadyReplicaCursor {
                                worker_id: worker_key.worker_id,
                                block_id: None,
                                worker_end_block_id: None,
                            }),
                        });
                    }
                    break;
                }
                if visited == limit {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: Some(ReadyReplicaCursor {
                            worker_id: worker_key.worker_id,
                            block_id: Some(*block_id),
                            worker_end_block_id: Some(worker_end_block_id),
                        }),
                    });
                }
            }
            if replicas.len() == replicas_before_worker {
                visited += 1;
                if visited == limit && !is_end_worker {
                    return Ok(ReadyReplicaPage {
                        replicas,
                        next_cursor: Some(ReadyReplicaCursor {
                            worker_id: worker_key.worker_id,
                            block_id: None,
                            worker_end_block_id: None,
                        }),
                    });
                }
            }
            if is_end_worker {
                return Ok(ReadyReplicaPage {
                    replicas,
                    next_cursor: None,
                });
            }
        }

        Ok(ReadyReplicaPage {
            replicas,
            next_cursor: None,
        })
    }

    /// Returns whether an exact replica is still Ready in the current worker run.
    ///
    /// Registration and report guards are held together so a replacement run
    /// cannot be paired with the previous run's report.
    pub(crate) fn is_current_ready_replica(&self, replica: &ReplicaKey) -> bool {
        let worker_key = WorkerRegistrationKey::new(&replica.group_name, replica.worker_id);
        let registrations = self.registrations.read();
        let observations = self.block_report_observations.read();

        let Some(registration) = registrations.get(&worker_key) else {
            return false;
        };
        if !registration.worker_run_id.matches(replica.worker_run_id) {
            return false;
        }

        let Some(report) = observations.reports.get(&worker_key) else {
            return false;
        };
        if !report.worker_run_id.matches(replica.worker_run_id) {
            return false;
        }
        let Some(active) = &report.active else {
            return false;
        };

        active
            .blocks
            .get(&replica.block_id)
            .is_some_and(|block| block.block_state == BlockReportBlockState::Ready)
    }

    /// Subscribe before checking Ready evidence so a concurrent report cannot
    /// be lost between the snapshot check and the asynchronous wait.
    pub(crate) fn subscribe_publication_observations(&self) -> Receiver<u64> {
        self.publication_observation.subscribe()
    }

    /// Check all newly visible write targets against one current worker view.
    ///
    /// This observation never becomes durable authority. Registration,
    /// heartbeat, descriptor, and full-report guards remain held together while
    /// every target is checked, and callers must recheck after every wakeup and
    /// immediately before proposing the visibility-changing Raft command.
    pub(crate) fn check_publish_ready(
        &self,
        group_name: &GroupName,
        targets: &[PublishReadyTarget],
    ) -> PublishReadyStatus {
        let descriptors = self.descriptors.read();
        let registrations = self.registrations.read();
        let runtime = self.runtime.read();
        let observations = self.block_report_observations.read();
        let now = Instant::now();
        let timeout = self.heartbeat_timeout();

        for expected in targets {
            let target = &expected.target;
            if target.worker_endpoints.is_empty() {
                return PublishReadyStatus::Conflict(PublishReadyConflict::MissingWriteEndpoint {
                    block_id: target.block_id,
                });
            }

            let mut conflict = None;
            let mut ready = false;
            for endpoint in &target.worker_endpoints {
                let key = WorkerRegistrationKey::new(group_name, endpoint.worker_id);
                let Some(registration) = registrations.get(&key) else {
                    conflict = Some(PublishReadyConflict::WorkerRunMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                        expected: endpoint.worker_run_id,
                        current: None,
                    });
                    continue;
                };
                if !registration.worker_run_id.matches(endpoint.worker_run_id) {
                    conflict = Some(PublishReadyConflict::WorkerRunMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                        expected: endpoint.worker_run_id,
                        current: Some(registration.worker_run_id),
                    });
                    continue;
                }

                let endpoint_matches = descriptors.get(&key).is_some_and(|descriptor| {
                    descriptor.address == endpoint.endpoint
                        && descriptor.worker_net_protocol == WORKER_NET_PROTOCOL_GRPC
                        && endpoint.worker_net_protocol == WorkerNetProtocol::Grpc
                }) && registration.address == endpoint.endpoint
                    && registration.worker_net_protocol == WORKER_NET_PROTOCOL_GRPC;
                if !endpoint_matches {
                    conflict = Some(PublishReadyConflict::EndpointMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                    });
                    continue;
                }

                let Some(worker_runtime) = runtime.get(&key) else {
                    continue;
                };
                if !worker_runtime.worker_run_id.matches(endpoint.worker_run_id)
                    || now.duration_since(worker_runtime.last_seen_at) >= timeout
                {
                    continue;
                }

                let Some(report) = observations.reports.get(&key) else {
                    continue;
                };
                if !report.worker_run_id.matches(endpoint.worker_run_id) {
                    continue;
                }
                let Some(active) = &report.active else {
                    continue;
                };
                let Some(block) = active.blocks.get(&target.block_id) else {
                    continue;
                };
                if block.lease_epoch < target.fencing_token.epoch.as_raw() {
                    continue;
                }
                if block.lease_epoch > target.fencing_token.epoch.as_raw() {
                    conflict = Some(PublishReadyConflict::LeaseEpochMismatch {
                        block_id: target.block_id,
                        worker_id: endpoint.worker_id,
                        expected: target.fencing_token.epoch.as_raw(),
                        reported: block.lease_epoch,
                    });
                    continue;
                }
                if block.block_state == BlockReportBlockState::Ready && block.effective_len < expected.effective_len {
                    continue;
                }
                match block.block_state {
                    BlockReportBlockState::Ready => {
                        ready = true;
                        break;
                    }
                    BlockReportBlockState::Corrupt | BlockReportBlockState::Deleting => {
                        conflict = Some(PublishReadyConflict::UnreadableBlock {
                            block_id: target.block_id,
                            worker_id: endpoint.worker_id,
                            state: block.block_state,
                        });
                    }
                }
            }

            if !ready {
                return conflict.map_or(
                    PublishReadyStatus::Pending {
                        block_id: target.block_id,
                    },
                    PublishReadyStatus::Conflict,
                );
            }
        }

        PublishReadyStatus::Ready
    }
}

#[cfg(test)]
mod tests {
    //! Tests for worker manager and registration.

    use super::{
        BlockReportBlock, BlockReportBlockState, BlockReportChange, PublishReadyConflict, PublishReadyStatus,
        PublishReadyTarget, WorkerLiveState, WorkerManager, WorkerRegistrationKey,
    };
    use crate::error::MetadataError;
    use crate::MetadataResult;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
    use beryl_types::lease::{FencingToken, LeaseEpoch};
    use beryl_types::{
        BlockFormatId, ClientId, GroupName, LocatedBlock, Tier, TierFree, WorkerEndpointInfo, WorkerNetProtocol,
        WorkerRunId,
    };
    use std::time::{Duration, Instant};

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn report_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440100".parse().unwrap()
    }

    fn report_block(index: u32) -> BlockReportBlock {
        let block_id = BlockId::new(InodeId::new(9), BlockIndex::new(index));
        report_block_with_id(block_id)
    }

    fn report_block_with_id(block_id: BlockId) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch: u64::from(block_id.index.as_raw()) + 100,
            block_state: BlockReportBlockState::Ready,
            effective_len: 64,
        }
    }

    fn upsert(index: u32) -> BlockReportChange {
        BlockReportChange::Upsert(report_block(index))
    }

    fn remove(index: u32) -> BlockReportChange {
        BlockReportChange::Remove(report_block(index).block_id)
    }

    fn record_heartbeat(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        heartbeat_seq: u64,
        free_bytes: u64,
    ) -> MetadataResult<WorkerLiveState> {
        let descriptor = manager
            .get_descriptor(group_name, worker_id)
            .expect("worker descriptor should be registered");
        manager.record_heartbeat_with_tier_free(
            group_name,
            worker_id,
            worker_run_id,
            heartbeat_seq,
            &descriptor.address,
            descriptor.worker_net_protocol,
            vec![TierFree {
                tier: Tier::Hdd,
                free_bytes,
            }],
        )
    }

    fn register_live_report_worker(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        run_id: WorkerRunId,
    ) {
        manager
            .register_worker_run(group_name, worker_id, "127.0.0.1:9090".to_string(), 1, run_id, None)
            .unwrap();
        record_heartbeat(manager, group_name, worker_id, run_id, 1, 900).unwrap();
    }

    fn publication_target(
        worker_id: WorkerId,
        run_id: WorkerRunId,
        block_id: BlockId,
        lease_epoch: u64,
    ) -> PublishReadyTarget {
        PublishReadyTarget {
            effective_len: 64,
            target: LocatedBlock {
                write_offset: 0,
                block_id,
                file_offset: 0,
                block_size: 64,
                worker_endpoints: vec![WorkerEndpointInfo {
                    worker_id,
                    endpoint: "127.0.0.1:9090".to_string(),
                    worker_net_protocol: WorkerNetProtocol::Grpc,
                    worker_run_id: run_id,
                }],
                fencing_token: FencingToken {
                    block_id,
                    owner: ClientId::new(7),
                    epoch: LeaseEpoch::new(lease_epoch),
                },
                chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
                block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
                tier: Tier::Hdd,
            },
        }
    }

    #[test]
    fn publication_ready_check_requires_current_live_exact_worker_evidence() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-publish");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        let block_id = BlockId::new(InodeId::new(91), BlockIndex::new(0));
        let target = publication_target(worker_id, run_id, block_id, 7);
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { block_id }
        );

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 7,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Ready
        );

        manager
            .runtime
            .write()
            .get_mut(&WorkerRegistrationKey::new(&group_name_value, worker_id))
            .unwrap()
            .last_seen_at = Instant::now() - Duration::from_secs(61);
        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { block_id }
        );
    }

    #[test]
    fn publication_ready_check_rejects_run_epoch_endpoint_and_unreadable_conflicts() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-conflict");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        let block_id = BlockId::new(InodeId::new(92), BlockIndex::new(0));
        let target = publication_target(worker_id, run_id, block_id, 7);
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 8,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Conflict(PublishReadyConflict::LeaseEpochMismatch { .. })
        ));

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                2,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 7,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 32,
                }],
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { .. }
        ));

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                3,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 7,
                    block_state: BlockReportBlockState::Corrupt,
                    effective_len: 64,
                }],
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Conflict(PublishReadyConflict::UnreadableBlock { .. })
        ));

        let mut wrong_endpoint = target.clone();
        wrong_endpoint.target.worker_endpoints[0].endpoint = "127.0.0.1:9191".to_string();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&wrong_endpoint)),
            PublishReadyStatus::Conflict(PublishReadyConflict::EndpointMismatch { .. })
        ));

        let replacement_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440101".parse().unwrap();
        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                replacement_run,
                None,
            )
            .unwrap();
        assert!(matches!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Conflict(PublishReadyConflict::WorkerRunMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn publication_observation_does_not_lose_report_before_wait() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g-watch");
        let worker_id = WorkerId::new(5);
        let run_id = report_run_id();
        let block_id = BlockId::new(InodeId::new(93), BlockIndex::new(0));
        let target = publication_target(worker_id, run_id, block_id, 7);
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
        let mut observations = manager.subscribe_publication_observations();

        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Pending { block_id }
        );
        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                1,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id,
                    lease_epoch: 7,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();

        tokio::time::timeout(Duration::from_millis(100), observations.changed())
            .await
            .expect("observation should wake")
            .expect("sender remains open");
        assert_eq!(
            manager.check_publish_ready(&group_name_value, std::slice::from_ref(&target)),
            PublishReadyStatus::Ready
        );
    }

    #[test]
    fn full_batches_publish_atomically_and_stale_baselines_cannot_roll_back() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g25");
        let worker_id = WorkerId::new(9);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(0)])
            .unwrap();
        let second_worker = WorkerId::new(8);
        let second_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440101".parse().unwrap();
        register_live_report_worker(&manager, &group_name_value, second_worker, second_run);
        manager
            .receive_full_block_report(
                &group_name_value,
                second_worker,
                second_run,
                1,
                0,
                true,
                vec![report_block(0)],
            )
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![second_worker, worker_id]
        );

        let first_batch = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 8, 0, false, vec![report_block(1)])
            .unwrap();
        assert_eq!(first_batch.next_batch_seq, 1);
        assert!(!first_batch.baseline_published);
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![second_worker]
        );
        assert!(manager
            .get_block_locations(&group_name_value, report_block(1).block_id)
            .is_empty());

        let replay = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 8, 0, false, vec![report_block(1)])
            .unwrap();
        assert_eq!(replay.next_batch_seq, 1);
        let published = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 8, 1, true, vec![report_block(2)])
            .unwrap();
        assert!(published.baseline_published);
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(1).block_id),
            vec![worker_id]
        );
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(2).block_id),
            vec![worker_id]
        );

        let stale = manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(3)])
            .expect_err("stale baseline_seq must not reset the published baseline");
        assert!(stale.to_string().contains("full report required"));
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(1).block_id),
            vec![worker_id]
        );
        assert!(manager
            .get_block_locations(&group_name_value, report_block(3).block_id)
            .is_empty());
    }

    #[test]
    fn delta_report_requires_ready_baseline_and_ordered_sequence() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g23");
        let worker_id = WorkerId::new(7);
        let run_id = report_run_id();
        register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

        let before_full = manager
            .apply_delta_block_report(&group_name_value, worker_id, run_id, 1, 0, vec![upsert(0)])
            .expect_err("delta before full report must fail");
        assert!(before_full.to_string().contains("full report required"));

        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(0)])
            .unwrap();

        manager
            .apply_delta_block_report(&group_name_value, worker_id, run_id, 7, 0, vec![upsert(1)])
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(1).block_id),
            vec![worker_id]
        );

        manager
            .apply_delta_block_report(&group_name_value, worker_id, run_id, 7, 0, vec![upsert(1)])
            .unwrap();

        let newer_baseline = manager
            .apply_delta_block_report(&group_name_value, worker_id, run_id, 8, 1, vec![remove(1)])
            .expect_err("a newer unrecognized baseline must require Full recovery");
        assert!(newer_baseline.to_string().contains("full report required"));
        assert!(manager
            .get_block_locations(&group_name_value, report_block(0).block_id)
            .is_empty());
        assert!(manager
            .get_block_locations(&group_name_value, report_block(1).block_id)
            .is_empty());
        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(2)])
            .expect_err("an invalidated baseline must remain below the Full high watermark");
        assert!(manager
            .get_block_locations(&group_name_value, report_block(2).block_id)
            .is_empty());

        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                run_id,
                9,
                0,
                true,
                vec![report_block(0), report_block(1)],
            )
            .unwrap();
        let gap = manager
            .apply_delta_block_report(&group_name_value, worker_id, run_id, 9, 3, vec![remove(1)])
            .expect_err("delta gap must require Full recovery");
        assert!(gap.to_string().contains("full report required"));
        assert!(manager
            .get_block_locations(&group_name_value, report_block(0).block_id)
            .is_empty());
        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 9, 0, true, vec![report_block(2)])
            .expect_err("a gapped baseline must not be republished by a delayed Full");
        manager
            .receive_full_block_report(&group_name_value, worker_id, run_id, 10, 0, true, vec![report_block(2)])
            .expect("a strictly newer Full must recover continuity");
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(2).block_id),
            vec![worker_id]
        );
    }

    #[test]
    fn worker_run_registration_replaces_restart_and_resets_run_state() {
        let manager = WorkerManager::new(60_000);
        let worker_id = WorkerId::new(4);
        let group_name_value = group_name("g1");
        let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440023".parse().unwrap();
        let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440024".parse().unwrap();

        register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
        manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                first_run_id,
                1,
                0,
                true,
                vec![report_block(0)],
            )
            .unwrap();
        assert_eq!(
            manager.get_block_locations(&group_name_value, report_block(0).block_id),
            vec![worker_id]
        );

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                second_run_id,
                None,
            )
            .unwrap();

        assert_eq!(
            manager
                .get_registration(&group_name_value, worker_id)
                .unwrap()
                .worker_run_id,
            second_run_id
        );
        assert!(!manager.is_worker_live(&group_name_value, worker_id));
        assert!(manager
            .get_block_locations(&group_name_value, report_block(0).block_id)
            .is_empty());
        let old_heartbeat = record_heartbeat(&manager, &group_name_value, worker_id, first_run_id, 2, 900)
            .expect_err("old worker_run_id must be fenced after replacement");
        assert!(matches!(old_heartbeat, MetadataError::StaleState(_)));
        assert!(old_heartbeat.to_string().contains("worker_run_id mismatch"));

        let old_report = manager
            .receive_full_block_report(
                &group_name_value,
                worker_id,
                first_run_id,
                2,
                0,
                true,
                vec![report_block(1)],
            )
            .expect_err("old worker_run_id block report must be fenced after replacement");
        assert!(matches!(old_report, MetadataError::StaleState(_)));
        assert!(old_report.to_string().contains("worker_run_id mismatch"));

        record_heartbeat(&manager, &group_name_value, worker_id, second_run_id, 1, 900).unwrap();
        let delta = manager
            .apply_delta_block_report(&group_name_value, worker_id, second_run_id, 1, 0, vec![upsert(1)])
            .expect_err("replacement must require a new full report baseline");
        assert!(matches!(delta, MetadataError::FullReportRequired(_)));
    }

    #[test]
    fn worker_heartbeat_updates_live_state_without_moving_stale_seq_backward() {
        let manager = WorkerManager::new(60_000);
        let group_name_value = group_name("g1");
        let worker_id = WorkerId::new(1);
        let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440040".parse().unwrap();

        manager
            .register_worker_run(
                &group_name_value,
                worker_id,
                "127.0.0.1:9090".to_string(),
                1,
                run_id,
                Some("rack-a".to_string()),
            )
            .unwrap();

        let first = record_heartbeat(&manager, &group_name_value, worker_id, run_id, 10, 900).unwrap();
        assert_eq!(first.heartbeat_seq, 10);

        let stale = record_heartbeat(&manager, &group_name_value, worker_id, run_id, 9, 1_000).unwrap();
        assert_eq!(stale.heartbeat_seq, 10);

        let runtime = manager.runtime.read();
        let worker = runtime
            .get(&WorkerRegistrationKey::new(&group_name_value, worker_id))
            .unwrap();
        assert_eq!(
            worker.tier_free,
            vec![TierFree {
                tier: Tier::Hdd,
                free_bytes: 900,
            }]
        );
    }
}
