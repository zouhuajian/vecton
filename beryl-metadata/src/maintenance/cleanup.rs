// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Detection and scheduling of reported block replicas no longer referenced by metadata.

use crate::config::BlockCleanupConfig;
use crate::error::MetadataResult;
use crate::inode::InodeKind;
use crate::observe;
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::session_registry::SessionRegistry;
use crate::worker::{ReadyReplicaCursor, ReplicaKey, WorkerManager};
use beryl_types::{BlockId, GroupName, WorkerId, WorkerRunId};
use openraft::ServerState;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

/// Result of comparing one reported Ready replica with current metadata authority.
///
/// `Wait` is fail-closed: incomplete or inconsistent authority must never be
/// converted into cleanup permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupDecision {
    /// The replica is referenced by current visible metadata and must remain.
    Keep,
    /// Current evidence is insufficient to authorize reclaim or dispatch.
    Wait,
    /// The replica is absent from visible authority and may enter the grace period.
    Reclaimable,
}

/// Process-local grace and retry state for one exact reported replica.
///
/// Verification is bound to a leader term; neither the entry nor its retry
/// history is durable authority.
#[derive(Clone, Debug)]
struct CleanupEntry {
    /// First complete cycle that classified this replica as reclaimable.
    first_seen: Instant,
    /// Earliest dispatch time; later observations do not extend this deadline.
    not_before: Instant,
    /// Earliest redispatch time after the previous heartbeat command.
    next_attempt_at: Instant,
    /// Number of commands selected for this replica in the current process.
    attempts: u32,
    /// Latest leader term whose complete cycle verified this candidate.
    verified_term: u64,
}

impl CleanupEntry {
    /// Returns whether the candidate may be dispatched in this leader term.
    ///
    /// A candidate must have passed a complete scan in the current term, its
    /// grace deadline, and its retry deadline.
    fn is_due(&self, term: u64, now: Instant) -> bool {
        self.verified_term == term && now >= self.not_before && now >= self.next_attempt_at
    }
}

/// One in-progress weakly consistent traversal of the ordered Ready keyspace.
///
/// Page classifications remain provisional here until EOF. This prevents a
/// partial traversal from retiring candidates that occur on later pages. Report
/// churn may defer keys behind the cursor or beyond a Worker's positional high
/// watermark, but cannot grant deletion authority.
struct CleanupScanCycle {
    /// Leader term that authorized the cycle's linearizable authority read.
    leader_term: u64,
    /// Inclusive Worker high watermark captured when the cycle starts.
    scan_end_worker_id: Option<WorkerId>,
    /// Exclusive position from which the next bounded page resumes.
    next_cursor: Option<ReadyReplicaCursor>,
    /// Existing candidates re-observed as reclaimable during this cycle.
    seen_existing: HashSet<ReplicaKey>,
    /// Reclaimable replicas first observed during this cycle.
    pending_new: HashSet<ReplicaKey>,
}

impl CleanupScanCycle {
    /// Starts an empty traversal bound to one leader term.
    fn new(leader_term: u64, scan_end_worker_id: Option<WorkerId>) -> Self {
        Self {
            leader_term,
            scan_end_worker_id,
            next_cursor: None,
            seen_existing: HashSet::new(),
            pending_new: HashSet::new(),
        }
    }
}

/// Process-local cleanup candidates and the active paginated scan.
///
/// Both collections share one mutex so page progress and EOF candidate commit
/// cannot be observed or changed independently.
#[derive(Default)]
struct CleanupState {
    /// Candidates committed by a complete, fenced scan cycle.
    entries: HashMap<ReplicaKey, CleanupEntry>,
    /// Provisional state for the cycle currently traversing Ready reports.
    active_cycle: Option<CleanupScanCycle>,
}

/// One exact worker-local block replica selected for cleanup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockCleanupCommand {
    /// Exact block identity to reclaim on the addressed worker.
    pub block_id: BlockId,
}

/// Produces a stable order for equally attempted cleanup candidates.
///
/// Dispatch sorts by attempt count before this key so retries cannot
/// permanently monopolize a bounded heartbeat batch.
fn replica_sort_key(replica: &ReplicaKey) -> (u64, u64, u32) {
    (
        replica.worker_id.as_raw(),
        replica.block_id.inode_id.as_raw(),
        replica.block_id.index.as_raw(),
    )
}

/// Coordinates reclaimable-replica detection and heartbeat dispatch.
///
/// The coordinator owns only leader-local, report-derived soft state. Durable
/// namespace and layout state remain the cleanup authority, while workers
/// fence local access and reclaim the exact block identity from heartbeat commands.
pub(crate) struct BlockCleanupCoordinator {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    worker_manager: Arc<WorkerManager>,
    session_registry: Arc<SessionRegistry>,
    group_name: GroupName,
    scan_interval: Duration,
    reclaim_grace: Duration,
    max_replicas_per_scan: usize,
    max_candidates: usize,
    enabled: bool,
    max_commands_per_heartbeat: usize,
    retry_initial_backoff: Duration,
    retry_max_backoff: Duration,
    state: Mutex<CleanupState>,
}

impl BlockCleanupCoordinator {
    /// Creates a coordinator with process-local candidate and retry state.
    ///
    /// All state is intentionally rebuilt from complete worker reports after
    /// restart; no cleanup task or acknowledgement is persisted.
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        worker_manager: Arc<WorkerManager>,
        session_registry: Arc<SessionRegistry>,
        group_name: GroupName,
        config: &BlockCleanupConfig,
    ) -> Self {
        Self {
            raft_node,
            storage,
            worker_manager,
            session_registry,
            group_name,
            scan_interval: Duration::from_millis(config.scan_interval_ms),
            reclaim_grace: Duration::from_millis(config.reclaim_grace_ms),
            max_replicas_per_scan: config.max_replicas_per_scan,
            max_candidates: config.max_candidates,
            enabled: config.enabled,
            max_commands_per_heartbeat: config.max_commands_per_heartbeat,
            retry_initial_backoff: Duration::from_millis(config.retry_initial_backoff_ms),
            retry_max_backoff: Duration::from_millis(config.retry_max_backoff_ms),
            state: Mutex::new(CleanupState::default()),
        }
    }

    /// Interval owned by this coordinator's cleanup observation loop.
    pub(crate) fn scan_interval(&self) -> Duration {
        self.scan_interval
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Selects due cleanup commands for one accepted worker heartbeat.
    ///
    /// Selection is fenced by the current leader term and exact report identity.
    /// A report change observed before selection removes the stale candidate.
    /// A late command remains idempotent because allocated block identities are
    /// never reused and Worker reclamation drains all accesses to that identity.
    pub(crate) fn commands_for_heartbeat(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
        worker_run_id: WorkerRunId,
        now: Instant,
    ) -> Vec<BlockCleanupCommand> {
        if !self.enabled || group_name != &self.group_name {
            return Vec::new();
        }
        let Some(term) = self.current_leader_term() else {
            return Vec::new();
        };
        let mut due: Vec<_> = self
            .state
            .lock()
            .entries
            .iter()
            .filter(|(key, entry)| {
                &key.group_name == group_name
                    && key.worker_id == worker_id
                    && key.worker_run_id.matches(worker_run_id)
                    && entry.is_due(term, now)
            })
            .map(|(key, entry)| (entry.attempts, key.clone()))
            .collect();
        due.sort_by_key(|(attempts, key)| (*attempts, replica_sort_key(key)));

        let mut commands = Vec::with_capacity(due.len().min(self.max_commands_per_heartbeat));
        for (_, key) in due {
            if commands.len() == self.max_commands_per_heartbeat {
                break;
            }
            match self.revalidate_reclaimable(&key) {
                Ok(CleanupDecision::Reclaimable) => {}
                Ok(CleanupDecision::Keep) => {
                    observe::record_cleanup_decision("keep");
                    self.state.lock().entries.remove(&key);
                    continue;
                }
                Ok(CleanupDecision::Wait) => {
                    observe::record_cleanup_decision("wait");
                    self.state.lock().entries.remove(&key);
                    continue;
                }
                Err(error) => {
                    observe::record_cleanup_anomaly("authority_read");
                    warn!(
                        group_name = %key.group_name,
                        worker_id = key.worker_id.as_raw(),
                        worker_run_id = %key.worker_run_id,
                        block_id = %key.block_id,
                        error = %error,
                        "Dropping a cleanup candidate because final metadata authority could not be read"
                    );
                    self.state.lock().entries.remove(&key);
                    continue;
                }
            }
            if self.current_leader_term() != Some(term) {
                self.state.lock().entries.clear();
                return Vec::new();
            }
            if !self.worker_manager.is_current_ready_replica(&key) {
                self.state.lock().entries.remove(&key);
                continue;
            }

            let mut state = self.state.lock();
            let Some(entry) = state.entries.get_mut(&key) else {
                continue;
            };
            if !entry.is_due(term, now) {
                continue;
            }

            let retry = entry.attempts > 0;
            entry.attempts = entry.attempts.saturating_add(1);
            entry.next_attempt_at = now + self.retry_backoff(entry.attempts);
            commands.push(BlockCleanupCommand { block_id: key.block_id });
            drop(state);

            observe::record_cleanup_command();
            if retry {
                observe::record_cleanup_retry();
            }
        }
        if self.current_leader_term() != Some(term) {
            self.state.lock().entries.clear();
            return Vec::new();
        }
        commands
    }

    /// Computes capped exponential redispatch delay without a terminal attempt limit.
    ///
    /// Cleanup completion is report-derived, so a replica that remains Ready
    /// continues to be retried at the configured maximum interval.
    fn retry_backoff(&self, attempts: u32) -> Duration {
        let multiplier = 1_u128 << attempts.saturating_sub(1).min(63);
        let delay = self.retry_initial_backoff.as_millis().saturating_mul(multiplier);
        Duration::from_millis(delay.min(self.retry_max_backoff.as_millis()) as u64)
    }

    /// Executes one bounded page of cleanup observation on the current Raft leader.
    ///
    /// A complete cycle may span multiple calls and tolerates concurrent report
    /// churn. Keys added before the cursor may wait for the next cycle, while
    /// stale keys are rejected by exact dispatch-time revalidation. Positive
    /// verification and absence-based retirement are committed only after the
    /// listing reaches EOF in the same leader term.
    pub(crate) async fn scan_once(&self) -> MetadataResult<()> {
        if !self.enabled {
            return Ok(());
        }
        let Some(scan_term) = self.current_leader_term() else {
            self.state.lock().active_cycle = None;
            observe::record_cleanup_scan("not_leader");
            self.record_candidate_metrics(self.raft_node.metrics().current_term, Instant::now(), false);
            return Ok(());
        };

        // A read barrier orders authority reads but does not freeze leadership.
        if let Err(error) = self.raft_node.read(true, |_| Ok(())).await {
            let mut state = self.state.lock();
            state.entries.clear();
            state.active_cycle = None;
            drop(state);
            observe::record_cleanup_scan("authority_unavailable");
            observe::set_cleanup_candidates(0, 0, 0.0);
            return Err(error);
        }

        if self.current_leader_term() != Some(scan_term) {
            self.state.lock().active_cycle = None;
            observe::record_cleanup_scan("leadership_changed");
            observe::record_cleanup_anomaly("leadership_changed");
            self.record_candidate_metrics(scan_term, Instant::now(), false);
            return Ok(());
        }

        let active_range = {
            let mut state = self.state.lock();
            if state
                .active_cycle
                .as_ref()
                .is_some_and(|cycle| cycle.leader_term != scan_term)
            {
                state.active_cycle = None;
            }
            state
                .active_cycle
                .as_ref()
                .map(|cycle| (cycle.next_cursor, cycle.scan_end_worker_id))
        };
        let (cursor, scan_end_worker_id) =
            active_range.unwrap_or_else(|| (None, self.worker_manager.ready_replica_scan_end(&self.group_name)));
        let (replicas, next_cursor) = if let Some(scan_end_worker_id) = scan_end_worker_id {
            match self.worker_manager.list_ready_replica_page(
                &self.group_name,
                cursor,
                scan_end_worker_id,
                self.max_replicas_per_scan,
            ) {
                Ok(page) => (page.replicas, page.next_cursor),
                Err(error) => {
                    let mut state = self.state.lock();
                    state.entries.clear();
                    state.active_cycle = None;
                    drop(state);
                    observe::record_cleanup_scan("report_inconsistent");
                    observe::record_cleanup_anomaly("report_inconsistent");
                    observe::set_cleanup_candidates(0, 0, 0.0);
                    return Err(error);
                }
            }
        } else {
            (Vec::new(), None)
        };
        let now = Instant::now();
        let classified = self.classify_replicas(replicas);
        if self.current_leader_term() != Some(scan_term) {
            self.state.lock().active_cycle = None;
            observe::record_cleanup_scan("leadership_changed");
            observe::record_cleanup_anomaly("leadership_changed");
            self.record_candidate_metrics(scan_term, now, false);
            return Ok(());
        }
        let complete = self.apply_scan_page(classified, scan_term, scan_end_worker_id, next_cursor, now);
        if self.current_leader_term() != Some(scan_term) {
            let mut state = self.state.lock();
            state.entries.clear();
            state.active_cycle = None;
            drop(state);
            observe::record_cleanup_scan("leadership_changed");
            observe::record_cleanup_anomaly("leadership_changed");
            observe::set_cleanup_candidates(0, 0, 0.0);
            return Ok(());
        }
        observe::record_cleanup_scan(if complete { "complete" } else { "page" });
        self.record_candidate_metrics(scan_term, now, true);
        Ok(())
    }

    /// Returns the current term only when this node is presently the leader.
    fn current_leader_term(&self) -> Option<u64> {
        let metrics = self.raft_node.metrics();
        (metrics.state == ServerState::Leader).then_some(metrics.current_term)
    }

    /// Classifies one bounded replica page without mutating candidate state.
    ///
    /// Authority read failures become `Wait`, ensuring an unreadable replica
    /// cannot remain or become reclaimable in the committed scan result.
    fn classify_replicas(&self, replicas: Vec<ReplicaKey>) -> Vec<(ReplicaKey, CleanupDecision)> {
        replicas
            .into_iter()
            .map(|replica| {
                let decision = match self.classify(&replica) {
                    Ok(decision) => decision,
                    Err(error) => {
                        observe::record_cleanup_anomaly("authority_read");
                        warn!(
                            group_name = %replica.group_name,
                            worker_id = replica.worker_id.as_raw(),
                            worker_run_id = %replica.worker_run_id,
                            block_id = %replica.block_id,
                            error = %error,
                            "Waiting to classify a reported replica because metadata authority could not be read"
                        );
                        CleanupDecision::Wait
                    }
                };
                (replica, decision)
            })
            .collect()
    }

    /// Merges one classified page and commits the candidate set only at EOF.
    ///
    /// Returns `true` only when this page reaches EOF. Non-EOF pages preserve
    /// existing committed candidates and store classifications provisionally in
    /// the active cycle.
    fn apply_scan_page(
        &self,
        classified: Vec<(ReplicaKey, CleanupDecision)>,
        scan_term: u64,
        scan_end_worker_id: Option<WorkerId>,
        next_cursor: Option<ReadyReplicaCursor>,
        now: Instant,
    ) -> bool {
        let mut state = self.state.lock();
        let mut cycle = state
            .active_cycle
            .take()
            .unwrap_or_else(|| CleanupScanCycle::new(scan_term, scan_end_worker_id));
        if cycle.leader_term != scan_term || cycle.scan_end_worker_id != scan_end_worker_id {
            return false;
        }

        for (replica, decision) in classified {
            match decision {
                CleanupDecision::Keep => {
                    state.entries.remove(&replica);
                    cycle.pending_new.remove(&replica);
                    observe::record_cleanup_decision("keep");
                }
                CleanupDecision::Wait => {
                    state.entries.remove(&replica);
                    cycle.pending_new.remove(&replica);
                    observe::record_cleanup_decision("wait");
                }
                CleanupDecision::Reclaimable => {
                    observe::record_cleanup_decision("reclaimable");
                    if state.entries.contains_key(&replica) {
                        cycle.seen_existing.insert(replica);
                    } else if cycle.pending_new.contains(&replica) {
                        continue;
                    } else if state.entries.len() + cycle.pending_new.len() < self.max_candidates {
                        cycle.pending_new.insert(replica);
                    } else {
                        observe::record_cleanup_anomaly("candidate_limit");
                    }
                }
            }
        }

        cycle.next_cursor = next_cursor;
        if cycle.next_cursor.is_some() {
            state.active_cycle = Some(cycle);
            return false;
        }

        state.entries.retain(|key, _| cycle.seen_existing.contains(key));
        for entry in state.entries.values_mut() {
            entry.verified_term = scan_term;
        }
        for replica in cycle.pending_new {
            self.observe_candidate(&mut state.entries, replica, scan_term, now);
        }
        true
    }

    /// Classifies one replica and revalidates every potentially reclaimable result.
    ///
    /// The second phase prevents a mixed authority/session view from producing
    /// a false reclaimable observation during write publication.
    fn classify(&self, replica: &ReplicaKey) -> MetadataResult<CleanupDecision> {
        let decision = self.classify_authority(replica)?;
        if decision != CleanupDecision::Reclaimable {
            return Ok(decision);
        }
        self.revalidate_reclaimable(replica)
    }

    /// Rechecks session and durable authority before accepting `Reclaimable`.
    ///
    /// File publication persists visible blocks before removing its session.
    /// Reading the session first and authority second therefore cannot combine
    /// a pre-publish inode with that publication's already-removed session.
    fn revalidate_reclaimable(&self, replica: &ReplicaKey) -> MetadataResult<CleanupDecision> {
        if self
            .session_registry
            .get_session_identity(replica.block_id.inode_id)
            .is_some()
        {
            return Ok(CleanupDecision::Wait);
        }
        self.classify_authority(replica)
    }

    /// Classifies one replica from durable inode authority.
    ///
    /// A missing inode is reclaimable, any visible matching block id is kept,
    /// and corrupt inode authority or never-allocated block indexes wait.
    fn classify_authority(&self, replica: &ReplicaKey) -> MetadataResult<CleanupDecision> {
        let inode_id = replica.block_id.inode_id;
        let Some(inode) = self.storage.get_inode(inode_id)? else {
            return Ok(CleanupDecision::Reclaimable);
        };
        if inode.inode_id != inode_id {
            observe::record_cleanup_anomaly("inode_authority_corrupt");
            warn!(
                group_name = %replica.group_name,
                block_id = %replica.block_id,
                inode_id = inode_id.as_raw(),
                inode_inode_id = inode.inode_id.as_raw(),
                inode_kind = ?inode.file_type(),
                payload_kind = ?inode.file_type(),
                "Waiting to classify a reported replica because its inode authority is inconsistent"
            );
            return Ok(CleanupDecision::Wait);
        }

        let InodeKind::File(file) = &inode.kind else {
            observe::record_cleanup_anomaly("inode_not_file");
            warn!(
                group_name = %replica.group_name,
                block_id = %replica.block_id,
                inode_id = inode_id.as_raw(),
                "Waiting to classify a reported replica because its inode is not a file"
            );
            return Ok(CleanupDecision::Wait);
        };

        if let Err(error) = file.validate(inode_id) {
            observe::record_cleanup_anomaly("inode_authority_corrupt");
            warn!(group_name = %replica.group_name, block_id = %replica.block_id, %error,
                "Waiting to classify a replica with invalid file authority");
            return Ok(CleanupDecision::Wait);
        }
        let crate::inode::FileData { blocks, next_index, .. } = file;
        if blocks.contains(&replica.block_id) {
            return Ok(CleanupDecision::Keep);
        }

        if u64::from(replica.block_id.index.as_raw()) >= *next_index {
            observe::record_cleanup_anomaly("unexpected_block_index");
            warn!(
                group_name = %replica.group_name,
                block_id = %replica.block_id,
                next_index,
                "Waiting to classify a reported replica whose block index was not durably allocated"
            );
            return Ok(CleanupDecision::Wait);
        }

        Ok(CleanupDecision::Reclaimable)
    }

    /// Creates or renews a bounded candidate without resetting its grace period.
    ///
    /// Existing candidates retain their first observation time, but their
    /// verification advances only after a complete scan cycle.
    fn observe_candidate(
        &self,
        entries: &mut HashMap<ReplicaKey, CleanupEntry>,
        replica: ReplicaKey,
        term: u64,
        now: Instant,
    ) {
        if let Some(entry) = entries.get_mut(&replica) {
            entry.verified_term = term;
            return;
        }
        if entries.len() >= self.max_candidates {
            observe::record_cleanup_anomaly("candidate_limit");
            return;
        }
        entries.insert(
            replica,
            CleanupEntry {
                first_seen: now,
                not_before: now + self.reclaim_grace,
                next_attempt_at: now,
                attempts: 0,
                verified_term: term,
            },
        );
    }

    /// Returns total, ready, and oldest-age metrics for candidate observations.
    ///
    /// A candidate is ready only after its grace period and a complete scan in
    /// the supplied leader term. Callers suppress ready state when current
    /// leadership was not verified.
    fn candidate_metrics(&self, term: u64, now: Instant, ready_authorized: bool) -> (usize, usize, f64) {
        let state = self.state.lock();
        let ready = if ready_authorized {
            state
                .entries
                .values()
                .filter(|entry| entry.verified_term == term && now >= entry.not_before)
                .count()
        } else {
            0
        };
        let oldest_age_seconds = state
            .entries
            .values()
            .map(|entry| now.saturating_duration_since(entry.first_seen).as_secs_f64())
            .fold(0.0, f64::max);
        (state.entries.len(), ready, oldest_age_seconds)
    }

    /// Publishes the current bounded candidate gauges.
    fn record_candidate_metrics(&self, term: u64, now: Instant, ready_authorized: bool) {
        let (total, ready, oldest_age_seconds) = self.candidate_metrics(term, now, ready_authorized);
        observe::set_cleanup_candidates(total, ready, oldest_age_seconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RaftConfig;
    use crate::inode::InodeAttrs;
    use crate::inode::{Inode, InodeKind};
    use crate::raft::{AppMetadataRaftState, AppRaftStateMachine};
    use crate::session_registry::BeginSessionInput;
    use crate::worker::{BlockReportBlock, BlockReportBlockState};
    use crate::MountTable;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, MountId, WorkerId};
    use beryl_types::CommittedBlock;
    use beryl_types::{ClientId, ContentGeneration, FileLayout, LeaseEpoch, Tier, TierFree, WorkerRunId, WriteMode};
    use tempfile::TempDir;

    fn group_name() -> GroupName {
        GroupName::parse("root").unwrap()
    }

    fn cleanup_config() -> BlockCleanupConfig {
        BlockCleanupConfig {
            scan_interval_ms: 1_000,
            reclaim_grace_ms: 100,
            max_replicas_per_scan: 100,
            max_candidates: 100,
            enabled: true,
            max_commands_per_heartbeat: 2,
            retry_initial_backoff_ms: 10,
            retry_max_backoff_ms: 40,
        }
    }

    async fn test_raft(storage: Arc<RocksDBStorage>, leader: bool) -> Arc<AppRaftNode> {
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_node = Arc::new(
            AppRaftNode::new(
                1,
                storage,
                state_machine,
                Arc::new(MountTable::new()),
                &RaftConfig::default(),
            )
            .await
            .unwrap(),
        );
        if leader {
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
        }
        raft_node
    }

    fn replica(inode_id: u64, index: u32) -> ReplicaKey {
        replica_for_worker(
            inode_id,
            index,
            WorkerId::new(1),
            "550e8400-e29b-41d4-a716-446655440301".parse().unwrap(),
        )
    }

    fn replica_for_worker(inode_id: u64, index: u32, worker_id: WorkerId, worker_run_id: WorkerRunId) -> ReplicaKey {
        ReplicaKey {
            group_name: group_name(),
            worker_id,
            worker_run_id,
            block_id: BlockId::new(InodeId::new(inode_id), BlockIndex::new(index)),
        }
    }

    fn persist_file(
        storage: &RocksDBStorage,
        inode_id: InodeId,
        blocks: Vec<CommittedBlock>,
        next_index: u64,
    ) -> InodeId {
        let mut inode = Inode::new_file(
            inode_id,
            InodeAttrs::new(),
            MountId::new(1),
            beryl_types::FileLayout::new(4096),
        );
        inode.kind = InodeKind::File(crate::inode::FileData {
            len: blocks.iter().map(|block| block.len).sum(),
            layout: beryl_types::FileLayout::new(4096),
            blocks: blocks.into_iter().map(|block| block.block_id).collect(),
            generation: ContentGeneration::new(1),
            lease_epoch: LeaseEpoch::new(1),
            next_index,
            last_commit: None,
        });
        storage.put_inode(&inode).unwrap();
        inode_id
    }

    fn create_session(registry: &SessionRegistry, inode_id: InodeId) {
        let client_id = ClientId::new(1);
        let opening = registry
            .begin_session(BeginSessionInput {
                normalized_path: "/file".to_string(),
                inode_id,
                mount_id: MountId::new(1),
                current_lease_epoch: LeaseEpoch::new(0),
                mode: WriteMode::Overwrite,
                open_client_id: client_id,
                layout: FileLayout::new(64),
                ancestor_inode_ids: vec![inode_id],
            })
            .expect("session capacity");
        let file = crate::inode::FileData {
            layout: beryl_types::FileLayout::new(64),
            len: 0,
            generation: ContentGeneration::default(),
            blocks: Vec::new(),
            next_index: 0,
            lease_epoch: LeaseEpoch::new(1),
            last_commit: None,
        };
        opening
            .activate(LeaseEpoch::new(1), &file, None)
            .expect("session created");
    }

    fn publish_report(manager: &WorkerManager, replicas: &[ReplicaKey]) {
        let worker_id = WorkerId::new(1);
        let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440301".parse().unwrap();
        publish_worker_report(manager, worker_id, run_id, 1, replicas);
    }

    fn publish_worker_report(
        manager: &WorkerManager,
        worker_id: WorkerId,
        run_id: WorkerRunId,
        report_seq: u64,
        replicas: &[ReplicaKey],
    ) {
        assert!(replicas
            .iter()
            .all(|replica| replica.worker_id == worker_id && replica.worker_run_id.matches(run_id)));
        let group_name = group_name();
        let address = format!("127.0.0.1:{}", 19_000 + worker_id.as_raw());
        manager
            .register_worker_run(&group_name, worker_id, address.clone(), 1, run_id, None)
            .unwrap();
        manager
            .record_heartbeat_with_tier_free(
                &group_name,
                worker_id,
                run_id,
                report_seq,
                &address,
                1,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes: 900,
                }],
            )
            .unwrap();
        manager
            .receive_full_block_report(
                &group_name,
                worker_id,
                run_id,
                report_seq,
                0,
                true,
                replicas
                    .iter()
                    .map(|replica| BlockReportBlock {
                        tier: Some(beryl_types::Tier::Hdd),
                        block_id: replica.block_id,
                        lease_epoch: 1,
                        block_state: BlockReportBlockState::Ready,
                        effective_len: 64,
                    })
                    .collect(),
            )
            .unwrap();
    }

    async fn coordinator(
        dir: &TempDir,
        config: BlockCleanupConfig,
        leader: bool,
    ) -> (
        BlockCleanupCoordinator,
        Arc<RocksDBStorage>,
        Arc<WorkerManager>,
        Arc<SessionRegistry>,
    ) {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let raft_node = test_raft(Arc::clone(&storage), leader).await;
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let session_registry = Arc::new(SessionRegistry::default());
        let coordinator = BlockCleanupCoordinator::new(
            raft_node,
            Arc::clone(&storage),
            Arc::clone(&worker_manager),
            Arc::clone(&session_registry),
            group_name(),
            &config,
        );
        (coordinator, storage, worker_manager, session_registry)
    }

    #[tokio::test]
    async fn classification_keeps_visible_blocks_and_waits_for_unsafe_states() {
        let dir = TempDir::new().unwrap();
        let (coordinator, storage, _worker_manager, sessions) = coordinator(&dir, cleanup_config(), false).await;

        let detached = replica(10, 0);
        assert_eq!(coordinator.classify(&detached).unwrap(), CleanupDecision::Reclaimable);

        let missing_inode = replica(11, 0);
        assert_eq!(
            coordinator.classify(&missing_inode).unwrap(),
            CleanupDecision::Reclaimable
        );

        let visible = replica(12, 0);
        persist_file(
            &storage,
            visible.block_id.inode_id,
            vec![CommittedBlock {
                block_id: visible.block_id,
                len: 64,
            }],
            1,
        );
        assert_eq!(coordinator.classify(&visible).unwrap(), CleanupDecision::Keep);

        let active = replica(13, 0);
        persist_file(&storage, active.block_id.inode_id, Vec::new(), 1);
        create_session(&sessions, active.block_id.inode_id);
        assert_eq!(coordinator.classify(&active).unwrap(), CleanupDecision::Wait);
        sessions.remove_session_if_epoch(active.block_id.inode_id, LeaseEpoch::new(1));
        assert_eq!(coordinator.classify(&active).unwrap(), CleanupDecision::Reclaimable);

        // A missing block list is not evidence of detachment when length still references it.
        let mut damaged = storage.get_inode(active.block_id.inode_id).unwrap().unwrap();
        damaged.file_mut().unwrap().len = 1;
        storage.put_inode(&damaged).unwrap();
        assert_eq!(coordinator.classify(&active).unwrap(), CleanupDecision::Wait);

        let unallocated = replica(14, 1);
        persist_file(&storage, unallocated.block_id.inode_id, Vec::new(), 1);
        assert_eq!(coordinator.classify(&unallocated).unwrap(), CleanupDecision::Wait);
    }

    #[tokio::test]
    async fn grace_requires_current_term_revalidation_and_drops_reachable_replica() {
        let dir = TempDir::new().unwrap();
        let config = cleanup_config();
        let grace = Duration::from_millis(config.reclaim_grace_ms);
        let (coordinator, storage, _worker_manager, _sessions) = coordinator(&dir, config, false).await;
        let becomes_visible = replica(20, 0);
        let remains_detached = replica(21, 0);
        let first_seen = Instant::now();

        let classified = coordinator.classify_replicas(vec![becomes_visible.clone(), remains_detached.clone()]);
        assert!(coordinator.apply_scan_page(classified, 7, None, None, first_seen));
        {
            let state = coordinator.state.lock();
            let entries = &state.entries;
            assert_eq!(entries.len(), 2);
            assert!(entries
                .values()
                .all(|entry| entry.verified_term == 7 && first_seen < entry.not_before));
            assert!(entries.values().all(|entry| entry.verified_term != 8));
        }
        let after_grace = first_seen + grace;
        assert_eq!(coordinator.candidate_metrics(8, after_grace, true).1, 0);

        persist_file(
            &storage,
            becomes_visible.block_id.inode_id,
            vec![CommittedBlock {
                block_id: becomes_visible.block_id,
                len: 64,
            }],
            1,
        );
        let classified = coordinator.classify_replicas(vec![becomes_visible.clone(), remains_detached.clone()]);
        assert!(coordinator.apply_scan_page(classified, 8, None, None, after_grace));

        let state = coordinator.state.lock();
        let entries = &state.entries;
        assert!(!entries.contains_key(&becomes_visible));
        let entry = entries.get(&remains_detached).expect("detached candidate remains");
        assert_eq!(entry.first_seen, first_seen);
        assert_eq!(entry.verified_term, 8);
        assert!(after_grace >= entry.not_before);
        drop(state);
        assert_eq!(coordinator.candidate_metrics(8, after_grace, true).1, 1);
        assert_eq!(coordinator.candidate_metrics(8, after_grace, false).1, 0);
    }

    #[tokio::test]
    async fn paginated_cycle_commits_only_at_eof_and_bounds_candidates() {
        let dir = TempDir::new().unwrap();
        let mut config = cleanup_config();
        config.max_candidates = 1;
        config.max_replicas_per_scan = 1;
        let (coordinator, _storage, worker_manager, _sessions) = coordinator(&dir, config, true).await;
        let first = replica(30, 0);
        let second = replica(31, 0);

        publish_report(&worker_manager, &[first.clone(), second.clone()]);
        coordinator.scan_once().await.unwrap();
        {
            let state = coordinator.state.lock();
            assert!(state.entries.is_empty());
            assert!(state.active_cycle.is_some());
        }

        coordinator.scan_once().await.unwrap();
        let state = coordinator.state.lock();
        assert_eq!(state.entries.len(), 1);
        assert!(state.active_cycle.is_none());
        assert!(state.entries.contains_key(&first));
        assert!(!state.entries.contains_key(&second));
    }

    #[tokio::test]
    async fn final_authority_revalidation_catches_publish_after_initial_read() {
        let dir = TempDir::new().unwrap();
        let (coordinator, storage, _worker_manager, sessions) = coordinator(&dir, cleanup_config(), false).await;
        let published = replica(60, 0);
        let inode_id = persist_file(&storage, published.block_id.inode_id, Vec::new(), 1);
        create_session(&sessions, published.block_id.inode_id);

        let initial = coordinator.classify_authority(&published).unwrap();
        assert_eq!(initial, CleanupDecision::Reclaimable);

        let mut inode = storage.get_inode(inode_id).unwrap().unwrap();
        let file = inode.file_mut().unwrap();
        file.blocks.push(published.block_id);
        file.len = 64;
        storage
            .put_inode_atomic(&inode, &AppMetadataRaftState::default())
            .unwrap();
        sessions.remove_session_if_epoch(published.block_id.inode_id, LeaseEpoch::new(1));

        let decision = coordinator.revalidate_reclaimable(&published).unwrap();
        assert_eq!(decision, CleanupDecision::Keep);
        assert!(coordinator.apply_scan_page(vec![(published.clone(), decision)], 1, None, None, Instant::now()));
        assert!(!coordinator.state.lock().entries.contains_key(&published));
    }

    #[tokio::test]
    async fn worker_run_change_during_scan_cannot_dispatch_the_old_run_candidate() {
        let dir = TempDir::new().unwrap();
        let mut config = cleanup_config();
        config.max_replicas_per_scan = 1;
        config.enabled = true;
        config.reclaim_grace_ms = 0;
        let (coordinator, _storage, worker_manager, _sessions) = coordinator(&dir, config, true).await;
        let old_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440301".parse().unwrap();
        let new_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440303".parse().unwrap();
        let old_first = replica_for_worker(70, 0, WorkerId::new(1), old_run);
        let old_last = replica_for_worker(71, 0, WorkerId::new(1), old_run);
        publish_worker_report(
            &worker_manager,
            WorkerId::new(1),
            old_run,
            1,
            &[old_first.clone(), old_last],
        );

        coordinator.scan_once().await.unwrap();
        publish_worker_report(&worker_manager, WorkerId::new(1), new_run, 1, &[]);
        coordinator.scan_once().await.unwrap();

        {
            let state = coordinator.state.lock();
            assert!(state.active_cycle.is_none());
            assert!(state.entries.contains_key(&old_first));
        }
        let now = Instant::now();
        assert!(coordinator
            .commands_for_heartbeat(&old_first.group_name, old_first.worker_id, old_first.worker_run_id, now)
            .is_empty());
        assert!(!coordinator.state.lock().entries.contains_key(&old_first));
    }

    #[tokio::test]
    async fn leader_term_change_discards_a_partial_cycle() {
        let dir = TempDir::new().unwrap();
        let (coordinator, _storage, _worker_manager, _sessions) = coordinator(&dir, cleanup_config(), false).await;
        let candidate = replica(72, 0);
        let cursor = ReadyReplicaCursor {
            worker_id: candidate.worker_id,
            block_id: Some(candidate.block_id),
            worker_end_block_id: Some(candidate.block_id),
        };
        let now = Instant::now();

        assert!(!coordinator.apply_scan_page(
            vec![(candidate.clone(), CleanupDecision::Reclaimable)],
            7,
            Some(candidate.worker_id),
            Some(cursor),
            now,
        ));
        assert!(coordinator.state.lock().active_cycle.is_some());

        assert!(!coordinator.apply_scan_page(Vec::new(), 8, Some(candidate.worker_id), None, now));
        let state = coordinator.state.lock();
        assert!(state.active_cycle.is_none());
        assert!(state.entries.is_empty());
    }

    #[tokio::test]
    async fn cleanup_dispatch_revalidates_session_and_inode_authority() {
        let dir = TempDir::new().unwrap();
        let mut config = cleanup_config();
        config.enabled = true;
        config.reclaim_grace_ms = 0;
        let (coordinator, storage, worker_manager, sessions) = coordinator(&dir, config, true).await;
        let session_started = replica(67, 0);
        let became_visible = replica(68, 0);
        let authority_unreadable = replica(69, 0);
        persist_file(&storage, became_visible.block_id.inode_id, Vec::new(), 1);
        persist_file(&storage, authority_unreadable.block_id.inode_id, Vec::new(), 1);
        let candidates = [
            session_started.clone(),
            became_visible.clone(),
            authority_unreadable.clone(),
        ];
        publish_report(&worker_manager, &candidates);
        coordinator.scan_once().await.unwrap();
        assert!(candidates
            .iter()
            .all(|candidate| coordinator.state.lock().entries.contains_key(candidate)));

        persist_file(&storage, session_started.block_id.inode_id, Vec::new(), 1);
        create_session(&sessions, session_started.block_id.inode_id);
        let mut pending = sessions
            .begin_publication(session_started.block_id.inode_id, LeaseEpoch::new(1))
            .unwrap();
        pending.mark_submitted().unwrap();
        // Lost completion ownership cannot make a pre-existing GC candidate safe.
        drop(pending);
        assert_eq!(coordinator.classify(&session_started).unwrap(), CleanupDecision::Wait);

        create_session(&sessions, became_visible.block_id.inode_id);
        let mut visible_inode = storage.get_inode(became_visible.block_id.inode_id).unwrap().unwrap();
        let InodeKind::File(crate::inode::FileData { blocks, .. }) = &mut visible_inode.kind else {
            panic!("test inode must be a file");
        };
        blocks.push(became_visible.block_id);
        storage
            .put_inode_atomic(&visible_inode, &AppMetadataRaftState::default())
            .unwrap();
        sessions.remove_session_if_epoch(became_visible.block_id.inode_id, LeaseEpoch::new(1));

        storage
            .with_pinned_db(|db| {
                let cf = db.cf_handle("inodes").unwrap();
                let mut key = b"inode/".to_vec();
                key.extend_from_slice(&authority_unreadable.block_id.inode_id.to_be_bytes());
                db.put_cf(cf, key, b"corrupt-inode").unwrap();
                Ok(())
            })
            .unwrap();

        let commands = coordinator.commands_for_heartbeat(
            &group_name(),
            session_started.worker_id,
            session_started.worker_run_id,
            Instant::now(),
        );

        assert!(commands.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| !coordinator.state.lock().entries.contains_key(candidate)));
    }

    #[tokio::test]
    async fn cleanup_dispatch_is_bounded_fair_and_drops_candidates_after_report_or_run_change() {
        let dir = TempDir::new().unwrap();
        let mut config = cleanup_config();
        config.enabled = true;
        config.reclaim_grace_ms = 1;
        config.max_commands_per_heartbeat = 2;
        config.retry_max_backoff_ms = config.retry_initial_backoff_ms;
        let (coordinator, _storage, workers, _sessions) = coordinator(&dir, config, true).await;
        let candidates = vec![replica(74, 0), replica(72, 0), replica(73, 0)];
        publish_report(&workers, &candidates);
        coordinator.scan_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        coordinator.scan_once().await.unwrap();
        let now = Instant::now();

        let first = coordinator.commands_for_heartbeat(
            &group_name(),
            candidates[0].worker_id,
            candidates[0].worker_run_id,
            now,
        );
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].block_id, candidates[1].block_id);
        assert_eq!(first[1].block_id, candidates[2].block_id);

        let second = coordinator.commands_for_heartbeat(
            &group_name(),
            candidates[0].worker_id,
            candidates[0].worker_run_id,
            now + coordinator.retry_initial_backoff,
        );
        assert_eq!(second.len(), 2);
        assert_eq!(second[0].block_id, candidates[0].block_id);

        workers
            .receive_full_block_report(
                &group_name(),
                candidates[0].worker_id,
                candidates[0].worker_run_id,
                2,
                0,
                true,
                Vec::new(),
            )
            .unwrap();
        let later = now + Duration::from_secs(1);
        assert!(coordinator
            .commands_for_heartbeat(
                &group_name(),
                candidates[0].worker_id,
                candidates[0].worker_run_id,
                later,
            )
            .is_empty());
        coordinator.scan_once().await.unwrap();
        assert!(coordinator.state.lock().entries.is_empty());

        workers
            .receive_full_block_report(
                &group_name(),
                candidates[0].worker_id,
                candidates[0].worker_run_id,
                3,
                0,
                true,
                vec![BlockReportBlock {
                    tier: Some(beryl_types::Tier::Hdd),
                    block_id: candidates[0].block_id,
                    lease_epoch: 1,
                    block_state: BlockReportBlockState::Ready,
                    effective_len: 64,
                }],
            )
            .unwrap();
        coordinator.scan_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        coordinator.scan_once().await.unwrap();

        let replacement_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440399".parse().unwrap();
        workers
            .register_worker_run(
                &group_name(),
                candidates[0].worker_id,
                "127.0.0.1:19001".to_string(),
                1,
                replacement_run,
                None,
            )
            .unwrap();
        assert!(coordinator
            .commands_for_heartbeat(
                &group_name(),
                candidates[0].worker_id,
                candidates[0].worker_run_id,
                later,
            )
            .is_empty());
        coordinator.scan_once().await.unwrap();
        assert!(!coordinator.state.lock().entries.contains_key(&candidates[0]));
    }
}
