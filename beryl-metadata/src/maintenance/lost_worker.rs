// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Lost-worker cleanup.

use crate::error::MetadataResult;
use crate::raft::AppRaftNode;
use crate::worker::WorkerManager;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::info;

/// Dependencies for lost-worker cleanup.
pub struct LostWorkerCleanupDeps {
    pub raft_node: Arc<AppRaftNode>,
    pub worker_manager: Arc<WorkerManager>,
}

/// Summary for one lost-worker cleanup scan.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LostWorkerCleanupOutcome {
    pub removed_workers: usize,
    pub affected_blocks: usize,
}

/// Removes expired worker soft state and records the affected block count.
pub struct LostWorkerCleanupService {
    raft_node: Arc<AppRaftNode>,
    worker_manager: Arc<WorkerManager>,
}

impl LostWorkerCleanupService {
    pub fn new(deps: LostWorkerCleanupDeps) -> Self {
        Self {
            raft_node: deps.raft_node,
            worker_manager: deps.worker_manager,
        }
    }

    pub async fn run_once(&self) -> MetadataResult<LostWorkerCleanupOutcome> {
        if !self.raft_node.is_leader() {
            return Ok(LostWorkerCleanupOutcome::default());
        }

        let live_workers = self.worker_manager.list_live_workers();
        let registered_workers = self.worker_manager.list_registered_workers();
        let live_set: HashSet<_> = live_workers.iter().cloned().collect();
        let dead_workers: Vec<_> = registered_workers
            .into_iter()
            .filter(|worker| !live_set.contains(worker))
            .collect();

        let mut outcome = LostWorkerCleanupOutcome::default();
        for dead_worker in dead_workers {
            let (removed, affected_blocks) = self
                .worker_manager
                .remove_dead_worker(&dead_worker.group_name, dead_worker.worker_id);
            if !removed {
                continue;
            }
            info!(
                group_name = %dead_worker.group_name,
                worker_id = dead_worker.worker_id.as_raw(),
                "Removing dead worker"
            );
            outcome.removed_workers += 1;
            outcome.affected_blocks += affected_blocks.len();
            // TODO: schedule repairs for affected blocks after replication is implemented end to end.
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::RaftConfig;
    use crate::maintenance::lost_worker::{LostWorkerCleanupDeps, LostWorkerCleanupService};
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::worker::{BlockReportBlock, BlockReportBlockState, HealthStatus, WorkerInfo, WorkerManager};
    use crate::MountTable;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
    use beryl_types::{GroupName, Tier, TierFree, WorkerRunId};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::Duration;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    async fn test_raft(dir: &TempDir, leader: bool) -> Arc<AppRaftNode> {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, state_machine, mount_table, &raft_config)
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
        } else {
            assert!(!raft_node.is_leader());
        }
        raft_node
    }

    fn worker_run_id(worker_id: WorkerId) -> WorkerRunId {
        format!("550e8400-e29b-41d4-a716-{:012x}", worker_id.as_raw())
            .parse()
            .expect("valid test WorkerRunId")
    }

    fn live_worker(manager: &WorkerManager, worker_id: WorkerId) {
        let group_name = group_name("root");
        let address = format!("127.0.0.1:{}", 9000 + worker_id.as_raw());
        let run_id = worker_run_id(worker_id);
        manager
            .register_worker_run(&group_name, worker_id, address.clone(), 1, run_id, None)
            .unwrap();
        manager
            .record_heartbeat_with_tier_free(
                &group_name,
                worker_id,
                run_id,
                1,
                &address,
                1,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes: 500,
                }],
            )
            .unwrap();
    }

    fn report_block(block_id: BlockId) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch: u64::from(block_id.index.as_raw()) + 1,
            block_state: BlockReportBlockState::Ready,
            effective_len: 64,
        }
    }

    fn persisted_worker(group_name: GroupName, worker_id: WorkerId) -> WorkerInfo {
        WorkerInfo {
            group_name,
            worker_id,
            address: "127.0.0.1:9090".to_string(),
            worker_net_protocol: 1,
            capacity_total: 0,
            capacity_used: 0,
            capacity_available: 0,
            active_reads: 0,
            active_writes: 0,
            health: HealthStatus::Healthy,
            last_heartbeat: 0,
            fault_domain: None,
        }
    }

    fn publish_report(manager: &WorkerManager, worker_id: WorkerId, report_seq: u64, blocks: Vec<BlockId>) {
        let group_name = group_name("root");
        let run_id = manager
            .get_registration(&group_name, worker_id)
            .expect("worker registration")
            .worker_run_id;
        manager
            .receive_full_block_report(
                &group_name,
                worker_id,
                run_id,
                report_seq,
                0,
                true,
                blocks.into_iter().map(report_block).collect(),
            )
            .unwrap();
    }

    fn service(raft_node: Arc<AppRaftNode>, worker_manager: Arc<WorkerManager>) -> LostWorkerCleanupService {
        LostWorkerCleanupService::new(LostWorkerCleanupDeps {
            raft_node,
            worker_manager,
        })
    }

    #[tokio::test]
    async fn dead_worker_removed_and_affected_blocks_recorded() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1_000));
        let source = WorkerId::new(1);
        let dead = WorkerId::new(4);
        let block_id = BlockId::new(InodeId::new(11), BlockIndex::new(0));
        live_worker(&worker_manager, dead);
        publish_report(&worker_manager, dead, 1, vec![block_id]);
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        live_worker(&worker_manager, source);
        publish_report(&worker_manager, source, 1, vec![block_id]);

        let outcome = service(Arc::clone(&raft_node), Arc::clone(&worker_manager))
            .run_once()
            .await
            .unwrap();

        assert_eq!(outcome.removed_workers, 1);
        assert_eq!(outcome.affected_blocks, 1);
        assert!(worker_manager.get_registration(&group_name("root"), dead).is_none());
        assert!(worker_manager.get_descriptor(&group_name("root"), dead).is_some());
        assert!(!worker_manager
            .list_registered_workers()
            .iter()
            .any(|key| key.group_name == group_name("root") && key.worker_id == dead));
        assert_eq!(
            worker_manager.get_block_locations(&group_name("root"), block_id),
            vec![source]
        );
        let second_outcome = service(Arc::clone(&raft_node), Arc::clone(&worker_manager))
            .run_once()
            .await
            .unwrap();

        assert_eq!(second_outcome.removed_workers, 0);
        assert_eq!(second_outcome.affected_blocks, 0);
    }

    #[tokio::test]
    async fn persisted_descriptor_without_runtime_is_not_a_dead_worker_after_reload() {
        let dir = TempDir::new().unwrap();
        let raft_node = test_raft(&dir, true).await;
        let worker_manager = Arc::new(WorkerManager::new(1_000));
        let group_name_value = group_name("root");
        let worker_id = WorkerId::new(9);
        worker_manager
            .load_registered_workers(vec![persisted_worker(group_name_value.clone(), worker_id)])
            .unwrap();

        let outcome = service(Arc::clone(&raft_node), Arc::clone(&worker_manager))
            .run_once()
            .await
            .unwrap();

        assert_eq!(outcome.removed_workers, 0);
        assert_eq!(outcome.affected_blocks, 0);
        assert!(worker_manager.get_descriptor(&group_name_value, worker_id).is_some());
        assert!(worker_manager.get_registration(&group_name_value, worker_id).is_none());
        assert!(worker_manager.list_registered_workers().is_empty());
    }
}
