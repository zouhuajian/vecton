use beryl_common::header::CallerContextFields;
use beryl_metadata::placement::{
    PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus, WorkerPlacementView,
};
use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
use beryl_types::layout::{BlockFormatId, FileLayout};
use beryl_types::{GroupName, Tier, TierFree, WorkerRunId};

fn run_id(suffix: u32) -> WorkerRunId {
    format!("550e8400-e29b-41d4-a716-{suffix:012}")
        .parse()
        .expect("valid worker run id")
}

fn block(inode_id: u64, index: u32) -> BlockId {
    BlockId::new(InodeId::new(inode_id), BlockIndex::new(index))
}

fn group_name(raw: &str) -> GroupName {
    GroupName::parse(raw).unwrap()
}

fn worker(group_name: &GroupName, worker_id: u64, worker_run_id: WorkerRunId, host: &str) -> WorkerPlacementView {
    WorkerPlacementView {
        group_name: group_name.clone(),
        worker_id: WorkerId::new(worker_id),
        worker_run_id: Some(worker_run_id),
        endpoint: format!("{host}:19101"),
        worker_net_protocol: 1,
        registered: true,
        lease_valid: true,
        ip: None,
        host: Some(host.to_string()),
        az: None,
        rack: None,
        region: None,
        tier_free: vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: 4096,
        }],
        supported_block_formats: vec![BlockFormatId::DURABLE_PREFIX],
    }
}

fn request(group_name: &GroupName, op: PlacementOp, block_id: BlockId) -> PlacementRequest {
    let layout = FileLayout::new(4096);
    PlacementRequest {
        group_name: group_name.clone(),
        op,
        block_id,
        visible_len: 64,
        layout,
        caller: None,
        existing: Vec::new(),
        exclude_workers: Vec::new(),
        target_replicas: 1,
    }
}

#[test]
fn write_filters_workers_without_required_block_format() {
    let group = group_name("g9");
    let unsupported = WorkerPlacementView {
        supported_block_formats: Vec::new(),
        ..worker(&group, 1, run_id(11), "host-a")
    };
    let supported = worker(&group, 2, run_id(12), "host-b");
    let req = request(&group, PlacementOp::Write, block(55, 0));

    let plan = PlacementPlanner.plan(&req, &[unsupported.clone(), supported]);
    assert_eq!(plan.status, PlacementStatus::Ok);
    assert_eq!(
        plan.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
        vec![WorkerId::new(2)]
    );

    let plan = PlacementPlanner.plan(&req, &[unsupported]);
    assert_eq!(plan.status, PlacementStatus::UnsupportedBlockFormat);
    assert!(plan.workers.is_empty());
}

#[test]
fn write_uses_single_replica_and_prefers_caller_locality() {
    let group = group_name("g10");
    let block_id = block(66, 0);
    let mut req = request(&group, PlacementOp::Write, block_id);
    req.caller = Some(CallerContextFields::parse("host=host-b"));
    let workers = vec![
        worker(&group, 1, run_id(21), "host-a"),
        worker(&group, 2, run_id(22), "host-b"),
    ];

    let plan = PlacementPlanner.plan(&req, &workers);

    assert_eq!(req.target_replicas, 1);
    assert_eq!(plan.status, PlacementStatus::Ok);
    assert_eq!(
        plan.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
        vec![WorkerId::new(2)]
    );
}
