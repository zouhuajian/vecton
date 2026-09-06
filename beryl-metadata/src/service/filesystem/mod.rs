// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Filesystem semantics shared by metadata RPC handlers.

mod command;
mod guard;
mod namespace;
mod publish;
mod read;
mod write;

use crate::error::{to_rpc_error, MetadataError, MetadataResult};
use crate::inode::Inode;
use crate::metrics::MetadataMetrics;
use crate::mount::MountTable;
use crate::path_resolver::{MountContext, PathResolver, ResolvedPath};
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::session_registry::SessionRegistry;
use crate::state::StateStore;
use crate::worker::WorkerManager;
use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail};
use beryl_common::header::RequestHeader;
use beryl_types::ids::{InodeId, WorkerId};
use beryl_types::{FileLayout, GroupName, GroupStateWatermark, WorkerEndpointInfo, WorkerRunId, WriteHandle};
use command::RoutedFsWriteCtx;
use guard::{AdmissionFailure, AdmissionGuard, FreshnessValidator, StaleStateStatus};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) use namespace::{CreateDirectoryArgs, CreateFileArgs, DeleteArgs, RenameArgs};
pub(super) use publish::{CommitFileArgs, SyncWriteArgs};
pub(super) use read::{BlockLocationsTarget, GetBlockLocationsArgs, GetStatusArgs, ListStatusArgs, OpenFileArgs};
pub(super) use write::{AbortFileWriteArgs, AllocateBlockArgs, AuthorizeBlockWriteArgs, OpenWriteArgs, RenewLeaseArgs};

/// The supported runtime authorizes exactly one worker for each block.
const SUPPORTED_REPLICA_COUNT: u8 = 1;

#[derive(Clone, Debug)]
pub(crate) struct RequestContext {
    pub(crate) caller: RequestHeader,
    pub(crate) route_epoch: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Freshness {
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) route_epoch: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FileRange {
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct FsSuccess<T> {
    pub(crate) payload: T,
    pub(crate) group_name: Option<GroupName>,
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) route_epoch: Option<u64>,
    pub(crate) state: Vec<GroupStateWatermark>,
}

#[derive(Clone, Debug)]
pub(crate) struct FsFailure {
    pub(crate) error: Box<RpcErrorDetail>,
    pub(crate) group_name: Option<GroupName>,
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) route_epoch: Option<u64>,
    pub(crate) state: Vec<GroupStateWatermark>,
}

impl FsFailure {
    fn new(
        error: RpcErrorDetail,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        state: Vec<GroupStateWatermark>,
    ) -> Self {
        Self {
            error: Box::new(error),
            group_name,
            mount_epoch,
            route_epoch,
            state,
        }
    }
}

pub(crate) type FsResult<T> = Result<FsSuccess<T>, FsFailure>;

fn fs_failure_from_metadata_error(
    ctx: &RequestContext,
    err: MetadataError,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> FsFailure {
    fs_failure_from_rpc_error(ctx, to_rpc_error(err), group_name, mount_epoch, route_epoch)
}

fn fs_failure_from_rpc_error(
    ctx: &RequestContext,
    err: RpcErrorDetail,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> FsFailure {
    let group_name = group_name.or_else(|| ctx.caller.group_name.clone());
    FsFailure::new(err, group_name, mount_epoch, route_epoch, Vec::new())
}

#[allow(clippy::too_many_arguments)]
fn refresh_metadata_fs_failure(
    ctx: &RequestContext,
    kind: ErrorKind,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    hint: Option<RefreshHint>,
) -> FsFailure {
    let err = RpcErrorDetail::refresh_metadata(kind, hint.unwrap_or_default(), message);
    fs_failure_from_rpc_error(ctx, err, group_name, mount_epoch, route_epoch)
}

fn worker_endpoint_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_net_protocol: i32,
    worker_run_id: WorkerRunId,
) -> Result<WorkerEndpointInfo, MetadataError> {
    if worker_net_protocol != 1 {
        return Err(MetadataError::InvalidArgument(format!(
            "unsupported persisted worker network protocol {worker_net_protocol}"
        )));
    }
    beryl_proto::convert::worker_endpoint_info_from_parts(worker_id, endpoint, worker_run_id.to_string())
        .map_err(MetadataError::InvalidArgument)
}

fn missing_resolved_target_error(resolved: &ResolvedPath) -> MetadataError {
    let message = match (resolved.parent_inode_id, resolved.name.as_deref()) {
        (Some(parent_inode_id), Some(name)) => {
            format!("Entry not found: {} (parent inode: {})", name, parent_inode_id)
        }
        _ => "resolved path has no target".to_string(),
    };
    MetadataError::NotFound(message)
}

impl MetadataFileSystem {
    /// Return whether one exact file has a non-expired opening or active session.
    fn has_active_write(&self, inode_id: InodeId) -> bool {
        self.session_registry.has_active_write(inode_id)
    }

    /// Return whether an inode is or contains a non-expired leader-local write session.
    ///
    /// The ancestor index makes this check independent of namespace subtree size.
    fn has_active_write_under(&self, inode_id: InodeId) -> bool {
        self.session_registry.has_active_write_under(inode_id)
    }
}

pub(crate) struct MetadataFileSystemDeps {
    pub(crate) state_store: Arc<dyn StateStore>,
    pub(crate) mount_table: Arc<MountTable>,
    pub(crate) storage: Arc<RocksDBStorage>,
    pub(crate) raft_node: Option<Arc<AppRaftNode>>,
    pub(crate) session_registry: Arc<SessionRegistry>,
    pub(crate) worker_manager: Option<Arc<WorkerManager>>,
    pub(crate) metrics: Option<Arc<MetadataMetrics>>,
    pub(crate) readiness_gate: Option<Arc<RootReadinessGate>>,
    /// Validated server-owned layout used by atomic CreateFile.
    pub(crate) file_create_layout: FileLayout,
}

/// Metadata service state combining durable Raft authority with leader-local admission state.
pub(crate) struct MetadataFileSystem {
    path_resolver: PathResolver,
    /// Serializes path-bound write admission with topology-changing operations.
    ///
    /// Create/OpenWrite/RenewLease take a shared guard; Rename/Delete take an
    /// exclusive guard. This lock is leader-local admission only: Raft apply
    /// preconditions and persisted fencing epochs provide replay-safe durable
    /// authority, while OpenWrite revalidates its path before replying.
    namespace_topology: RwLock<()>,
    admission: AdmissionGuard,
    mount_table: Arc<MountTable>,
    freshness_validator: FreshnessValidator,
    storage: Arc<RocksDBStorage>,
    raft_node: Option<Arc<AppRaftNode>>,
    metrics: Option<Arc<MetadataMetrics>>,
    session_registry: Arc<SessionRegistry>,
    worker_manager: Option<Arc<WorkerManager>>,
    file_create_layout: FileLayout,
}

impl MetadataFileSystem {
    pub(crate) fn new(deps: MetadataFileSystemDeps) -> Self {
        let path_resolver = PathResolver::new(Arc::clone(&deps.mount_table), Arc::clone(&deps.storage));
        let admission = AdmissionGuard::new(
            Arc::clone(&deps.mount_table),
            deps.readiness_gate,
            deps.raft_node.clone(),
        );
        let freshness_validator = FreshnessValidator::new(Arc::clone(&deps.state_store), Arc::clone(&deps.mount_table));

        Self {
            path_resolver,
            namespace_topology: RwLock::new(()),
            admission,
            mount_table: deps.mount_table,
            freshness_validator,
            storage: deps.storage,
            raft_node: deps.raft_node,
            metrics: deps.metrics,
            session_registry: deps.session_registry,
            worker_manager: deps.worker_manager,
            file_create_layout: deps.file_create_layout,
        }
    }

    fn response_state_for_success(&self, group_name: Option<&GroupName>) -> Vec<GroupStateWatermark> {
        let (Some(group_name), Some(raft_node)) = (group_name, self.raft_node.as_ref()) else {
            // A response without a known owner group cannot authorize a state cache advance.
            return Vec::new();
        };
        if !raft_node.is_leader() {
            return Vec::new();
        }
        raft_node
            .get_last_applied_state_id()
            .map(|state_id| GroupStateWatermark::new(group_name.clone(), state_id))
            .into_iter()
            .collect()
    }

    fn success<T>(&self, payload: T, group_name: Option<GroupName>, mount_epoch: Option<u64>) -> FsResult<T> {
        self.success_with_route_epoch(payload, group_name, mount_epoch, None)
    }

    fn success_with_route_epoch<T>(
        &self,
        payload: T,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> FsResult<T> {
        Ok(FsSuccess {
            payload,
            group_name: group_name.clone(),
            mount_epoch,
            route_epoch,
            state: self.response_state_for_success(group_name.as_ref()),
        })
    }

    fn failure_from_error<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsResult<T> {
        self.failure_from_error_with_route_epoch(ctx, err, group_name, mount_epoch, None)
    }

    fn failure_from_error_with_route_epoch<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> FsResult<T> {
        Err(fs_failure_from_metadata_error(
            ctx,
            err,
            group_name,
            mount_epoch,
            route_epoch,
        ))
    }

    fn failure_from_admission<T>(&self, failure: AdmissionFailure) -> FsResult<T> {
        Err(FsFailure {
            error: failure.err,
            group_name: failure.group_name,
            mount_epoch: failure.mount_epoch,
            route_epoch: None,
            state: Vec::new(),
        })
    }

    fn failure_from_path_error<T>(&self, ctx: &RequestContext, path: &str, err: MetadataError) -> FsResult<T> {
        let mount_ctx = self
            .path_resolver
            .resolve_mount_components(path)
            .ok()
            .map(|(mount_ctx, _)| mount_ctx);
        self.failure_from_resolved_path_error(ctx, err, mount_ctx.as_ref())
    }

    fn failure_from_resolved_path_error<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        mount_ctx: Option<&MountContext>,
    ) -> FsResult<T> {
        let (group_name, mount_epoch) = mount_ctx
            .map(|mount| (Some(mount.owner_group_name.clone()), Some(mount.mount_epoch)))
            .unwrap_or((None, None));
        self.failure_from_error(ctx, err, group_name, mount_epoch)
    }

    fn require_worker_lookup_group(
        &self,
        ctx: &RequestContext,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        intent: &str,
    ) -> Result<GroupName, FsFailure> {
        group_name.clone().ok_or_else(|| {
            fs_failure_from_metadata_error(
                ctx,
                MetadataError::Internal(format!("{intent} worker lookup requires authoritative metadata group")),
                group_name,
                mount_epoch,
                route_epoch,
            )
        })
    }

    // Refresh failures must keep caller and server hint fields explicit.
    #[allow(clippy::too_many_arguments)]
    fn refresh_metadata_failure_with_hint<T>(
        &self,
        ctx: &RequestContext,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        mut hint: Option<RefreshHint>,
    ) -> FsResult<T> {
        if let Some(group_name_value) = &group_name {
            hint.get_or_insert_with(RefreshHint::default).group_name = Some(group_name_value.to_string());
        }
        if let Some(mount_epoch_value) = mount_epoch {
            hint.get_or_insert_with(RefreshHint::default).mount_epoch = Some(mount_epoch_value);
        }
        if let Some(route_epoch_value) = route_epoch {
            hint.get_or_insert_with(RefreshHint::default).route_epoch = Some(route_epoch_value);
        }

        Err(refresh_metadata_fs_failure(
            ctx,
            kind,
            message,
            group_name.clone(),
            mount_epoch,
            route_epoch,
            hint,
        ))
    }

    fn session_terminal_failure<T>(
        &self,
        ctx: &RequestContext,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsResult<T> {
        let group_name = group_name.or_else(|| ctx.caller.group_name.clone());
        Err(FsFailure::new(
            RpcErrorDetail::reopen_write_session(kind, RefreshHint::default(), message),
            group_name,
            mount_epoch,
            None,
            Vec::new(),
        ))
    }

    fn read_inode(&self, inode_id: InodeId) -> MetadataResult<Option<Inode>> {
        self.storage.get_inode(inode_id)
    }

    fn read_dentry(&self, parent_inode_id: InodeId, name: &str) -> MetadataResult<Option<InodeId>> {
        self.storage.get_dentry(parent_inode_id, name)
    }

    fn read_layout(&self, inode_id: InodeId) -> MetadataResult<FileLayout> {
        self.storage.get_layout(inode_id)
    }
}

fn validate_active_write_layout(layout: &FileLayout) -> Result<(), MetadataError> {
    layout
        .validate()
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid file layout: {error}")))
}

#[cfg(test)]
mod tests {
    pub(super) use super::*;
    use crate::config::FileLayoutDefaults;
    pub(super) use crate::config::RaftConfig;
    pub(super) use crate::inode::Inode;
    pub(super) use crate::inode::InodeAttrs;
    use crate::inode::InodeKind;
    pub(super) use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID};
    use crate::raft::PublishMode;
    pub(super) use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    pub(super) use crate::service::filesystem::publish::{CloseWriteIntent, CloseWriteOutput};
    pub(super) use crate::service::filesystem::write::OpenWriteOutput;
    use crate::session_registry::{BeginAllocateBlock, BeginSessionInput, WriteSession};
    use crate::state::RouteEpoch;
    pub(super) use crate::worker::{BlockReportBlock, BlockReportBlockState, WorkerDescriptor, WorkerManager};
    pub(super) use beryl_common::error::rpc::{
        ErrorKind, InternalErrorKind, MetadataErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail, WorkerErrorKind,
    };
    pub(super) use beryl_common::header::RequestHeader;
    pub(super) use beryl_types::ids::{BlockId, BlockIndex, ClientId, InodeId, MountId, WorkerId};
    pub(super) use beryl_types::layout::FileLayout;
    pub(super) use beryl_types::lease::FencingToken;
    use beryl_types::{BlockFormatId, ContentGeneration, LeaseEpoch, WriteMode};
    pub(super) use beryl_types::{CommittedBlock, GroupName, LocatedBlock, Tier, TierFree, WorkerRunId};
    use std::ops::Deref;
    use std::sync::atomic::{AtomicU64, Ordering};
    pub(super) use std::sync::Arc;
    pub(super) use std::time::Duration;
    pub(super) use tempfile::TempDir;

    pub(super) struct MemoryStateStore {
        route_epoch: AtomicU64,
    }

    impl MemoryStateStore {
        pub(super) fn new() -> Self {
            Self {
                route_epoch: AtomicU64::new(1),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateStore for MemoryStateStore {
        async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
            Ok(RouteEpoch::new(self.route_epoch.load(Ordering::Acquire)))
        }
    }

    pub(super) struct TestFilesystem {
        filesystem: MetadataFileSystem,
        session_registry: Arc<SessionRegistry>,
        _storage_dir: Option<TempDir>,
    }

    impl Deref for TestFilesystem {
        type Target = MetadataFileSystem;

        fn deref(&self) -> &Self::Target {
            &self.filesystem
        }
    }

    impl TestFilesystem {
        pub(super) fn write_session_for_inode(&self, inode_id: InodeId) -> Option<WriteSession> {
            self.session_registry.get_session(inode_id)
        }

        pub(super) fn session_registry(&self) -> Arc<SessionRegistry> {
            Arc::clone(&self.session_registry)
        }

        pub(super) fn mount_table(&self) -> Arc<MountTable> {
            Arc::clone(&self.filesystem.mount_table)
        }

        pub(super) fn raft_node(&self) -> Arc<AppRaftNode> {
            Arc::clone(self.filesystem.raft_node.as_ref().expect("test filesystem Raft node"))
        }
    }

    pub(super) struct TestFilesystemBuilder {
        mount_table: Arc<MountTable>,
        storage: Option<Arc<RocksDBStorage>>,
        raft_node: Option<Arc<AppRaftNode>>,
        session_registry: Option<Arc<SessionRegistry>>,
        worker_manager: Option<Arc<WorkerManager>>,
        state_store: Option<Arc<dyn StateStore>>,
    }

    impl TestFilesystemBuilder {
        fn new(mount_table: Arc<MountTable>) -> Self {
            Self {
                mount_table,
                storage: None,
                raft_node: None,
                session_registry: None,
                worker_manager: None,
                state_store: None,
            }
        }

        pub(super) fn with_storage(mut self, storage: Arc<RocksDBStorage>) -> Self {
            self.storage = Some(storage);
            self
        }

        pub(super) fn mount_table(&self) -> Arc<MountTable> {
            Arc::clone(&self.mount_table)
        }

        pub(super) fn with_raft_node(mut self, raft_node: Arc<AppRaftNode>) -> Self {
            self.raft_node = Some(raft_node);
            self
        }

        pub(super) fn with_worker_manager(mut self, worker_manager: Arc<WorkerManager>) -> Self {
            self.worker_manager = Some(worker_manager);
            self
        }

        pub(super) fn with_session_registry(mut self, session_registry: Arc<SessionRegistry>) -> Self {
            self.session_registry = Some(session_registry);
            self
        }

        pub(super) fn with_state_store(mut self, state_store: Arc<dyn StateStore>) -> Self {
            self.state_store = Some(state_store);
            self
        }

        pub(super) fn build(self) -> TestFilesystem {
            let (storage, storage_dir) = match self.storage {
                Some(storage) => (storage, None),
                None => {
                    let storage_dir = TempDir::new().unwrap();
                    let storage = Arc::new(RocksDBStorage::create_for_format(storage_dir.path()).unwrap());
                    (storage, Some(storage_dir))
                }
            };
            let session_registry = self
                .session_registry
                .unwrap_or_else(|| Arc::new(SessionRegistry::default()));
            let filesystem = MetadataFileSystem::new(MetadataFileSystemDeps {
                state_store: self.state_store.unwrap_or_else(|| Arc::new(MemoryStateStore::new())),
                mount_table: self.mount_table,
                storage,
                raft_node: self.raft_node,
                session_registry: Arc::clone(&session_registry),
                worker_manager: self.worker_manager,
                metrics: None,
                readiness_gate: None,
                file_create_layout: FileLayoutDefaults::default().layout().unwrap(),
            });

            TestFilesystem {
                filesystem,
                session_registry,
                _storage_dir: storage_dir,
            }
        }
    }

    pub(super) fn request_context() -> RequestContext {
        RequestContext {
            caller: RequestHeader::new(ClientId::new(7)),
            route_epoch: None,
        }
    }

    #[test]
    fn failure_without_resolved_authority_preserves_requested_group_identity() {
        let group_name = group_name("requested");
        let ctx = RequestContext {
            caller: RequestHeader::new(ClientId::new(7)).with_group_name(group_name.clone()),
            route_epoch: None,
        };

        let failure = fs_failure_from_metadata_error(
            &ctx,
            MetadataError::NotFound("missing inode".to_string()),
            None,
            None,
            None,
        );

        assert_eq!(failure.group_name, Some(group_name));
    }

    pub(super) fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    pub(super) fn filesystem_builder_with_mount(
        mount_id: MountId,
        mount_epoch: u64,
        group_name: &GroupName,
    ) -> TestFilesystemBuilder {
        let mount_table = Arc::new(MountTable::new());
        mount_table
            .upsert(MountEntry {
                mount_id,
                mount_prefix: "/".to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch,
                namespace_owner_group_name: group_name.clone(),
                root_inode_id: ROOT_INODE_ID,
            })
            .unwrap();
        TestFilesystemBuilder::new(mount_table)
    }

    pub(super) fn worker_run_id(group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
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

    pub(super) fn register_worker_descriptor(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        address: String,
    ) {
        manager
            .upsert_descriptor(WorkerDescriptor {
                group_name: group_name.clone(),
                worker_id,
                address,
                worker_net_protocol: 1,
                fault_domain: None,
            })
            .expect("worker descriptor should register");
    }

    pub(super) fn record_worker_heartbeat(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        free_bytes: u64,
    ) {
        let descriptor = manager
            .get_descriptor(group_name, worker_id)
            .expect("worker descriptor should be registered");
        let run_id = manager
            .get_registration(group_name, worker_id)
            .map(|registration| registration.worker_run_id)
            .unwrap_or_else(|| {
                let run_id = worker_run_id(group_name, worker_id);
                manager
                    .register_worker_run(
                        group_name,
                        worker_id,
                        descriptor.address.clone(),
                        descriptor.worker_net_protocol,
                        run_id,
                        descriptor.fault_domain.clone(),
                    )
                    .expect("worker run should register");
                run_id
            });
        manager
            .record_heartbeat_with_tier_free(
                group_name,
                worker_id,
                run_id,
                1,
                &descriptor.address,
                descriptor.worker_net_protocol,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes,
                }],
            )
            .expect("heartbeat should be accepted");
        manager
            .upsert_descriptor(descriptor)
            .expect("descriptor should be restored");
    }

    pub(super) fn report_block(block_id: BlockId) -> BlockReportBlock {
        report_block_with_epoch(block_id, 1)
    }

    pub(super) fn report_block_with_epoch(block_id: BlockId, lease_epoch: u64) -> BlockReportBlock {
        report_block_with_epoch_and_len(block_id, lease_epoch, 64)
    }

    pub(super) fn report_block_with_epoch_and_len(
        block_id: BlockId,
        lease_epoch: u64,
        effective_len: u64,
    ) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch,
            block_state: BlockReportBlockState::Ready,
            effective_len,
        }
    }

    pub(super) fn report_block_with_epoch_and_state(
        block_id: BlockId,
        lease_epoch: u64,
        block_state: BlockReportBlockState,
    ) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch,
            block_state,
            effective_len: if block_state == BlockReportBlockState::Ready {
                64
            } else {
                0
            },
        }
    }

    pub(super) fn publish_report_locations_with_epoch(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        report_seq: u64,
        lease_epoch: Option<u64>,
        blocks: Vec<BlockId>,
    ) {
        let run_id = manager
            .get_registration(group_name, worker_id)
            .expect("worker registration")
            .worker_run_id;
        manager
            .receive_full_block_report(
                group_name,
                worker_id,
                run_id,
                report_seq,
                0,
                true,
                blocks
                    .into_iter()
                    .map(|block_id| {
                        lease_epoch
                            .map(|epoch| report_block_with_epoch(block_id, epoch))
                            .unwrap_or_else(|| report_block(block_id))
                    })
                    .collect(),
            )
            .expect("full block report should publish locations");
    }

    pub(super) fn publish_report_block(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        report_seq: u64,
        block: BlockReportBlock,
    ) {
        let run_id = manager
            .get_registration(group_name, worker_id)
            .expect("worker registration")
            .worker_run_id;
        manager
            .receive_full_block_report(group_name, worker_id, run_id, report_seq, 0, true, vec![block])
            .expect("full block report should publish locations");
    }

    pub(super) fn worker_manager_for_write_targets(group_name: &GroupName) -> Arc<WorkerManager> {
        let manager = Arc::new(WorkerManager::new(60_000));
        for raw in 1..=3 {
            let worker_id = WorkerId::new(raw);
            register_worker_descriptor(&manager, group_name, worker_id, format!("127.0.0.1:{}", 9000 + raw));
            record_worker_heartbeat(&manager, group_name, worker_id, 1024 * 1024);
        }
        manager
    }

    pub(super) fn assert_block_location_unavailable(failure: &FsFailure, block_id: BlockId) {
        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
        assert!(
            failure.error.message.contains(&block_id.to_string()),
            "error should include block id context: {}",
            failure.error.message
        );
    }

    pub(super) fn assert_fail(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert_eq!(error.recovery, RecoveryAction::Fail);
    }

    pub(super) fn assert_retry(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert!(matches!(error.recovery, RecoveryAction::Retry { .. }));
    }

    pub(super) fn assert_refresh_metadata(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert!(matches!(error.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    pub(super) fn refresh_hint(error: &RpcErrorDetail) -> &RefreshHint {
        match &error.recovery {
            RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => hint,
            other => panic!("expected refresh-like recovery, got {other:?}"),
        }
    }

    pub(super) fn install_write_session_with_ancestors(
        filesystem: &TestFilesystem,
        inode_id: InodeId,
        mount_id: MountId,
        ancestor_inode_ids: Vec<InodeId>,
    ) {
        let writer = ClientId::new(7);
        let lease_epoch = LeaseEpoch::new(1);
        let block_id = BlockId::new(inode_id, BlockIndex::new(0));
        let target = LocatedBlock {
            write_offset: 0,
            block_id,
            file_offset: 0,
            block_size: 64,
            worker_endpoints: Vec::new(),
            fencing_token: FencingToken {
                block_id,
                owner: writer,
                epoch: lease_epoch,
            },

            chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: Tier::Hdd,
        };
        let session_registry = filesystem.session_registry();
        let opening = session_registry
            .begin_session(BeginSessionInput {
                normalized_path: "/file".to_string(),
                inode_id,
                mount_id,
                current_lease_epoch: LeaseEpoch::new(0),
                mode: WriteMode::Overwrite,
                open_client_id: writer,
                layout: FileLayout::new(64),
                ancestor_inode_ids,
            })
            .expect("session capacity");
        let file = crate::inode::FileData {
            layout: FileLayout::new(64),
            len: 0,
            generation: ContentGeneration::default(),
            blocks: Vec::new(),
            next_index: 0,
            lease_epoch,
            last_commit: None,
        };
        opening.activate(lease_epoch, &file, None).expect("session created");
        let target_reservation = match session_registry
            .begin_allocate_block(inode_id, lease_epoch, None)
            .expect("target capacity")
        {
            BeginAllocateBlock::Reserved(reservation) => reservation,
            BeginAllocateBlock::Replay(_) => panic!("new target must reserve capacity"),
        };
        target_reservation.complete(target).expect("target installed");
    }

    pub(super) fn committed_block(block_id: BlockId, len: u64) -> CommittedBlock {
        CommittedBlock { block_id, len }
    }

    pub(super) async fn allocate_block_for_key(filesystem: &MetadataFileSystem, key: &OpenWriteOutput) -> LocatedBlock {
        let previous_block_id = filesystem
            .session_registry
            .get_session(key.inode_id)
            .and_then(|session| session.issued_targets.last().map(|target| target.block_id));
        filesystem
            .allocate_block_session(
                &request_context(),
                key.inode_id,
                key.lease_epoch,
                previous_block_id,
                Freshness::default(),
            )
            .await
            .expect("AllocateBlock should succeed")
            .payload
            .block
    }

    pub(super) async fn commit_for_key(
        filesystem: &MetadataFileSystem,
        key: &OpenWriteOutput,
        committed_blocks: Vec<CommittedBlock>,
        final_size: u64,
    ) -> FsResult<CloseWriteOutput> {
        filesystem
            .close_write_session(
                &request_context(),
                WriteHandle {
                    inode_id: key.inode_id,
                    lease_epoch: key.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks,
                    final_size,
                    expected_file_size: key.base_size,
                },
                Freshness::default(),
                key.generation,
                match filesystem
                    .session_registry
                    .get_session(key.inode_id)
                    .expect("active write session")
                    .mode
                {
                    WriteMode::Overwrite => PublishMode::ReplaceIfUnchanged,
                    WriteMode::Append => PublishMode::AppendIfUnchanged,
                },
            )
            .await
    }

    pub(super) struct WriteFlowEnv {
        pub(super) _dir: TempDir,
        pub(super) storage: Arc<RocksDBStorage>,
        pub(super) filesystem: TestFilesystem,
        pub(super) inode_id: InodeId,
        pub(super) group_name: GroupName,
    }

    pub(super) async fn write_flow_env(base_size: u64) -> WriteFlowEnv {
        build_write_flow_env(base_size, worker_manager_for_write_targets).await
    }

    async fn build_write_flow_env(
        base_size: u64,
        worker_manager: impl FnOnce(&GroupName) -> Arc<WorkerManager>,
    ) -> WriteFlowEnv {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(57 + base_size);
        let group_name = group_name(&format!("g{}", 15 + base_size));
        let inode_id = InodeId::new(9570 + base_size);
        let state_store = Arc::new(MemoryStateStore::new());
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name)
            .with_state_store(Arc::clone(&state_store) as Arc<dyn StateStore>);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager(&group_name))
            .build();

        let attrs = InodeAttrs::new();
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, beryl_types::FileLayout::new(4096));
        let file = inode.file_mut().unwrap();
        file.layout = FileLayout::new(64);
        file.len = base_size;
        let count = crate::inode::FileData::block_count(base_size, 64).unwrap();
        file.blocks = (0..count)
            .map(|index| BlockId::new(inode_id, beryl_types::BlockIndex::new(index as u32)))
            .collect();
        file.next_index = count as u64;
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(64)).unwrap();

        WriteFlowEnv {
            _dir: dir,
            storage,
            filesystem,
            inode_id,
            group_name,
        }
    }

    pub(super) fn publish_env_write_target(env: &WriteFlowEnv, target: &LocatedBlock, report_seq: u64) {
        publish_env_write_target_with_len(env, target, report_seq, 64);
    }

    pub(super) fn publish_env_write_target_with_len(
        env: &WriteFlowEnv,
        target: &LocatedBlock,
        report_seq: u64,
        effective_len: u64,
    ) {
        let worker = target.worker_endpoints.first().expect("write target worker");
        let worker_manager = env.filesystem.worker_manager.as_ref().expect("worker manager");
        publish_report_block(
            worker_manager,
            &env.group_name,
            worker.worker_id,
            report_seq,
            report_block_with_epoch_and_len(target.block_id, target.fencing_token.epoch.as_raw(), effective_len),
        );
    }

    pub(super) fn stored_generation(storage: &RocksDBStorage, inode_id: InodeId) -> ContentGeneration {
        let inode = storage.get_inode(inode_id).unwrap().expect("test inode should exist");
        match inode.kind {
            InodeKind::File(crate::inode::FileData { generation, .. }) => generation,
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    pub(super) async fn single_node_raft(
        storage: Arc<RocksDBStorage>,
        mount_table: Arc<MountTable>,
    ) -> (Arc<AppRaftNode>, Arc<AppRaftStateMachine>) {
        for mount in mount_table.list_mounts() {
            storage.put_mount(&mount).unwrap();
        }
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, Arc::clone(&state_machine), mount_table, &raft_config)
                .await
                .unwrap(),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();
        (raft_node, state_machine)
    }
}
