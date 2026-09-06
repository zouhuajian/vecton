// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Leader-local write lease, placement, and session lifecycle.

use super::command::unexpected_raft_apply_success;
use super::{
    missing_resolved_target_error, validate_active_write_layout, worker_endpoint_from_parts, AdmissionFailure,
    Freshness, FsResult, FsSuccess, MetadataFileSystem, RequestContext, WriteHandle, SUPPORTED_REPLICA_COUNT,
};
use crate::error::MetadataError;
use crate::observe;
use crate::path_resolver::{PathResolver, ResolvedPath};
use crate::placement::{PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus};
use crate::raft::{ApplySuccess, Command};
use crate::session_registry::{
    BeginAllocateBlock, BeginAllocateBlockError, BeginSessionError, BeginSessionInput, CompleteWriteTargetError,
    SessionRegistry, WriteOpeningError, WriteSession, WriteSessionError, WriteTargetLimit,
};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::ids::{BlockId, InodeId};
use beryl_types::layout::FileLayout;
use beryl_types::lease::FencingToken;
use beryl_types::{ContentGeneration, LeaseEpoch, LocatedBlock, WriteMode};

/// Exact stream facts checked against the active session and durable inode authority.
pub(crate) struct AuthorizeBlockWriteArgs {
    pub group_name: beryl_types::GroupName,
    pub worker_id: beryl_types::WorkerId,
    pub worker_run_id: beryl_types::WorkerRunId,
    pub fencing_token: FencingToken,
    pub write_offset: u64,
    pub shape: beryl_types::layout::BlockShape,
    pub tier: beryl_types::Tier,
}

/// Acquired writer authority and visible base returned to the client.
#[derive(Clone, Debug)]
pub(crate) struct OpenWriteOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) layout: FileLayout,
    pub(crate) base_size: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) generation: ContentGeneration,
    pub(crate) tail_block: Option<LocatedBlock>,
}

/// Block registered in the active allocation chain before RPC success is returned.
#[derive(Clone, Debug)]
pub(crate) struct AllocateBlockOutput {
    pub(crate) block: LocatedBlock,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RenewLeaseOutput {
    pub(crate) expires_at_ms: u64,
}

/// Caller-supplied allocation predecessor under a write handle and freshness fence.
pub(crate) struct AllocateBlockArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) previous_block_id: Option<BlockId>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct AbortFileWriteArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) freshness: Freshness,
}

pub(crate) struct RenewLeaseArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    /// Authorizes one stream from the same session registry used by publication and cleanup.
    /// A linearizable read prevents an old leader from issuing a new write authorization.
    pub(crate) async fn authorize_block_write(
        &self,
        ctx: &RequestContext,
        args: AuthorizeBlockWriteArgs,
    ) -> FsResult<u64> {
        let inode_id = args.fencing_token.block_id.inode_id;
        if let Some(failure) = self.session_write_admission_failure(ctx, inode_id) {
            return self.failure_from_admission(failure);
        }
        let result = async {
            let raft = self.raft_node.as_ref().ok_or_else(|| {
                MetadataError::ServiceUnavailable("write authorization requires Raft authority".into())
            })?;
            raft.read(true, |_| {
                let invalid = || MetadataError::PermissionDenied("block writer is no longer authorized".into());
                let session = self.session_registry.get_session(inode_id).ok_or_else(invalid)?;
                self.session_registry
                    .validate_session(inode_id, args.fencing_token.epoch)
                    .map_err(|_| invalid())?;
                if session.open_client_id != args.fencing_token.owner || session.lease_epoch != args.fencing_token.epoch
                {
                    return Err(invalid());
                }
                let inode = self.storage.get_inode(inode_id)?.ok_or_else(invalid)?;
                let mount = self.mount_table.get_mount(inode.mount_id)?.ok_or_else(invalid)?;
                if mount.namespace_owner_group_name != args.group_name
                    || ctx.caller.group_name.as_ref() != Some(&args.group_name)
                {
                    return Err(invalid());
                }
                let file = inode.file()?;
                file.validate(inode_id)?;
                if file.lease_epoch != session.lease_epoch {
                    return Err(invalid());
                }
                let target = session
                    .issued_targets
                    .iter()
                    .find(|target| target.block_id == args.fencing_token.block_id)
                    .ok_or_else(invalid)?;
                if target.fencing_token != args.fencing_token
                    || target.block_size != args.shape.block_size
                    || target.block_format_id != args.shape.block_format_id
                    || target.chunk_size != args.shape.chunk_size
                    || target.tier != args.tier
                    || args.write_offset < target.write_offset
                    || args.write_offset >= target.block_size
                {
                    return Err(invalid());
                }
                if !target.worker_endpoints.iter().any(|endpoint| {
                    endpoint.worker_id == args.worker_id && endpoint.worker_run_id == args.worker_run_id
                }) {
                    return Err(invalid());
                }
                let manager = self.worker_manager.as_ref().ok_or_else(invalid)?;
                if !manager
                    .collect_worker_placement_views(&args.group_name)
                    .iter()
                    .any(|worker| {
                        worker.worker_id == args.worker_id
                            && worker.worker_run_id == Some(args.worker_run_id)
                            && worker.lease_valid
                    })
                {
                    return Err(invalid());
                }
                let visible_len = match file
                    .blocks
                    .iter()
                    .position(|block| *block == args.fencing_token.block_id)
                {
                    Some(ordinal) => file.block_len(ordinal),
                    None => 0,
                };
                if visible_len > args.write_offset {
                    return Err(invalid());
                }
                Ok(visible_len)
            })
            .await
        }
        .await;
        match result {
            Ok(visible_len) => self.success(visible_len, Some(args.group_name), None),
            Err(error) => self.failure_from_error(ctx, error, Some(args.group_name), None),
        }
    }

    /// Admit one allocation or replay without changing file visibility.
    pub(crate) async fn allocate_block(
        &self,
        ctx: &RequestContext,
        args: AllocateBlockArgs,
    ) -> FsResult<AllocateBlockOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .allocate_block_session(
                ctx,
                handle.inode_id,
                handle.lease_epoch,
                args.previous_block_id,
                args.freshness,
            )
            .await;
        match &result {
            Ok(success) => {
                let target = &success.payload.block;
                tracing::info!(
                    target: "metadata.block",
                    op = "AllocateBlock",
                    result = "ok",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    block_id = %target.block_id,
                    block_index = target.block_id.index.as_raw(),
                    group_id = success.group_name.as_ref().map(|group| group.as_str()),
                    target_count = target.worker_endpoints.len(),
                    targets_sample = ?target.worker_endpoints.iter().take(3).map(|endpoint| endpoint.worker_id.as_raw()).collect::<Vec<_>>(),
                    inode_id = target.block_id.inode_id.as_raw(),
                    handle_inode_id = handle.inode_id.as_raw(),
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "AllocateBlock succeeded"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.block",
                op = "AllocateBlock",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                handle_inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "AllocateBlock rejected"
            ),
        }
        result
    }

    pub(crate) async fn abort_file_write(&self, ctx: &RequestContext, args: AbortFileWriteArgs) -> FsResult<()> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let handle = args.handle;
        let result = self
            .abort_session(ctx, handle.inode_id, handle.lease_epoch, args.freshness)
            .await;
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "completed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "AbortFileWrite completed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "AbortFileWrite rejected"
            ),
        }
        result
    }

    /// Renew an active write session while excluding topology-changing operations.
    ///
    /// The shared topology guard keeps ownership validation and every session
    /// expiry index update within one namespace admission interval.
    pub(crate) async fn renew_lease(&self, ctx: &RequestContext, args: RenewLeaseArgs) -> FsResult<RenewLeaseOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let _topology_guard = self.namespace_topology.read().await;
        let handle = args.handle;
        let result = self
            .renew_session(ctx, handle.inode_id, handle.lease_epoch, args.freshness)
            .await;
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "RenewLease",
                result = "completed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "RenewLease completed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "RenewLease",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "RenewLease rejected"
            ),
        }
        result
    }

    /// Resolve data-write admission from lightweight session identity only.
    pub(super) fn session_write_admission_failure(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
    ) -> Option<AdmissionFailure> {
        if let Some(session) = self.session_registry.get_session_identity(inode_id) {
            self.admission.check_data_write(ctx, session.mount_id).err()
        } else {
            self.admission.check_meta_write(ctx).err()
        }
    }
}

impl MetadataFileSystem {
    async fn abort_session(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        freshness: Freshness,
    ) -> FsResult<()> {
        let mount_id = match self.session_registry.get_session_identity(inode_id) {
            Some(session) => {
                if session.open_client_id != ctx.caller.client.client_id {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!("AbortFileWrite client does not own inode_id={inode_id}"),
                        None,
                        None,
                    );
                }
                if lease_epoch != session.lease_epoch {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!(
                            "write handle epoch mismatch for inode_id={inode_id}: expected {}, got {lease_epoch}",
                            session.lease_epoch
                        ),
                        None,
                        None,
                    );
                }
                session.mount_id
            }
            None => {
                // A restarted leader can authenticate the initial CreateFile owner
                // from its durable replay record even though local session state is gone.
                let replay = match self.storage.get_create_file_replay_for_inode(inode_id) {
                    Ok(Some(replay))
                        if replay.operation_id.client_id == ctx.caller.client.client_id
                            && replay.lease_epoch == lease_epoch =>
                    {
                        replay
                    }
                    Ok(Some(_)) | Ok(None) => return self.success((), None, None),
                    Err(error) => return self.failure_from_error(ctx, error, None, None),
                };
                replay.mount_id
            }
        };

        let (group_name, mount_epoch) = match self.freshness_validator.validate_mount_epoch(ctx, freshness, mount_id) {
            Ok(hints) => hints,
            Err(err) => return Err(err),
        };
        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "AbortFileWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let next_epoch = lease_epoch.checked_next();
        match self
            .propose_fs_write_command(
                Command::EndWriteLease {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    inode_id,
                    lease_epoch,
                },
                move |success| match success {
                    ApplySuccess::WriteLeaseEnded {
                        inode_id: returned_inode_id,
                        lease_epoch: ended_epoch,
                    } if returned_inode_id == inode_id && Some(ended_epoch) == next_epoch => Ok(()),
                    unexpected => Err(unexpected_raft_apply_success("EndWriteLease", unexpected)),
                },
            )
            .await
        {
            Ok(()) => {}
            Err(err) => return self.failure_from_error(ctx, err, group_name, mount_epoch),
        }
        self.session_registry.remove_session_if_epoch(inode_id, lease_epoch);

        self.success_with_route_epoch((), group_name, mount_epoch, route_epoch)
    }

    async fn renew_session(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        freshness: Freshness,
    ) -> FsResult<RenewLeaseOutput> {
        let session = match self.session_registry.get_session_identity(inode_id) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={inode_id}",),
                    None,
                    None,
                );
            }
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("RenewLease client does not own inode_id={inode_id}"),
                None,
                None,
            );
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={inode_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }

        let expires_at_ms =
            match self
                .session_registry
                .renew_session(inode_id, lease_epoch, ctx.caller.client.client_id)
            {
                Ok(expires_at_ms) => expires_at_ms,
                Err(WriteSessionError::Expired) => {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                        format!("lease renewal rejected for inode_id={inode_id}; write lease expired",),
                        group_name,
                        mount_epoch,
                    );
                }
                Err(error) => {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!("write session renewal rejected for inode_id={inode_id}: {error:?}"),
                        group_name,
                        mount_epoch,
                    );
                }
            };

        let route_epoch = match self.freshness_validator.authoritative_route_epoch().await {
            Ok(route_epoch) => Some(route_epoch),
            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
        };
        self.success_with_route_epoch(RenewLeaseOutput { expires_at_ms }, group_name, mount_epoch, route_epoch)
    }

    /// Install an opening, persist its fencing epoch, and atomically activate it.
    ///
    /// `ancestor_inode_ids` must be the bounded mount-root-to-file chain
    /// captured while namespace topology is stable.
    pub(super) async fn open_write_inode(
        &self,
        ctx: &RequestContext,
        normalized_path: String,
        inode_id: InodeId,
        ancestor_inode_ids: Vec<InodeId>,
        mode: WriteMode,
        freshness: Freshness,
    ) -> FsResult<OpenWriteOutput> {
        let caller_ctx = &ctx.caller;

        if let Err(message) = SessionRegistry::validate_ancestor_chain(inode_id, &ancestor_inode_ids) {
            return self.failure_from_error(ctx, MetadataError::Internal(message), None, None);
        }

        let inode = match self.read_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => {
                return self.failure_from_error(ctx, err, None, None);
            }
        };

        if inode.inode_id != inode_id {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal(format!(
                    "inode authority is corrupt for OpenWrite: key={inode_id}, value_id={}, kind={:?}, payload={:?}",
                    inode.inode_id,
                    inode.file_type(),
                    inode.file_type()
                )),
                None,
                None,
            );
        }
        if !inode.file_type().is_file() {
            return self.failure_from_error(
                ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, inode.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "OpenWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let layout = match self.read_layout(inode_id) {
            Ok(layout) => layout,
            Err(err) => {
                return self.failure_from_error(ctx, err, group_name, mount_epoch);
            }
        };
        if let Err(err) = validate_active_write_layout(&layout) {
            return self.failure_from_error(ctx, err, group_name, mount_epoch);
        }

        let base_epoch = match inode.file() {
            Ok(file) => file.lease_epoch,
            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
        };

        let opening = match self.session_registry.begin_session(BeginSessionInput {
            normalized_path,
            inode_id,
            mount_id: inode.mount_id,
            current_lease_epoch: base_epoch,
            mode,
            open_client_id: caller_ctx.client.client_id,
            layout,
            ancestor_inode_ids,
        }) {
            Ok(opening) => opening,
            Err(BeginSessionError::Busy) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Busy(format!(
                        "File already has an opening or active write session: {inode_id}"
                    )),
                    group_name,
                    mount_epoch,
                );
            }
            Err(BeginSessionError::LimitExceeded(rejection)) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::WriteSessionLimitExceeded(format!(
                        "{} limit {} reached",
                        rejection.limit.label(),
                        rejection.maximum
                    )),
                    group_name,
                    mount_epoch,
                );
            }
            Err(BeginSessionError::LeaseEpochExhausted) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::ResourceExhausted(format!("write lease epoch exhausted for inode {inode_id}")),
                    group_name,
                    mount_epoch,
                );
            }
            Err(BeginSessionError::OpeningIdExhausted) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::ResourceExhausted("leader-local write opening identity exhausted".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
            Err(BeginSessionError::InvalidAncestorChain) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Internal("validated write session ancestor chain was rejected".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };
        let lease_epoch = opening.proposed_lease_epoch();

        let lease_result = self
            .propose_fs_write_command(
                Command::AcquireWriteLease {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    inode_id,
                    expected_lease_epoch: base_epoch,
                },
                move |success| match success {
                    ApplySuccess::WriteLeaseAcquired {
                        inode_id: returned_inode_id,
                        lease_epoch: returned_lease_epoch,
                    } if returned_inode_id == inode_id && returned_lease_epoch == lease_epoch => Ok(()),
                    unexpected => Err(unexpected_raft_apply_success("AcquireWriteLease", unexpected)),
                },
            )
            .await;
        match lease_result {
            Ok(()) => {}
            Err(err) => {
                return self.failure_from_error(ctx, err, group_name, mount_epoch);
            }
        }

        // AcquireWriteLease may have ordered behind an earlier publication. Capture
        // the visible file only after its new durable epoch has been installed.
        let snapshot = match self
            .read_inode(inode_id)
            .and_then(|inode| inode.ok_or_else(|| MetadataError::NotFound("opened file disappeared".into())))
        {
            Ok(inode) => inode,
            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
        };
        let file = match snapshot.file().and_then(|file| {
            file.validate(inode_id)?;
            Ok(file)
        }) {
            Ok(file) if file.lease_epoch == lease_epoch => file,
            Ok(_) => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    "opened writer was fenced",
                    group_name,
                    mount_epoch,
                )
            }
            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
        };
        let tail_block = if mode == WriteMode::Append && file.len % u64::from(file.layout.block_size) != 0 {
            let group =
                self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "OpenWrite")?;
            match self.locate_append_tail(&group, file, ctx.caller.client.client_id, lease_epoch) {
                Ok(tail) => Some(tail),
                Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
            }
        } else {
            None
        };
        let session = match opening.activate(lease_epoch, file, tail_block) {
            Err(WriteOpeningError::TargetLimit) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::GlobalWriteTargetLimitExceeded("opening a tail exceeds the target limit".into()),
                    group_name,
                    mount_epoch,
                )
            }

            Ok(result) => result,
            Err(WriteOpeningError::Expired | WriteOpeningError::NotCurrent) => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("write opening expired before activation for inode_id={inode_id}"),
                    group_name,
                    mount_epoch,
                );
            }
            Err(WriteOpeningError::LeaseEpochMismatch { expected, got }) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Internal(format!(
                        "write opening epoch mismatch after Raft apply for inode_id={inode_id}: expected {expected}, got {got}"
                    )),
                    group_name,
                    mount_epoch,
                );
            }
        };

        self.success_with_route_epoch(open_write_output(&session), group_name, mount_epoch, route_epoch)
    }

    /// Resolves the existing tail on a live replica without allocating a new block identity.
    fn locate_append_tail(
        &self,
        group_name: &beryl_types::GroupName,
        file: &crate::inode::FileData,
        owner: beryl_types::ClientId,
        epoch: LeaseEpoch,
    ) -> Result<LocatedBlock, MetadataError> {
        let ordinal = file
            .blocks
            .len()
            .checked_sub(1)
            .ok_or_else(|| MetadataError::Internal("partial file has no tail".into()))?;
        let block_id = file.blocks[ordinal];
        let len = file.block_len(ordinal);
        let manager = self
            .worker_manager
            .as_ref()
            .ok_or_else(|| MetadataError::ServiceUnavailable("Worker manager is unavailable".into()))?;
        let request = PlacementRequest {
            group_name: group_name.clone(),
            op: PlacementOp::Read,
            block_id,
            visible_len: len,
            layout: file.layout,
            caller: None,
            existing: manager.reported_block_locations(group_name, block_id),
            exclude_workers: Vec::new(),
            target_replicas: SUPPORTED_REPLICA_COUNT,
        };
        let placement = PlacementPlanner.plan(&request, &manager.collect_worker_placement_views(group_name));
        if placement.status != PlacementStatus::Ok {
            return Err(MetadataError::ServiceUnavailable(placement.failure_message(&request)));
        }
        let tier = placement
            .workers
            .first()
            .and_then(|worker| {
                request
                    .existing
                    .iter()
                    .find(|location| {
                        location.worker_id == worker.worker_id && location.worker_run_id == worker.worker_run_id
                    })
                    .map(|location| location.tier)
            })
            .ok_or_else(|| MetadataError::ServiceUnavailable("tail placement has no tier".into()))?;
        let worker_endpoints = placement
            .workers
            .into_iter()
            .map(|worker| {
                worker_endpoint_from_parts(
                    worker.worker_id,
                    worker.endpoint,
                    worker.worker_net_protocol,
                    worker.worker_run_id,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LocatedBlock {
            block_id,
            file_offset: ordinal as u64 * u64::from(file.layout.block_size),
            block_size: u64::from(file.layout.block_size),
            worker_endpoints,
            fencing_token: FencingToken::new(block_id, owner, epoch),
            write_offset: len,
            chunk_size: file
                .layout
                .block_format_id
                .spec()
                .map_err(|e| MetadataError::Internal(e.to_string()))?
                .storage_chunk_size,
            block_format_id: file.layout.block_format_id,
            tier,
        })
    }

    /// Replay an issued target or reserve leader-local capacity before Raft allocation.
    ///
    /// The durable allocator never reuses an index. Placement failure or cancellation
    /// after Raft may leave a gap; only a completed reservation becomes replayable.
    /// No Worker data is created and no file contents are published by this method.
    pub(super) async fn allocate_block_session(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        previous_block_id: Option<BlockId>,
        freshness: Freshness,
    ) -> FsResult<AllocateBlockOutput> {
        let session = match self.session_registry.get_session_identity(inode_id) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={inode_id}"),
                    None,
                    None,
                );
            }
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("AllocateBlock client does not own inode_id={inode_id}"),
                None,
                None,
            );
        }

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "AllocateBlock")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={inode_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }
        if self.session_registry.validate_session(inode_id, lease_epoch).is_err() {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={inode_id}; reopen before AllocateBlock"),
                group_name,
                mount_epoch,
            );
        }

        let reservation = match self
            .session_registry
            .begin_allocate_block(inode_id, lease_epoch, previous_block_id)
        {
            Ok(BeginAllocateBlock::Replay(target)) => {
                return self.success_with_route_epoch(
                    AllocateBlockOutput { block: target },
                    group_name,
                    mount_epoch,
                    route_epoch,
                )
            }
            Ok(BeginAllocateBlock::Reserved(reservation)) => reservation,
            Err(BeginAllocateBlockError::Session(message)) => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("AllocateBlock session is no longer current for inode_id={inode_id}: {message}"),
                    group_name,
                    mount_epoch,
                )
            }
            Err(BeginAllocateBlockError::Internal(message)) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Internal(format!(
                        "AllocateBlock replay state is inconsistent for inode_id={inode_id}: {message}"
                    )),
                    group_name,
                    mount_epoch,
                )
            }
            Err(BeginAllocateBlockError::InvalidArgument(message)) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "AllocateBlock rejected for inode_id={inode_id}: {message}"
                    )),
                    group_name,
                    mount_epoch,
                )
            }
            Err(BeginAllocateBlockError::Pending) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Again(format!(
                        "AllocateBlock is already pending for inode_id={inode_id} and predecessor={previous_block_id:?}"
                    )),
                    group_name,
                    mount_epoch,
                )
            }
            Err(BeginAllocateBlockError::PublicationInProgress) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Again(format!(
                        "AllocateBlock cannot allocate inode_id={inode_id} while file publication is in progress"
                    )),
                    group_name,
                    mount_epoch,
                )
            }
            Err(BeginAllocateBlockError::LimitExceeded(exceeded)) => {
                let error = match exceeded.limit {
                    WriteTargetLimit::Global => MetadataError::GlobalWriteTargetLimitExceeded(format!(
                        "global limit {} reached before allocating inode_id={inode_id}",
                        exceeded.maximum
                    )),
                    WriteTargetLimit::PerSession => MetadataError::ResourceExhausted(format!(
                        "write session target limit {} reached for inode_id={inode_id}",
                        exceeded.maximum
                    )),
                };
                return self.failure_from_error(ctx, error, group_name, mount_epoch);
            }
        };
        let layout = reservation.layout();
        let file_offset = reservation.file_offset();
        let open_client_id = reservation.open_client_id();
        let storage_chunk_size = match layout.block_format_id.spec() {
            Ok(spec) => spec.storage_chunk_size,
            Err(error) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Internal(format!("active write layout has an unknown block format: {error}")),
                    group_name,
                    mount_epoch,
                )
            }
        };
        let block_id = match self.propose_block_allocation(inode_id, lease_epoch).await {
            Ok(block_id) => block_id,
            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
        };

        let worker_manager = match self.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::ServiceUnavailable("Worker manager not available".to_string()),
                    group_name,
                    mount_epoch,
                )
            }
        };
        let placement_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "AllocateBlock")?;
        let placement_views = worker_manager.collect_worker_placement_views(&placement_group_name);
        let placement_request = PlacementRequest {
            group_name: placement_group_name,
            op: PlacementOp::Write,
            block_id,
            visible_len: 0,
            layout,
            caller: ctx
                .caller
                .caller_context
                .as_ref()
                .map(CallerContextFields::from_caller_context),
            existing: Vec::new(),
            exclude_workers: Vec::new(),
            target_replicas: SUPPORTED_REPLICA_COUNT,
        };
        let placement = PlacementPlanner.plan(&placement_request, &placement_views);
        if placement.status != PlacementStatus::Ok {
            return self.failure_from_error(
                ctx,
                MetadataError::ServiceUnavailable(format!(
                    "Failed to select write placement: {}",
                    placement.failure_message(&placement_request)
                )),
                group_name,
                mount_epoch,
            );
        }
        let mut worker_endpoints = Vec::with_capacity(placement.workers.len());
        let mut selected_tier = None;
        for worker in placement.workers {
            selected_tier = selected_tier.or(worker.tier);
            let endpoint = match worker_endpoint_from_parts(
                worker.worker_id,
                worker.endpoint,
                worker.worker_net_protocol,
                worker.worker_run_id,
            ) {
                Ok(endpoint) => endpoint,
                Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
            };
            worker_endpoints.push(endpoint);
        }
        let Some(tier) = selected_tier else {
            return self.failure_from_error(
                ctx,
                MetadataError::ServiceUnavailable("selected write placement is missing storage tier".to_string()),
                group_name,
                mount_epoch,
            );
        };
        if worker_endpoints.is_empty() {
            return self.failure_from_error(
                ctx,
                MetadataError::ServiceUnavailable("selected placement has no live worker endpoints".to_string()),
                group_name,
                mount_epoch,
            );
        }
        let target = LocatedBlock {
            block_id,
            file_offset,
            block_size: u64::from(layout.block_size),
            worker_endpoints,
            fencing_token: FencingToken {
                block_id,
                owner: open_client_id,
                epoch: lease_epoch,
            },
            write_offset: 0,
            chunk_size: storage_chunk_size,
            block_format_id: layout.block_format_id,
            tier,
        };
        let target = match reservation.complete(target) {
            Ok(target) => target,
            Err(CompleteWriteTargetError::NotCurrent) => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("write session expired before AllocateBlock completed for inode_id={inode_id}"),
                    group_name,
                    mount_epoch,
                )
            }
            Err(CompleteWriteTargetError::InvalidTarget(message)) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "AllocateBlock rejected for inode_id={inode_id}: {message}"
                    )),
                    group_name,
                    mount_epoch,
                )
            }
        };
        self.success_with_route_epoch(
            AllocateBlockOutput { block: target },
            group_name,
            mount_epoch,
            route_epoch,
        )
    }
}

fn open_write_output(session: &WriteSession) -> OpenWriteOutput {
    OpenWriteOutput {
        inode_id: session.inode_id,
        lease_epoch: session.lease_epoch,
        layout: session.layout,
        base_size: session.base_size,
        expires_at_ms: session.expires_at_ms,
        generation: session.generation,
        tail_block: session
            .issued_targets
            .first()
            .filter(|target| target.write_offset > 0)
            .cloned(),
    }
}

pub(crate) struct OpenWriteArgs {
    pub(crate) path: String,
    pub(crate) mode: WriteMode,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    /// Open a path for writing under shared namespace-topology admission.
    pub(crate) async fn open_write(&self, ctx: &RequestContext, args: OpenWriteArgs) -> FsResult<OpenWriteOutput> {
        let path = args.path.clone();
        let result = self.open_write_inner(ctx, args).await;
        match &result {
            Ok(success) => {
                let payload = &success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "OpenWrite",
                    result = "opened",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    path = %path,
                    inode_id = payload.inode_id.as_raw(),
                    inode_id = payload.inode_id.as_raw(),
                    lease_epoch = payload.lease_epoch.as_raw(),
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "OpenWrite opened"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "OpenWrite",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                "OpenWrite rejected"
            ),
        }
        result
    }

    /// Resolve the path, install an opening, persist fencing, and index its ancestors.
    ///
    /// The shared guard spans resolution, Raft fencing-epoch acquisition, session
    /// creation, and the final topology safety predicate.
    async fn open_write_inner(&self, ctx: &RequestContext, args: OpenWriteArgs) -> FsResult<OpenWriteOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx) {
            return self.failure_from_admission(failure);
        }
        let _topology_guard = self.namespace_topology.read().await;
        let open_path = match PathResolver::normalize(&args.path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let resolved = match self.path_resolver.resolve_path(&open_path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let Some(inode_id) = resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            );
        };
        if let Err(failure) = self.admission.check_data_write(ctx, resolved.mount_ctx.mount_id) {
            return self.failure_from_admission(failure);
        }
        let opened = self
            .open_write_inode(
                ctx,
                open_path.clone(),
                inode_id,
                resolved.ancestor_inode_ids.clone(),
                args.mode,
                args.freshness,
            )
            .await?;

        self.finish_open_write(ctx, &open_path, &resolved, opened)
    }

    /// Revalidate the complete path identity before exposing a new write session.
    ///
    /// A previously submitted topology mutation may still apply after its RPC
    /// task is canceled and its guard is dropped. Any mismatch removes only
    /// the matching active session epoch and returns `EAGAIN`.
    fn finish_open_write(
        &self,
        ctx: &RequestContext,
        open_path: &str,
        resolved: &ResolvedPath,
        opened: FsSuccess<OpenWriteOutput>,
    ) -> FsResult<OpenWriteOutput> {
        let inode_id = opened.payload.inode_id;
        let topology_unchanged = self.path_resolver.resolve_path(open_path).is_ok_and(|current| {
            current.mount_ctx.mount_id == resolved.mount_ctx.mount_id
                && current.mount_ctx.mount_epoch == resolved.mount_ctx.mount_epoch
                && current.mount_ctx.owner_group_name == resolved.mount_ctx.owner_group_name
                && current.mount_ctx.root_inode_id == resolved.mount_ctx.root_inode_id
                && current.inode_id == Some(inode_id)
                && current.ancestor_inode_ids == resolved.ancestor_inode_ids
        });
        if !topology_unchanged {
            self.session_registry
                .remove_session_if_epoch(opened.payload.inode_id, opened.payload.lease_epoch);
            return self.failure_from_error(
                ctx,
                MetadataError::Again("namespace topology changed during OpenWrite".to_string()),
                opened.group_name,
                opened.mount_epoch,
            );
        }

        Ok(opened)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeKind;
    use crate::raft::Command;
    use crate::service::filesystem::tests::*;
    use beryl_common::header::RequestHeader;
    use beryl_types::ClientId;

    fn request_context_for(client_id: ClientId) -> RequestContext {
        RequestContext {
            caller: RequestHeader::new(client_id),
            route_epoch: None,
        }
    }

    #[tokio::test]
    async fn session_limit_plus_one_rejects_before_raft_proposal() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(49);
        let first_inode_id = InodeId::new(490);
        let second_inode_id = InodeId::new(491);
        let third_inode_id = InodeId::new(492);
        for inode_id in [first_inode_id, second_inode_id, third_inode_id] {
            storage
                .put_inode(&Inode::new_file(
                    inode_id,
                    InodeAttrs::new(),
                    mount_id,
                    beryl_types::FileLayout::new(4096),
                ))
                .unwrap();
            storage.put_layout(inode_id, FileLayout::new(4096)).unwrap();
        }

        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name("g6"));
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let session_registry = Arc::new(SessionRegistry::new(2, 1, 100, 100, 60_000));
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_session_registry(session_registry)
            .build();
        let first_client = ClientId::new(7);
        let other_client = ClientId::new(8);

        filesystem
            .open_write_inode(
                &request_context_for(first_client),
                "/first".to_string(),
                first_inode_id,
                vec![first_inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("first client session");

        let log_before_per_client_rejection = storage.get_last_log_index().unwrap();
        let applied_before_per_client_rejection = filesystem.raft_node().get_last_applied_state_id();
        let per_client_rejection = filesystem
            .open_write_inode(
                &request_context_for(first_client),
                "/second".to_string(),
                second_inode_id,
                vec![second_inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect_err("per-client limit plus one must fail");
        assert_retry(
            &per_client_rejection.error,
            ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted),
        );
        assert!(per_client_rejection.error.message.contains("per_client limit 1"));
        assert_eq!(storage.get_last_log_index().unwrap(), log_before_per_client_rejection);
        assert_eq!(
            filesystem.raft_node().get_last_applied_state_id(),
            applied_before_per_client_rejection
        );
        filesystem
            .open_write_inode(
                &request_context_for(other_client),
                "/second".to_string(),
                second_inode_id,
                vec![second_inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("another client uses remaining global capacity");

        let log_before_global_rejection = storage.get_last_log_index().unwrap();
        let applied_before_global_rejection = filesystem.raft_node().get_last_applied_state_id();
        let global_rejection = filesystem
            .open_write_inode(
                &request_context_for(ClientId::new(9)),
                "/third".to_string(),
                third_inode_id,
                vec![third_inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect_err("global limit plus one must fail");
        assert_retry(
            &global_rejection.error,
            ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted),
        );
        assert!(global_rejection.error.message.contains("global limit 2"));
        assert_eq!(storage.get_last_log_index().unwrap(), log_before_global_rejection);
        assert_eq!(
            filesystem.raft_node().get_last_applied_state_id(),
            applied_before_global_rejection
        );
    }

    #[tokio::test]
    async fn open_write_rejects_a_path_moved_by_an_already_admitted_rename() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(68);
        let old_parent_inode_id = InodeId::new(680);
        let new_parent_inode_id = InodeId::new(681);
        let file_inode_id = InodeId::new(682);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name("g20"));
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        for inode_id in [ROOT_INODE_ID, old_parent_inode_id, new_parent_inode_id] {
            storage
                .put_inode(&Inode::new_dir(inode_id, InodeAttrs::new(), mount_id))
                .unwrap();
        }
        storage
            .put_inode(&Inode::new_file(
                file_inode_id,
                InodeAttrs::new(),
                mount_id,
                beryl_types::FileLayout::new(4096),
            ))
            .unwrap();
        storage.put_dentry(ROOT_INODE_ID, "old", old_parent_inode_id).unwrap();
        storage.put_dentry(ROOT_INODE_ID, "new", new_parent_inode_id).unwrap();
        storage.put_dentry(old_parent_inode_id, "file", file_inode_id).unwrap();
        storage.put_layout(file_inode_id, FileLayout::new(64)).unwrap();

        let open_path = "/old/file";
        let resolved = filesystem.path_resolver.resolve_path(open_path).unwrap();
        let opened = filesystem
            .open_write_inode(
                &request_context(),
                open_path.to_string(),
                file_inode_id,
                resolved.ancestor_inode_ids.clone(),
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("AcquireWriteLease");
        let rename_result = filesystem
            .raft_node()
            .propose(Command::Rename {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                src_parent_inode_id: old_parent_inode_id,
                src_name: "file".to_string(),
                expected_src_inode_id: file_inode_id,
                dst_parent_inode_id: new_parent_inode_id,
                dst_name: "file".to_string(),
                expected_dst_inode_id: None,
                expected_dst_lease_epoch: None,
                flags: 0,
            })
            .await
            .expect("already admitted Rename must apply");
        assert!(matches!(rename_result, ApplySuccess::RenameApplied));

        let failure = filesystem
            .finish_open_write(&request_context(), open_path, &resolved, opened)
            .expect_err("OpenWrite must not publish a stale ancestor chain");

        assert_retry(&failure.error, ErrorKind::Metadata(MetadataErrorKind::Conflict));
        assert!(filesystem.write_session_for_inode(file_inode_id).is_none());
        let moved = filesystem.path_resolver.resolve_path("/new/file").unwrap();
        assert_eq!(moved.inode_id, Some(file_inode_id));
        assert_eq!(
            moved.ancestor_inode_ids,
            vec![ROOT_INODE_ID, new_parent_inode_id, file_inode_id]
        );
    }

    #[tokio::test]
    async fn open_write_uses_inode_identity_and_duplicate_fails_without_advancing_epoch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(51);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(510);
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                InodeAttrs::new(),
                mount_id,
                beryl_types::FileLayout::new(4096),
            ))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096)).unwrap();

        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build();

        let success = filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                inode_id,
                vec![inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open_write should succeed");

        let session = filesystem
            .write_session_for_inode(success.payload.inode_id)
            .expect("session should be stored");
        assert!(session.issued_targets.is_empty());
        assert_eq!(success.payload.inode_id, inode_id);
        assert_eq!(session.inode_id, inode_id);

        let persisted_epoch = storage
            .get_inode(inode_id)
            .unwrap()
            .and_then(|inode| match inode.kind {
                InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(lease_epoch),
                _ => None,
            })
            .expect("OpenWrite must persist the acquired lease epoch");
        let duplicate = filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                inode_id,
                vec![inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect_err("a duplicate OpenWrite must fail closed while the lease is active");
        assert_fail(&duplicate.error, ErrorKind::Metadata(MetadataErrorKind::Busy));
        let epoch_after_duplicate = storage.get_inode(inode_id).unwrap().and_then(|inode| match inode.kind {
            InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(lease_epoch),
            _ => None,
        });
        assert_eq!(epoch_after_duplicate, Some(persisted_epoch));
    }

    #[tokio::test]
    async fn allocation_failures_release_capacity_without_reusing_durable_indices() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(55);
        let group_name_value = group_name("g10");
        let inode_id = InodeId::new(550);
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                InodeAttrs::new(),
                mount_id,
                beryl_types::FileLayout::new(4096),
            ))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096)).unwrap();

        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(Arc::clone(&worker_manager))
            .build();
        let opened = filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                inode_id,
                vec![inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("OpenWrite");

        let lease_epoch = opened.payload.lease_epoch;
        let next_index = || match storage.get_inode(inode_id).unwrap().unwrap().kind {
            InodeKind::File(crate::inode::FileData { next_index, .. }) => next_index,
            _ => panic!("expected file"),
        };
        let registry = filesystem.session_registry();
        let reservation = match registry.begin_allocate_block(inode_id, lease_epoch, None).unwrap() {
            BeginAllocateBlock::Reserved(reservation) => reservation,
            BeginAllocateBlock::Replay(_) => panic!("first allocation must reserve"),
        };
        let duplicate = filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None, Freshness::default())
            .await
            .expect_err("pending duplicate must fail before Raft");
        assert_retry(&duplicate.error, ErrorKind::Metadata(MetadataErrorKind::Conflict));
        assert_eq!(next_index(), 0);
        drop(reservation);

        filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None, Freshness::default())
            .await
            .expect_err("placement without a live worker fails after durable allocation");
        assert_eq!(next_index(), 1);

        let worker_id = WorkerId::new(1);
        register_worker_descriptor(
            &worker_manager,
            &group_name_value,
            worker_id,
            "127.0.0.1:9001".to_string(),
        );
        record_worker_heartbeat(&worker_manager, &group_name_value, worker_id, 1024 * 1024);
        let block = filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None, Freshness::default())
            .await
            .expect("placement recovery releases the failed reservation")
            .payload
            .block;
        assert_eq!(block.block_id.index, BlockIndex::new(1));
        let replay = filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None, Freshness::default())
            .await
            .unwrap()
            .payload
            .block;
        assert_eq!(replay, block);
        assert_eq!(next_index(), 2);
        assert_eq!(
            filesystem.write_session_for_inode(inode_id).unwrap().issued_targets,
            vec![block]
        );
    }
}
