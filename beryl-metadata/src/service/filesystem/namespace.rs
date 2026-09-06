// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable namespace creation, rename, and delete operations.

use super::command::unexpected_raft_apply_success;
use super::{validate_active_write_layout, Freshness, FsResult, MetadataFileSystem, RequestContext, RoutedFsWriteCtx};
use crate::error::{MetadataError, MetadataResult};
use crate::inode::InodeAttrs;
use crate::inode::{Inode, InodeKind};
use crate::observe;
use crate::path_resolver::PathResolver;
use crate::raft::{ApplySuccess, Command};
use crate::session_registry::{
    BeginCreateSession, BeginCreateSessionError, BeginCreateSessionInput, CreateFileOperationId, WriteOpeningError,
};

use beryl_types::ids::InodeId;
use beryl_types::layout::FileLayout;
use beryl_types::{ContentGeneration, LeaseEpoch};
use std::sync::atomic::Ordering;

pub(crate) struct CreateDirectoryArgs {
    pub(crate) path: String,
    pub(crate) recursive: bool,
    pub(crate) freshness: Freshness,
}

pub(crate) struct CreateDirectoryOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) attrs: InodeAttrs,
}

pub(crate) struct RenameArgs {
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
    pub(crate) flags: u32,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    /// Create a directory while excluding concurrent Rename/Delete topology changes.
    pub(crate) async fn create_directory(
        &self,
        ctx: &RequestContext,
        args: CreateDirectoryArgs,
    ) -> FsResult<CreateDirectoryOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx) {
            return self.failure_from_admission(failure);
        }
        let _topology_guard = self.namespace_topology.read().await;

        let CreateDirectoryArgs {
            path,
            recursive,
            freshness,
        } = args;
        let attrs = InodeAttrs::new();
        let path = match PathResolver::normalize(&path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let result = if recursive {
            self.create_directory_recursive(ctx, &path, attrs, freshness).await
        } else {
            self.create_directory_once(ctx, &path, attrs, freshness).await
        };
        let parent_inode_id = self
            .path_resolver
            .resolve_path(&path)
            .ok()
            .and_then(|resolved| resolved.parent_inode_id);

        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "CreateDirectory",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                inode_id = success.payload.inode_id.as_raw(),
                parent_inode_id = parent_inode_id.map(|id| id.as_raw()),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "CreateDirectory committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CreateDirectory",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                parent_inode_id = parent_inode_id.map(|id| id.as_raw()),
                "CreateDirectory rejected"
            ),
        }
        result
    }

    async fn create_directory_once(
        &self,
        ctx: &RequestContext,
        path: &str,
        attrs: InodeAttrs,
        freshness: Freshness,
    ) -> FsResult<CreateDirectoryOutput> {
        let resolved = match self.path_resolver.resolve_path(path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, path, err),
        };
        let (Some(parent_inode_id), Some(name)) = (resolved.parent_inode_id, resolved.name.clone()) else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&resolved.mount_ctx),
            );
        };

        self.execute_create_directory(
            ctx,
            Command::CreateDirectory {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                root_inode_id: parent_inode_id,
                components: vec![name],
                attrs,
                recursive: false,
            },
            freshness,
        )
        .await
    }

    async fn create_directory_recursive(
        &self,
        ctx: &RequestContext,
        path: &str,
        attrs: InodeAttrs,
        freshness: Freshness,
    ) -> FsResult<CreateDirectoryOutput> {
        let (mount_ctx, components) = match self.path_resolver.resolve_mount_components(path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, path, err),
        };
        if components.is_empty() {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&mount_ctx),
            );
        }

        self.execute_create_directory(
            ctx,
            Command::CreateDirectory {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                root_inode_id: mount_ctx.root_inode_id,
                components,
                attrs,
                recursive: true,
            },
            freshness,
        )
        .await
    }

    async fn execute_create_directory(
        &self,
        ctx: &RequestContext,
        command: Command,
        freshness: Freshness,
    ) -> FsResult<CreateDirectoryOutput> {
        let root_inode_id = match &command {
            Command::CreateDirectory { root_inode_id, .. } => *root_inode_id,
            _ => unreachable!("execute_create_directory requires CreateDirectory"),
        };
        let routed = match self.route_ctx_for_write(ctx, &[root_inode_id], freshness) {
            Ok(routed) => routed,
            Err(err) => return Err(err),
        };
        let result = match self
            .propose_fs_write_command(command, |success| match success {
                ApplySuccess::DirectoryEnsured { inode_id, attrs } => Ok(CreateDirectoryOutput { inode_id, attrs }),
                unexpected => Err(unexpected_raft_apply_success("CreateDirectory", unexpected)),
            })
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(ctx, err, Some(routed.group_name.clone()), Some(routed.mount_epoch));
            }
        };
        self.success(result, Some(routed.group_name), Some(routed.mount_epoch))
    }

    /// Resolve and commit a rename under exclusive namespace-topology admission.
    ///
    /// The exclusive guard prevents new path-bound sessions or creates from
    /// crossing the subtree writer checks and the Raft proposal.
    pub(crate) async fn rename(&self, ctx: &RequestContext, args: RenameArgs) -> FsResult<()> {
        if let Err(failure) = self.admission.check_meta_write(ctx) {
            return self.failure_from_admission(failure);
        }
        let _topology_guard = self.namespace_topology.write().await;
        let src_path = match PathResolver::normalize(&args.src_path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.src_path, err),
        };
        let dst_path = match PathResolver::normalize(&args.dst_path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.dst_path, err),
        };
        let (src_resolved, dst_resolved) = match self.path_resolver.resolve_rename(&src_path, &dst_path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        };
        let (Some(src_parent_inode_id), Some(src_name)) = (src_resolved.parent_inode_id, src_resolved.name.clone())
        else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot rename a mount root".to_string()),
                Some(&src_resolved.mount_ctx),
            );
        };
        let Some(expected_src_inode_id) = src_resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::NotFound(format!("Source not found: {src_path}")),
                Some(&src_resolved.mount_ctx),
            );
        };
        let (Some(dst_parent_inode_id), Some(dst_name)) = (dst_resolved.parent_inode_id, dst_resolved.name.clone())
        else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot rename to a mount root".to_string()),
                Some(&dst_resolved.mount_ctx),
            );
        };
        let expected_dst_lease_epoch = match dst_resolved.inode_id {
            Some(dst_inode_id) => match self.read_inode(dst_inode_id) {
                Ok(Some(inode)) => match &inode.kind {
                    InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(*lease_epoch),
                    _ => None,
                },
                Ok(None) => None,
                Err(err) => return self.failure_from_resolved_path_error(ctx, err, Some(&dst_resolved.mount_ctx)),
            },
            None => None,
        };

        let result = self
            .execute_rename(
                ctx,
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id,
                    src_name,
                    expected_src_inode_id,
                    dst_parent_inode_id,
                    dst_name,
                    expected_dst_inode_id: dst_resolved.inode_id,
                    expected_dst_lease_epoch,
                    flags: args.flags,
                },
                args.freshness,
            )
            .await;

        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "Rename",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                src = %args.src_path,
                dst = %args.dst_path,
                parent_inode_id = src_parent_inode_id.as_raw(),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "Rename committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "Rename",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                src = %args.src_path,
                dst = %args.dst_path,
                parent_inode_id = src_parent_inode_id.as_raw(),
                "Rename rejected"
            ),
        }
        result
    }

    /// Validate subtree writer preconditions and propose one resolved rename.
    ///
    /// An active writer in the source subtree or overwritten target subtree
    /// fails closed with `EBUSY`; persisted inode identity and fencing-epoch
    /// preconditions are still revalidated by Raft apply.
    async fn execute_rename(
        &self,
        request_ctx: &RequestContext,
        command: Command,
        freshness: Freshness,
    ) -> FsResult<()> {
        let Command::Rename {
            src_parent_inode_id,
            src_name: _,
            expected_src_inode_id,
            dst_parent_inode_id,
            ref dst_name,
            expected_dst_inode_id: _,
            expected_dst_lease_epoch: _,
            flags,
            ..
        } = command
        else {
            unreachable!("execute_rename requires Rename")
        };
        let supported_mask: u32 = 0x1;
        if flags & !supported_mask != 0 {
            return self.failure_from_error(
                request_ctx,
                MetadataError::NotSupported(format!("Unsupported rename flags: {flags}")),
                None,
                None,
            );
        }

        let src_parent_inode = match self.read_inode(src_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::NotFound(format!("Source parent inode not found: {src_parent_inode_id}")),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(request_ctx, err, None, None),
        };
        let dst_parent_inode = match self.read_inode(dst_parent_inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::NotFound(format!("Destination parent inode not found: {dst_parent_inode_id}")),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(request_ctx, err, None, None),
        };

        if src_parent_inode.mount_id != dst_parent_inode.mount_id {
            if let Some(metrics) = &self.metrics {
                metrics
                    .fs_write_cross_mount_rename_exdev_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            let (group_name, mount_epoch) = self
                .freshness_validator
                .mount_hints_for_mount(src_parent_inode.mount_id);
            return self.failure_from_error(
                request_ctx,
                MetadataError::CrossMountRename(format!(
                    "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                    src_parent_inode.mount_id, dst_parent_inode.mount_id
                )),
                group_name,
                mount_epoch,
            );
        }

        let ctx = match self.route_ctx_for_write(request_ctx, &[src_parent_inode_id, dst_parent_inode_id], freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        if self.has_active_write_under(expected_src_inode_id) {
            return self.failure_from_error(
                request_ctx,
                MetadataError::Busy(format!(
                    "Rename source contains an active write lease: {expected_src_inode_id}"
                )),
                Some(ctx.group_name.clone()),
                Some(ctx.mount_epoch),
            );
        }

        match self.read_dentry(dst_parent_inode_id, dst_name) {
            Ok(Some(dst_inode_id)) => match self.read_inode(dst_inode_id) {
                Ok(Some(inode)) => {
                    let has_active_write = if inode.file_type().is_file() {
                        self.has_active_write(dst_inode_id)
                    } else {
                        self.has_active_write_under(dst_inode_id)
                    };
                    if has_active_write {
                        return self.failure_from_error(
                            request_ctx,
                            MetadataError::Busy(format!(
                                "Rename target contains an active write lease: {}",
                                dst_inode_id
                            )),
                            Some(ctx.group_name.clone()),
                            Some(ctx.mount_epoch),
                        );
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    return self.failure_from_error(
                        request_ctx,
                        err,
                        Some(ctx.group_name.clone()),
                        Some(ctx.mount_epoch),
                    );
                }
            },
            Ok(None) => {}
            Err(err) => {
                return self.failure_from_error(request_ctx, err, Some(ctx.group_name.clone()), Some(ctx.mount_epoch));
            }
        }

        let result = self
            .propose_fs_write_command(command, |success| match success {
                ApplySuccess::RenameApplied => Ok(()),
                unexpected => Err(unexpected_raft_apply_success("Rename", unexpected)),
            })
            .await;

        self.routed_unit_result(request_ctx, &ctx, result)
    }

    fn routed_unit_result(
        &self,
        request_ctx: &RequestContext,
        ctx: &RoutedFsWriteCtx,
        result: MetadataResult<()>,
    ) -> FsResult<()> {
        match result {
            Ok(()) => self.success((), Some(ctx.group_name.clone()), Some(ctx.mount_epoch)),
            Err(error) => {
                self.failure_from_error(request_ctx, error, Some(ctx.group_name.clone()), Some(ctx.mount_epoch))
            }
        }
    }
}

pub(crate) struct CreateFileArgs {
    pub(crate) path: String,
    pub(crate) freshness: Freshness,
}

/// Atomic CreateFile result needed to begin client-side writes immediately.
pub(crate) struct CreatedFileOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) layout: FileLayout,
    pub(crate) expires_at_ms: u64,
    pub(crate) generation: ContentGeneration,
}

impl MetadataFileSystem {
    /// Create a file while excluding concurrent Rename/Delete topology changes.
    pub(crate) async fn create_file(&self, ctx: &RequestContext, args: CreateFileArgs) -> FsResult<CreatedFileOutput> {
        let path = args.path.clone();
        let result = self.create_file_inner(ctx, args).await;
        match &result {
            Ok(success) => {
                let payload = &success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "CreateFile",
                    result = "committed",
                    error_code = "none",
                    client_id = %ctx.caller.client.client_id,
                    call_id = %ctx.caller.client.call_id,
                    path = %path,
                    inode_id = payload.inode_id.as_raw(),
                    layout_block_size = payload.layout.block_size,
                    block_format_id = payload.layout.block_format_id.as_raw(),
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "CreateFile committed"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CreateFile",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %path,
                "CreateFile rejected"
            ),
        }
        result
    }

    async fn create_file_inner(&self, ctx: &RequestContext, args: CreateFileArgs) -> FsResult<CreatedFileOutput> {
        if let Err(failure) = self.admission.check_meta_write(ctx) {
            return self.failure_from_admission(failure);
        }
        let _topology_guard = self.namespace_topology.read().await;

        let CreateFileArgs { path, freshness } = args;
        if ctx.caller.deadline.has_passed() {
            return self.failure_from_path_error(
                ctx,
                &path,
                MetadataError::InvalidArgument("CreateFile request deadline has expired".to_string()),
            );
        }

        let path = match PathResolver::normalize(&path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let resolved = match self.path_resolver.resolve_path(&path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let (Some(parent_inode_id), Some(_)) = (resolved.parent_inode_id, resolved.name.as_ref()) else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&resolved.mount_ctx),
            );
        };
        if let Err(failure) = self.admission.check_data_write(ctx, resolved.mount_ctx.mount_id) {
            return self.failure_from_admission(failure);
        }
        let mut parent_ancestor_inode_ids = resolved.ancestor_inode_ids.clone();
        if resolved.inode_id.is_some() {
            parent_ancestor_inode_ids.pop();
        }
        if parent_ancestor_inode_ids.last() != Some(&parent_inode_id) {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal("CreateFile resolved parent chain is inconsistent".to_string()),
                Some(resolved.mount_ctx.owner_group_name),
                Some(resolved.mount_ctx.mount_epoch),
            );
        }
        let success = self
            .create_resolved(
                ctx,
                path,
                resolved.relative_components,
                parent_inode_id,
                parent_ancestor_inode_ids,
                freshness,
            )
            .await?;
        Ok(success)
    }

    /// Reserve a session, commit atomic file authority, and activate the exact reservation.
    async fn create_resolved(
        &self,
        request_ctx: &RequestContext,
        normalized_path: String,
        relative_components: Vec<String>,
        parent_inode_id: InodeId,
        parent_ancestor_inode_ids: Vec<InodeId>,
        freshness: Freshness,
    ) -> FsResult<CreatedFileOutput> {
        let layout = self.file_create_layout;
        if let Err(err) = validate_active_write_layout(&layout) {
            return self.failure_from_error(request_ctx, err, None, None);
        }

        let ctx = match self.route_ctx_for_write(request_ctx, &[parent_inode_id], freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };

        let operation_id = CreateFileOperationId {
            client_id: request_ctx.caller.client.client_id,
            call_id: request_ctx.caller.client.call_id,
        };
        let request_deadline_ms = match u64::try_from(request_ctx.caller.deadline.as_unix_ms()) {
            Ok(deadline) => deadline,
            Err(_) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::InvalidArgument("CreateFile deadline must be non-negative".to_string()),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
        };
        let opening = match self.session_registry.begin_create_session(BeginCreateSessionInput {
            operation_id,
            request_deadline_ms,
            normalized_path: normalized_path.clone(),
            mount_id: ctx.mount_id,
            expected_mount_epoch: ctx.mount_epoch,
            mount_root_inode_id: ctx.mount_root_inode_id,
            open_client_id: request_ctx.caller.client.client_id,
            parent_ancestor_inode_ids,
        }) {
            Ok(BeginCreateSession::Replay(session)) => {
                return self.success(
                    CreatedFileOutput {
                        inode_id: session.inode_id,
                        lease_epoch: session.lease_epoch,
                        layout: session.layout,
                        expires_at_ms: session.expires_at_ms,
                        generation: session.generation,
                    },
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Ok(BeginCreateSession::Reserved(opening)) => opening,
            Err(BeginCreateSessionError::Pending) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::Again("the same CreateFile operation is still pending".to_string()),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Err(BeginCreateSessionError::PathBusy) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::Again("another CreateFile operation is pending for this path".to_string()),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Err(BeginCreateSessionError::IdentityMismatch) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::InvalidArgument(
                        "CreateFile operation identity was reused for another request".to_string(),
                    ),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Err(BeginCreateSessionError::LimitExceeded(rejection)) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::WriteSessionLimitExceeded(format!(
                        "{} limit {} reached",
                        rejection.limit.label(),
                        rejection.maximum
                    )),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Err(BeginCreateSessionError::OpeningIdExhausted) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::ResourceExhausted("leader-local write opening identity exhausted".to_string()),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Err(BeginCreateSessionError::InvalidAncestorChain) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::Internal("validated CreateFile parent chain was rejected".to_string()),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
        };
        let session_expires_at_ms = opening.expires_at_ms();

        let result = match self
            .propose_fs_write_command(
                Command::CreateFile {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    operation_id,
                    request_deadline_ms,
                    session_expires_at_ms,
                    normalized_path,
                    mount_id: ctx.mount_id,
                    expected_mount_epoch: ctx.mount_epoch,
                    mount_root_inode_id: ctx.mount_root_inode_id,
                    relative_components,
                    attrs: InodeAttrs::new(),
                    layout,
                },
                |success| match success {
                    ApplySuccess::FileCreated {
                        inode_id,
                        layout,
                        lease_epoch,
                        expires_at_ms,
                        generation,
                    } => Ok((inode_id, layout, lease_epoch, expires_at_ms, generation)),
                    unexpected => Err(unexpected_raft_apply_success("CreateFile", unexpected)),
                },
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                return self.failure_from_error(request_ctx, err, Some(ctx.group_name.clone()), Some(ctx.mount_epoch));
            }
        };

        let (inode_id, layout, lease_epoch, expires_at_ms, generation) = result;
        let session = match opening.activate(inode_id, lease_epoch, expires_at_ms, layout, generation) {
            Ok(session) => session,
            Err(WriteOpeningError::Expired | WriteOpeningError::NotCurrent | WriteOpeningError::TargetLimit) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::Again(format!(
                        "CreateFile committed but its local write opening expired for inode {inode_id}"
                    )),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
            Err(WriteOpeningError::LeaseEpochMismatch { expected, got }) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::Internal(format!(
                        "CreateFile opening epoch mismatch for inode {inode_id}: expected {expected}, got {got}"
                    )),
                    Some(ctx.group_name),
                    Some(ctx.mount_epoch),
                );
            }
        };

        self.success(
            CreatedFileOutput {
                inode_id: session.inode_id,
                lease_epoch: session.lease_epoch,
                layout: session.layout,
                expires_at_ms: session.expires_at_ms,
                generation: session.generation,
            },
            Some(ctx.group_name),
            Some(ctx.mount_epoch),
        )
    }
}

pub(crate) struct DeleteArgs {
    pub(crate) path: String,
    pub(crate) recursive: bool,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    /// Resolves and submits a namespace delete under metadata write admission.
    ///
    /// The exclusive topology guard prevents new path-bound writes and creates
    /// from crossing the subtree activity check and the Raft proposal.
    ///
    /// A successful result means the namespace mutation committed. Physical
    /// block reclamation follows the configured cleanup grace asynchronously.
    pub(crate) async fn delete(&self, ctx: &RequestContext, args: DeleteArgs) -> FsResult<()> {
        if let Err(failure) = self.admission.check_meta_write(ctx) {
            return self.failure_from_admission(failure);
        }
        let _topology_guard = self.namespace_topology.write().await;

        let path = match PathResolver::normalize(&args.path) {
            Ok(path) => path,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let resolved = match self.path_resolver.resolve_path(&path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &path, err),
        };
        let (Some(parent_inode_id), Some(_)) = (resolved.parent_inode_id, resolved.name.as_ref()) else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&resolved.mount_ctx),
            );
        };
        let Some(target_inode_id) = resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::NotFound(format!("Entry not found: {path}")),
                Some(&resolved.mount_ctx),
            );
        };
        let result = self
            .delete_resolved(
                ctx,
                parent_inode_id,
                resolved.relative_components,
                target_inode_id,
                args.recursive,
                args.freshness,
            )
            .await;

        match &result {
            Ok(_) => tracing::info!(
                target: "metadata.state",
                op = "Delete",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %args.path,
                inode_id = target_inode_id.as_raw(),
                parent_inode_id = parent_inode_id.as_raw(),
                recursive = args.recursive,
                "Delete committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "Delete",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                path = %args.path,
                parent_inode_id = parent_inode_id.as_raw(),
                recursive = args.recursive,
                "Delete rejected"
            ),
        }
        result
    }

    /// Commits one resolved delete with bounded path and exact inode preconditions.
    ///
    /// A bounded ancestor index rejects an active writer anywhere below the
    /// target. The exclusive topology guard held by the caller keeps that
    /// leader-local decision ordered with path-bound write admission.
    ///
    /// Physical cleanup is deliberately decoupled from this mutation. Worker
    /// reports later rediscover unreachable replicas, wait for the configured
    /// grace, and revalidate authority before dispatch.
    async fn delete_resolved(
        &self,
        request_ctx: &RequestContext,
        parent_inode_id: InodeId,
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        recursive: bool,
        freshness: Freshness,
    ) -> FsResult<()> {
        let ctx = match self.route_ctx_for_write(request_ctx, &[parent_inode_id], freshness) {
            Ok(ctx) => ctx,
            Err(err) => return Err(err),
        };
        if self.has_active_write_under(expected_inode_id) {
            return self.failure_from_error(
                request_ctx,
                MetadataError::Busy(format!(
                    "Delete target contains an active write lease: {expected_inode_id}"
                )),
                Some(ctx.group_name.clone()),
                Some(ctx.mount_epoch),
            );
        }
        let expected_file_lease_epoch = match self.read_inode(expected_inode_id) {
            Ok(Some(inode)) if inode.file_type().is_file() => Some(Self::file_lease_epoch(&inode)),
            Ok(Some(_)) => None,
            Ok(None) => {
                return self.failure_from_error(
                    request_ctx,
                    MetadataError::NotFound(format!("Delete target inode not found: {expected_inode_id}")),
                    Some(ctx.group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
            Err(err) => {
                return self.failure_from_error(request_ctx, err, Some(ctx.group_name.clone()), Some(ctx.mount_epoch));
            }
        };
        let command = Command::Delete {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            mount_id: ctx.mount_id,
            expected_mount_epoch: ctx.mount_epoch,
            mount_root_inode_id: ctx.mount_root_inode_id,
            relative_components,
            expected_inode_id,
            expected_file_lease_epoch,
            recursive,
        };

        let result = self
            .propose_fs_write_command(command, |success| match success {
                ApplySuccess::DeleteApplied => Ok(()),
                unexpected => Err(unexpected_raft_apply_success("Delete", unexpected)),
            })
            .await;
        self.routed_unit_result(request_ctx, &ctx, result)
    }

    /// Read the persisted file fencing epoch used by delete apply preconditions.
    fn file_lease_epoch(inode: &Inode) -> LeaseEpoch {
        match &inode.kind {
            InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => *lease_epoch,
            _ => LeaseEpoch::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::filesystem::tests::*;
    use crate::service::filesystem::OpenWriteArgs;
    use beryl_types::WriteMode;

    #[tokio::test]
    async fn recursive_delete_rejects_active_writer_at_any_descendant_depth() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(68);
        let group_name_value = group_name("g20");
        let parent_inode_id = ROOT_INODE_ID;
        let root_inode_id = InodeId::new(681);
        let nested_inode_id = InodeId::new(682);
        let file_inode_id = InodeId::new(683);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        for inode_id in [parent_inode_id, root_inode_id, nested_inode_id] {
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
        storage.put_dentry(parent_inode_id, "root", root_inode_id).unwrap();
        storage.put_dentry(root_inode_id, "nested", nested_inode_id).unwrap();
        storage.put_dentry(nested_inode_id, "file", file_inode_id).unwrap();
        storage.put_layout(file_inode_id, FileLayout::new(64)).unwrap();
        install_write_session_with_ancestors(
            &filesystem,
            file_inode_id,
            mount_id,
            vec![root_inode_id, nested_inode_id, file_inode_id],
        );

        let failure = filesystem
            .delete_resolved(
                &request_context(),
                parent_inode_id,
                vec!["root".to_string()],
                root_inode_id,
                true,
                Freshness::default(),
            )
            .await
            .unwrap_err();

        assert_fail(&failure.error, ErrorKind::Metadata(MetadataErrorKind::Busy));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "root").unwrap(),
            Some(root_inode_id)
        );
        assert!(storage.get_inode(file_inode_id).unwrap().is_some());
    }

    #[tokio::test]
    async fn concurrent_open_write_and_delete_have_one_linearized_outcome() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(69);
        let group_name_value = group_name("g21");
        let file_inode_id = InodeId::new(690);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        storage
            .put_inode(&Inode::new_dir(ROOT_INODE_ID, InodeAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                file_inode_id,
                InodeAttrs::new(),
                mount_id,
                beryl_types::FileLayout::new(4096),
            ))
            .unwrap();
        storage.put_dentry(ROOT_INODE_ID, "file", file_inode_id).unwrap();
        storage.put_layout(file_inode_id, FileLayout::new(64)).unwrap();

        let open_ctx = request_context();
        let delete_ctx = request_context();
        let (open_result, delete_result) = tokio::join!(
            filesystem.open_write(
                &open_ctx,
                OpenWriteArgs {
                    path: "/file".to_string(),
                    mode: WriteMode::Overwrite,
                    freshness: Freshness::default(),
                },
            ),
            filesystem.delete(
                &delete_ctx,
                DeleteArgs {
                    path: "/file".to_string(),
                    recursive: false,
                    freshness: Freshness::default(),
                },
            ),
        );

        match (open_result, delete_result) {
            (Ok(_), Err(failure)) => {
                assert_fail(&failure.error, ErrorKind::Metadata(MetadataErrorKind::Busy));
                assert_eq!(storage.get_dentry(ROOT_INODE_ID, "file").unwrap(), Some(file_inode_id));
            }
            (Err(_), Ok(_)) => {
                assert_eq!(storage.get_dentry(ROOT_INODE_ID, "file").unwrap(), None);
                assert!(storage.get_inode(file_inode_id).unwrap().is_none());
            }
            (open_result, delete_result) => {
                panic!(
                    "OpenWrite/Delete must linearize to one success and one rejection: open={:?}, delete={:?}",
                    open_result.is_ok(),
                    delete_result.is_ok()
                );
            }
        }
    }

    #[tokio::test]
    async fn rename_rejects_source_directory_with_active_descendant_writer() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(67);
        let group_name_value = group_name("g19");
        let parent_inode_id = InodeId::new(670);
        let source_inode_id = InodeId::new(671);
        let nested_inode_id = InodeId::new(672);
        let file_inode_id = InodeId::new(673);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();

        for inode_id in [parent_inode_id, source_inode_id, nested_inode_id] {
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
        storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
        storage.put_dentry(source_inode_id, "nested", nested_inode_id).unwrap();
        storage.put_dentry(nested_inode_id, "file", file_inode_id).unwrap();
        storage.put_layout(file_inode_id, FileLayout::new(64)).unwrap();
        install_write_session_with_ancestors(
            &filesystem,
            file_inode_id,
            mount_id,
            vec![source_inode_id, nested_inode_id, file_inode_id],
        );

        let failure = filesystem
            .execute_rename(
                &request_context(),
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    expected_src_inode_id: source_inode_id,
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "renamed".to_string(),
                    expected_dst_inode_id: None,
                    expected_dst_lease_epoch: None,
                    flags: 0,
                },
                Freshness::default(),
            )
            .await
            .unwrap_err();

        assert_fail(&failure.error, ErrorKind::Metadata(MetadataErrorKind::Busy));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "source").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(storage.get_dentry(parent_inode_id, "renamed").unwrap(), None);
    }

    #[tokio::test]
    async fn rename_rejects_target_directory_with_active_descendant_writer() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(69);
        let group_name_value = group_name("g21");
        let parent_inode_id = InodeId::new(690);
        let source_inode_id = InodeId::new(691);
        let target_inode_id = InodeId::new(692);
        let nested_inode_id = InodeId::new(693);
        let file_inode_id = InodeId::new(694);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_storage(Arc::clone(&storage))
            .build();

        for inode_id in [parent_inode_id, source_inode_id, target_inode_id, nested_inode_id] {
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
        storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
        storage.put_dentry(parent_inode_id, "target", target_inode_id).unwrap();
        storage.put_dentry(target_inode_id, "nested", nested_inode_id).unwrap();
        storage.put_dentry(nested_inode_id, "file", file_inode_id).unwrap();
        storage.put_layout(file_inode_id, FileLayout::new(64)).unwrap();
        install_write_session_with_ancestors(
            &filesystem,
            file_inode_id,
            mount_id,
            vec![target_inode_id, nested_inode_id, file_inode_id],
        );

        let failure = filesystem
            .execute_rename(
                &request_context(),
                Command::Rename {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    expected_src_inode_id: source_inode_id,
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    expected_dst_inode_id: Some(target_inode_id),
                    expected_dst_lease_epoch: None,
                    flags: 0,
                },
                Freshness::default(),
            )
            .await
            .unwrap_err();

        assert_fail(&failure.error, ErrorKind::Metadata(MetadataErrorKind::Busy));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "source").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(target_inode_id)
        );
    }
}
