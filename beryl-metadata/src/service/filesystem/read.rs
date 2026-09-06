// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Namespace and file-location read operations.

use super::{
    missing_resolved_target_error, refresh_metadata_fs_failure, worker_endpoint_from_parts, FileRange, Freshness,
    FsFailure, FsResult, FsSuccess, MetadataFileSystem, RequestContext, StaleStateStatus, SUPPORTED_REPLICA_COUNT,
};
use crate::error::MetadataError;
use crate::inode::InodeAttrs;
use crate::observe;
use crate::placement::{
    PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus, ReportedBlockLocation, WorkerPlacementView,
};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::ids::{InodeId, MountId};
use beryl_types::FileType;
use beryl_types::{ContentGeneration, FileBlockLocation, GroupName};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(super) struct GetAttrInput {
    pub(super) ctx: RequestContext,
    pub(super) inode_id: InodeId,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug)]
pub(super) struct GetAttrOutput {
    pub(super) kind: FileType,
    pub(super) attrs: InodeAttrs,
    pub(super) len: u64,
}

/// Internal request for one bounded directory-authority scan.
#[derive(Clone, Debug)]
struct ReadDirInput {
    ctx: RequestContext,
    parent_inode_id: InodeId,
    cursor_key: Option<Vec<u8>>,
    max_entries: usize,
    freshness: Freshness,
}

/// One direct child joined with its authoritative inode status.
#[derive(Clone, Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) name: String,
    pub(crate) kind: FileType,
    pub(crate) attrs: InodeAttrs,
    pub(crate) len: u64,
}

#[derive(Clone, Debug, Default)]
struct ReadDirOutput {
    entries: Vec<ReadDirEntry>,
    next_cursor_key: Vec<u8>,
    eof: bool,
}

#[derive(Clone, Copy, Debug)]
struct InodeMountGuardInputs {
    mount_id: MountId,
}

#[derive(Clone, Debug)]
pub(super) struct GetFileLayoutInput {
    pub(super) ctx: RequestContext,
    pub(super) inode_id: InodeId,
    pub(super) range: Option<FileRange>,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub(super) struct GetFileLayoutOutput {
    pub(super) file_size: u64,
    pub(super) generation: Option<ContentGeneration>,
    pub(super) locations: Vec<FileBlockLocation>,
}

pub(crate) struct GetStatusArgs {
    pub(crate) path: String,
    pub(crate) freshness: Freshness,
}

/// Required namespace status returned after inode authority validation.
pub(crate) struct GetStatusOutput {
    pub(crate) kind: FileType,
    pub(crate) attrs: InodeAttrs,
    pub(crate) len: u64,
}

/// Validated filesystem arguments for one public directory-listing page.
pub(crate) struct ListStatusArgs {
    pub(crate) path: String,
    pub(crate) cursor_key: Option<Vec<u8>>,
    /// Positive server-resolved page size; wire defaults and caps are already applied.
    pub(crate) max_entries: usize,
    pub(crate) freshness: Freshness,
}

pub(crate) struct ListStatusOutput {
    pub(crate) entries: Vec<ReadDirEntry>,
    pub(crate) next_cursor_key: Vec<u8>,
    pub(crate) eof: bool,
}

pub(crate) struct OpenFileArgs {
    pub(crate) path: String,
    pub(crate) freshness: Freshness,
}

pub(crate) struct OpenFileOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) file_size: u64,
    pub(crate) generation: Option<ContentGeneration>,
}

pub(crate) enum BlockLocationsTarget {
    Path(String),
    InodeId(InodeId),
}

pub(crate) struct GetBlockLocationsArgs {
    pub(crate) target: BlockLocationsTarget,
    pub(crate) range: Option<FileRange>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct GetBlockLocationsOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) file_size: u64,
    pub(crate) generation: Option<ContentGeneration>,
    pub(crate) locations: Vec<FileBlockLocation>,
}

impl MetadataFileSystem {
    pub(crate) async fn get_status(&self, ctx: &RequestContext, args: GetStatusArgs) -> FsResult<GetStatusOutput> {
        if let Err(failure) = self.admission.check_meta_read() {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
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

        self.get_attr_resolved(GetAttrInput {
            ctx: ctx.clone(),
            inode_id,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: GetStatusOutput {
                kind: success.payload.kind,
                attrs: success.payload.attrs,
                len: success.payload.len,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    /// Resolves a directory path and returns one bounded weakly consistent page.
    pub(crate) async fn list_status(&self, ctx: &RequestContext, args: ListStatusArgs) -> FsResult<ListStatusOutput> {
        if let Err(failure) = self.admission.check_meta_read() {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
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
        self.read_dir_resolved(ReadDirInput {
            ctx: ctx.clone(),
            parent_inode_id: inode_id,
            cursor_key: args.cursor_key,
            max_entries: args.max_entries,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: ListStatusOutput {
                entries: success.payload.entries,
                next_cursor_key: success.payload.next_cursor_key,
                eof: success.payload.eof,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    pub(crate) async fn open_file(&self, ctx: &RequestContext, args: OpenFileArgs) -> FsResult<OpenFileOutput> {
        if let Err(failure) = self.admission.check_meta_read() {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
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
        if let Err(failure) = self.admission.check_data_read(resolved.mount_ctx.mount_id) {
            return self.failure_from_admission(failure);
        }

        self.get_file_layout_resolved(GetFileLayoutInput {
            ctx: ctx.clone(),
            inode_id,
            range: None,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: OpenFileOutput {
                inode_id,
                file_size: success.payload.file_size,
                generation: success.payload.generation,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    pub(crate) async fn get_block_locations(
        &self,
        ctx: &RequestContext,
        args: GetBlockLocationsArgs,
    ) -> FsResult<GetBlockLocationsOutput> {
        if let Err(failure) = self.admission.check_meta_read() {
            return self.failure_from_admission(failure);
        }

        let inode_id = match args.target {
            BlockLocationsTarget::Path(path) => {
                let resolved = match self.path_resolver.resolve_path(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => return self.failure_from_path_error(ctx, &path, err),
                };
                let Some(inode_id) = resolved.inode_id else {
                    return self.failure_from_resolved_path_error(
                        ctx,
                        missing_resolved_target_error(&resolved),
                        Some(&resolved.mount_ctx),
                    );
                };
                if let Err(failure) = self.admission.check_data_read(resolved.mount_ctx.mount_id) {
                    return self.failure_from_admission(failure);
                }
                inode_id
            }
            BlockLocationsTarget::InodeId(inode_id) => {
                let mount_id = self.plan_inode_mount(ctx, inode_id).await?.payload.mount_id;
                if let Err(failure) = self.admission.check_data_read(mount_id) {
                    return self.failure_from_admission(failure);
                }
                inode_id
            }
        };

        self.get_file_layout_resolved(GetFileLayoutInput {
            ctx: ctx.clone(),
            inode_id,
            range: args.range,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: GetBlockLocationsOutput {
                inode_id,
                file_size: success.payload.file_size,
                generation: success.payload.generation,
                locations: success.payload.locations,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    async fn validate_read_freshness_for_mount(
        &self,
        req_ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
        intent: &str,
    ) -> Result<(Option<GroupName>, Option<u64>, Option<u64>), FsFailure> {
        let (group_name, mount_epoch) = self
            .freshness_validator
            .validate_mount_epoch(req_ctx, freshness, mount_id)?;
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(req_ctx, freshness, group_name.clone(), mount_epoch, intent)
            .await?;
        match self.freshness_validator.validate_stale_state(
            req_ctx,
            self.raft_node
                .as_ref()
                .and_then(|raft_node| raft_node.get_last_applied_state_id()),
            group_name.clone(),
            mount_epoch,
        )? {
            StaleStateStatus::Ready => Ok((group_name, mount_epoch, route_epoch)),
            StaleStateStatus::UnknownLastApplied => Err(refresh_metadata_fs_failure(
                req_ctx,
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                "local applied state is unavailable for read freshness validation",
                group_name,
                mount_epoch,
                route_epoch,
                None,
            )),
        }
    }

    fn caller_context_fields(req_ctx: &RequestContext) -> Option<CallerContextFields> {
        req_ctx
            .caller
            .caller_context
            .as_ref()
            .map(CallerContextFields::from_caller_context)
    }

    fn has_usable_read_endpoint(worker: &WorkerPlacementView) -> bool {
        let Some(worker_run_id) = worker.worker_run_id else {
            return false;
        };
        worker_endpoint_from_parts(
            worker.worker_id,
            worker.endpoint.clone(),
            worker.worker_net_protocol,
            worker_run_id,
        )
        .is_ok()
    }

    fn classify_unavailable_read_location(
        reported: &[ReportedBlockLocation],
        views: &[WorkerPlacementView],
    ) -> ErrorKind {
        if reported.iter().any(|location| {
            views.iter().any(|worker| {
                worker.worker_id == location.worker_id
                    && worker.worker_run_id.is_some_and(|run| run != location.worker_run_id)
            })
        }) {
            ErrorKind::Worker(WorkerErrorKind::RunMismatch)
        } else {
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable)
        }
    }

    async fn plan_inode_mount(&self, req_ctx: &RequestContext, inode_id: InodeId) -> FsResult<InodeMountGuardInputs> {
        let inode = match self.read_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    req_ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };
        if inode.inode_id != inode_id {
            return self.failure_from_error(
                req_ctx,
                MetadataError::Internal(format!(
                    "inode authority is corrupt for GetBlockLocations: key={inode_id}, value_id={}, kind={:?}, payload={:?}",
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
                req_ctx,
                MetadataError::IsDir(format!("Inode is not a file: {inode_id}")),
                None,
                None,
            );
        }
        self.success(
            InodeMountGuardInputs {
                mount_id: inode.mount_id,
            },
            None,
            None,
        )
    }

    pub(super) async fn get_attr_resolved(&self, req: GetAttrInput) -> FsResult<GetAttrOutput> {
        let started = Instant::now();
        let result = async {
            let inode = match self.read_inode(req.inode_id) {
                Ok(Some(inode)) => inode,
                Ok(None) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                        None,
                        None,
                    );
                }
                Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
            };
            if inode.inode_id != req.inode_id {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal(format!(
                        "inode authority is corrupt for GetStatus: key={}, value_id={}, kind={:?}, payload={:?}",
                        req.inode_id,
                        inode.inode_id,
                        inode.file_type(),
                        inode.file_type()
                    )),
                    None,
                    None,
                );
            }

            let (group_name, mount_epoch, route_epoch) = self
                .validate_read_freshness_for_mount(&req.ctx, req.freshness, inode.mount_id, "GetStatus")
                .await?;
            self.success_with_route_epoch(
                GetAttrOutput {
                    kind: inode.file_type(),
                    attrs: inode.attrs.clone(),
                    len: inode.len(),
                },
                group_name,
                mount_epoch,
                route_epoch,
            )
        }
        .await;
        record_fs_read_result("get_status", started, &result);
        result
    }

    /// Reads a bounded dentry page and joins each dentry with its inode authority.
    async fn read_dir_resolved(&self, req: ReadDirInput) -> FsResult<ReadDirOutput> {
        let started = Instant::now();
        let result = async {
            let parent_inode = match self.read_inode(req.parent_inode_id) {
                Ok(Some(parent_inode)) => parent_inode,
                Ok(None) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::NotFound(format!("Parent inode not found: {}", req.parent_inode_id)),
                        None,
                        None,
                    );
                }
                Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
            };
            if parent_inode.inode_id != req.parent_inode_id {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal(format!(
                        "inode authority is corrupt for ListStatus: key={}, value_id={}, kind={:?}, payload={:?}",
                        req.parent_inode_id,
                        parent_inode.inode_id,
                        parent_inode.file_type(),
                        parent_inode.file_type()
                    )),
                    None,
                    None,
                );
            }
            if !parent_inode.file_type().is_dir() {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument(format!("Parent is not a directory: {}", req.parent_inode_id)),
                    None,
                    None,
                );
            }

            let (group_name, mount_epoch, route_epoch) = self
                .validate_read_freshness_for_mount(&req.ctx, req.freshness, parent_inode.mount_id, "ListStatus")
                .await?;

            let cursor_key = req.cursor_key.as_deref();
            let (entries, next_cursor_key, eof) =
                match self
                    .storage
                    .list_dentries_with_cursor(req.parent_inode_id, cursor_key, req.max_entries)
                {
                    Ok(result) => result,
                    Err(err) => return self.failure_from_error(&req.ctx, err, group_name, mount_epoch),
                };

            let mut dir_entries = Vec::with_capacity(entries.len());
            for (name, child_inode_id) in entries {
                let child_inode = match self.read_inode(child_inode_id) {
                    Ok(Some(child_inode)) => child_inode,
                    Ok(None) => {
                        return self.failure_from_error_with_route_epoch(
                            &req.ctx,
                            MetadataError::NotFound(format!(
                                "Directory dentry '{}' under parent inode {} points to missing inode {}",
                                name, req.parent_inode_id, child_inode_id
                            )),
                            group_name,
                            mount_epoch,
                            route_epoch,
                        );
                    }
                    Err(err) => {
                        return self.failure_from_error_with_route_epoch(
                            &req.ctx,
                            err,
                            group_name,
                            mount_epoch,
                            route_epoch,
                        );
                    }
                };
                if child_inode.inode_id != child_inode_id {
                    return self.failure_from_error_with_route_epoch(
                        &req.ctx,
                        MetadataError::Internal(format!(
                            "child inode authority is corrupt for ListStatus: key={child_inode_id}, value_id={}, kind={:?}, payload={:?}",
                            child_inode.inode_id,
                            child_inode.file_type(),
                            child_inode.file_type()
                        )),
                        group_name,
                        mount_epoch,
                        route_epoch,
                    );
                }
                dir_entries.push(ReadDirEntry {
                    name,
                    kind: child_inode.file_type(),
                    attrs: child_inode.attrs.clone(), len: child_inode.len(),
                });
            }

            self.success_with_route_epoch(
                ReadDirOutput {
                    entries: dir_entries,
                    next_cursor_key: next_cursor_key.unwrap_or_default(),
                    eof,
                },
                group_name,
                mount_epoch,
                route_epoch,
            )
        }
        .await;
        record_fs_read_result("list_status", started, &result);
        result
    }

    pub(super) async fn get_file_layout_resolved(&self, req: GetFileLayoutInput) -> FsResult<GetFileLayoutOutput> {
        let started = Instant::now();
        let result = async {
            let inode = match self.read_inode(req.inode_id) {
                Ok(Some(inode)) => inode,
                Ok(None) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                        None,
                        None,
                    );
                }
                Err(err) => {
                    return self.failure_from_error(&req.ctx, err, None, None);
                }
            };

            if inode.inode_id != req.inode_id {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal(format!(
                        "inode authority is corrupt for GetFileLayout: key={}, value_id={}, kind={:?}, payload={:?}",
                        req.inode_id,
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
                    &req.ctx,
                    MetadataError::IsDir(format!("Inode is not a file: {}", req.inode_id)),
                    None,
                    None,
                );
            }

            let (group_name, mount_epoch, route_epoch) = self
                .validate_read_freshness_for_mount(&req.ctx, req.freshness, inode.mount_id, "GetFileLayout")
                .await?;

            let file = inode.file().expect("file kind checked above");
            if let Err(error) = file.validate(req.inode_id) {
                return self.failure_from_error_with_route_epoch(&req.ctx, error, group_name, mount_epoch, route_epoch);
            }
            let layout = file.layout;
            let storage_chunk_size = layout
                .block_format_id
                .spec()
                .expect("validated layout")
                .storage_chunk_size;
            let (range_start, range_end) = match req.range {
                Some(range) => match range.offset.checked_add(range.len) {
                    Some(end) => (range.offset, end),
                    None => {
                        return self.failure_from_error_with_route_epoch(
                            &req.ctx,
                            MetadataError::InvalidArgument("range end overflows".into()),
                            group_name,
                            mount_epoch,
                            route_epoch,
                        )
                    }
                },
                None => (0, file.len),
            };
            let worker_manager = self.worker_manager.as_ref();
            let worker_group = if worker_manager.is_some() && !file.blocks.is_empty() && range_start < range_end {
                Some(self.require_worker_lookup_group(
                    &req.ctx,
                    group_name.clone(),
                    mount_epoch,
                    route_epoch,
                    "GetFileLayout",
                )?)
            } else {
                None
            };
            let caller = Self::caller_context_fields(&req.ctx);
            let mut locations = Vec::new();
            for (ordinal, block_id) in file.blocks.iter().copied().enumerate() {
                let file_offset = ordinal as u64 * u64::from(layout.block_size);
                let effective_len = file.block_len(ordinal);
                if range_start == range_end || file_offset >= range_end || file_offset + effective_len <= range_start {
                    continue;
                }
                let mut workers = Vec::new();
                if let (Some(manager), Some(worker_group)) = (worker_manager, worker_group.as_ref()) {
                    let reported = manager.reported_block_locations(worker_group, block_id);
                    let views: Vec<_> = manager
                        .collect_worker_placement_views(worker_group)
                        .into_iter()
                        .filter(Self::has_usable_read_endpoint)
                        .collect();
                    let plan = PlacementPlanner.plan(
                        &PlacementRequest {
                            group_name: worker_group.clone(),
                            op: PlacementOp::Read,
                            block_id,
                            visible_len: effective_len,
                            layout,
                            caller: caller.clone(),
                            existing: reported.clone(),
                            exclude_workers: Vec::new(),
                            target_replicas: SUPPORTED_REPLICA_COUNT,
                        },
                        &views,
                    );
                    if plan.status == PlacementStatus::NoLiveReplica {
                        return self.refresh_metadata_failure_with_hint(
                            &req.ctx,
                            Self::classify_unavailable_read_location(&reported, &views),
                            format!("no live replica holds the visible prefix of block {block_id}"),
                            group_name,
                            mount_epoch,
                            route_epoch,
                            Some(RefreshHint {
                                worker_resolve_required: true,
                                ..RefreshHint::default()
                            }),
                        );
                    }
                    for worker in plan.workers {
                        if let Ok(endpoint) = worker_endpoint_from_parts(
                            worker.worker_id,
                            worker.endpoint,
                            worker.worker_net_protocol,
                            worker.worker_run_id,
                        ) {
                            workers.push(endpoint);
                        }
                    }
                }
                locations.push(FileBlockLocation {
                    block_id,
                    file_offset,
                    len: effective_len,
                    workers,
                    block_format_id: layout.block_format_id,
                    block_size: u64::from(layout.block_size),
                    chunk_size: storage_chunk_size,
                    effective_len,
                });
            }
            let generation = Some(file.generation);

            self.success_with_route_epoch(
                GetFileLayoutOutput {
                    file_size: inode.len(),
                    generation,
                    locations,
                },
                group_name,
                mount_epoch,
                route_epoch,
            )
        }
        .await;
        record_fs_read_result("get_file_layout", started, &result);
        result
    }
}

fn record_fs_read_result<T>(operation: &str, started: Instant, result: &FsResult<T>) {
    match result {
        Ok(_) => observe::record_fs_op(operation, "ok", "none", started.elapsed().as_secs_f64()),
        Err(failure) => observe::record_fs_op(
            operation,
            "error",
            observe::rpc_error_kind(&failure.error),
            started.elapsed().as_secs_f64(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::{Inode, InodeKind};
    use crate::service::filesystem::tests::*;
    use beryl_types::{GroupStateWatermark, RaftLogId, WriteMode};

    fn seed_visible_block(storage: &RocksDBStorage, mount_id: MountId, inode_id: InodeId, block_id: BlockId) {
        let attrs = InodeAttrs::new();
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, beryl_types::FileLayout::new(4096));
        inode.kind = InodeKind::File(crate::inode::FileData {
            len: 512,
            layout: FileLayout::new(4096),
            blocks: vec![block_id],
            generation: ContentGeneration::new(1),
            lease_epoch: beryl_types::LeaseEpoch::default(),
            next_index: 1,
            last_commit: None,
        });
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096)).unwrap();
    }

    #[tokio::test]
    async fn get_file_layout_rejects_inode_key_identity_mismatch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(48);
        let storage_key_inode_id = InodeId::new(481);
        let stored_inode_id = InodeId::new(482);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g8"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage
            .put_inode_at_storage_key(
                storage_key_inode_id,
                &Inode::new_file(
                    stored_inode_id,
                    InodeAttrs::new(),
                    mount_id,
                    beryl_types::FileLayout::new(4096),
                ),
            )
            .unwrap();
        storage.put_layout(storage_key_inode_id, FileLayout::new(4096)).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id: storage_key_inode_id,
                range: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("key/value identity mismatch must fail closed");

        assert_fail(&failure.error, ErrorKind::Internal(InternalErrorKind::Internal));
        assert!(failure.error.message.contains("inode authority is corrupt"));
    }

    #[tokio::test]
    async fn get_file_layout_rejects_unavailable_or_stale_reported_locations() {
        #[derive(Clone, Copy, Debug)]
        enum Case {
            MissingReport,
            NonReady,
            ShortPrefix,
            ExpiredWorker,
        }

        for (offset, case) in [
            (0, Case::MissingReport),
            (1, Case::NonReady),
            (2, Case::ShortPrefix),
            (3, Case::ExpiredWorker),
        ] {
            let dir = TempDir::new().unwrap();
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let mount_id = MountId::new(53 + offset);
            let group_name_value = group_name("g8");
            let inode_id = InodeId::new(530 + offset);
            let block_id = BlockId::new(inode_id, BlockIndex::new(0));
            let worker_id = WorkerId::new(1);
            let worker_manager = Arc::new(WorkerManager::new(if matches!(case, Case::ExpiredWorker) {
                1_000
            } else {
                60_000
            }));

            if !matches!(case, Case::MissingReport) {
                register_worker_descriptor(
                    &worker_manager,
                    &group_name_value,
                    worker_id,
                    "127.0.0.1:9101".to_string(),
                );
                record_worker_heartbeat(&worker_manager, &group_name_value, worker_id, 1024);
            }
            match case {
                Case::MissingReport => {}
                Case::NonReady => publish_report_block(
                    &worker_manager,
                    &group_name_value,
                    worker_id,
                    1,
                    report_block_with_epoch_and_state(block_id, 41, BlockReportBlockState::Corrupt),
                ),
                Case::ShortPrefix => publish_report_locations_with_epoch(
                    &worker_manager,
                    &group_name_value,
                    worker_id,
                    1,
                    Some(40),
                    vec![block_id],
                ),
                Case::ExpiredWorker => {
                    publish_report_locations_with_epoch(
                        &worker_manager,
                        &group_name_value,
                        worker_id,
                        1,
                        Some(41),
                        vec![block_id],
                    );
                    std::thread::sleep(Duration::from_millis(1100));
                    assert_eq!(
                        worker_manager.expire_liveness(),
                        vec![(group_name_value.clone(), worker_id)]
                    );
                }
            }

            let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
                .with_storage(Arc::clone(&storage))
                .with_worker_manager(worker_manager)
                .build();
            seed_visible_block(&storage, mount_id, inode_id, block_id);

            let failure = match filesystem
                .get_file_layout_resolved(GetFileLayoutInput {
                    ctx: request_context(),
                    inode_id,
                    range: None,
                    freshness: Freshness::default(),
                })
                .await
            {
                Ok(success) => panic!("case {case:?} unexpectedly returned {success:?}"),
                Err(failure) => failure,
            };

            assert_block_location_unavailable(&failure, block_id);
        }
    }

    #[tokio::test]
    async fn list_status_rejects_stale_mount_epoch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(71);
        let parent_inode_id = InodeId::new(710);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g18"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, InodeAttrs::new(), mount_id))
            .unwrap();

        let failure = filesystem
            .read_dir_resolved(ReadDirInput {
                ctx: request_context(),
                parent_inode_id,
                cursor_key: None,
                max_entries: 100,
                freshness: Freshness {
                    mount_epoch: Some(8),
                    route_epoch: None,
                },
            })
            .await
            .expect_err("stale mount_epoch must reject ListStatus");

        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
        );
        assert_eq!(failure.group_name, Some(group_name("g18")));
        assert_eq!(failure.mount_epoch, Some(9));
    }

    #[tokio::test]
    async fn get_locations_rejects_stale_state_watermark() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open write should succeed");
        let key = open.payload;
        let target = allocate_block_for_key(&env.filesystem, &key).await;
        publish_env_write_target(&env, &target, 1);
        commit_for_key(&env.filesystem, &key, vec![committed_block(target.block_id, 64)], 64)
            .await
            .expect("commit should succeed");

        let current_state = env
            .filesystem
            .raft_node
            .as_ref()
            .and_then(|raft_node| raft_node.get_last_applied_state_id())
            .expect("commit should advance applied state");
        let mut ctx = request_context();
        ctx.caller.state.push(GroupStateWatermark::new(
            group_name("g15"),
            RaftLogId {
                term: current_state.term,
                leader_node_id: current_state.leader_node_id,
                index: current_state.index + 1,
            },
        ));

        let failure = env
            .filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx,
                inode_id: env.inode_id,
                range: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("read should reject state watermark beyond local applied state");

        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
    }
}
