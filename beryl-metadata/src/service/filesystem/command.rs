// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared routing and Raft proposal boundary for filesystem writes.

use super::{fs_failure_from_metadata_error, Freshness, FsFailure, MetadataFileSystem, RequestContext};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{ApplySuccess, Command};
use crate::session_registry::WritePublication;
use beryl_types::ids::{BlockId, InodeId, MountId};
use beryl_types::{ContentGeneration, GroupName, LeaseEpoch};
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::debug;

#[derive(Clone, Debug)]
pub(super) struct RoutedFsWriteCtx {
    pub(super) mount_id: MountId,
    /// Durable path anchor copied into path-addressed Raft commands.
    pub(super) mount_root_inode_id: InodeId,
    /// Namespace owner group selected from the resolved mount.
    pub(super) group_name: GroupName,
    pub(super) mount_epoch: u64,
}

impl MetadataFileSystem {
    pub(super) fn route_ctx_for_write(
        &self,
        req_ctx: &RequestContext,
        parent_inode_ids: &[InodeId],
        freshness: Freshness,
    ) -> Result<RoutedFsWriteCtx, FsFailure> {
        self.route_ctx_for_write_with_error_hints(req_ctx, parent_inode_ids, freshness, None, None)
    }

    pub(super) fn route_ctx_for_write_with_error_hints(
        &self,
        req_ctx: &RequestContext,
        parent_inode_ids: &[InodeId],
        freshness: Freshness,
        error_group_name: Option<GroupName>,
        error_mount_epoch: Option<u64>,
    ) -> Result<RoutedFsWriteCtx, FsFailure> {
        let ctx = match self.route_fs_write_ctx(parent_inode_ids) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(fs_failure_from_metadata_error(
                    req_ctx,
                    err,
                    error_group_name,
                    error_mount_epoch,
                    None,
                ));
            }
        };

        if let Err(failure) = self
            .freshness_validator
            .validate_mount_epoch(req_ctx, freshness, ctx.mount_id)
        {
            if let Some(metrics) = &self.metrics {
                metrics
                    .fs_write_mount_epoch_mismatch_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Err(failure);
        }
        Ok(ctx)
    }

    pub(super) fn route_fs_write_ctx(&self, parent_inode_ids: &[InodeId]) -> MetadataResult<RoutedFsWriteCtx> {
        let parent_inode_id = parent_inode_ids
            .first()
            .ok_or_else(|| MetadataError::InvalidArgument("No parent inode provided".to_string()))?;
        let parent_inode = self
            .read_inode(*parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;

        let mount_id = parent_inode.mount_id;
        for other_parent in parent_inode_ids.iter().skip(1) {
            let inode = self
                .read_inode(*other_parent)?
                .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", other_parent)))?;
            if inode.mount_id != mount_id {
                return Err(MetadataError::CrossMountRename(
                    "cross-mount operation is not allowed".to_string(),
                ));
            }
        }

        let mount_entry = self
            .mount_table
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)))?;

        debug!(
            mount_id = %mount_id.as_raw(),
            owner_group_name = %mount_entry.namespace_owner_group_name,
            mount_epoch = mount_entry.mount_epoch,
            "FS write routed to mount namespace owner group"
        );

        if let Some(ref metrics) = self.metrics {
            metrics.fs_write_routed_total.fetch_add(1, Ordering::Relaxed);
        }

        Ok(RoutedFsWriteCtx {
            mount_id,
            mount_root_inode_id: mount_entry.root_inode_id,
            group_name: mount_entry.namespace_owner_group_name,
            mount_epoch: mount_entry.mount_epoch,
        })
    }

    /// Propose one filesystem command and record its fully validated outcome.
    ///
    /// The Raft node has already converted committed application rejections to
    /// `MetadataError`. `decode_success` must accept only the exact success
    /// variant and identity expected by the submitted command, so FS metrics
    /// cannot report success before that invariant is checked.
    pub(super) async fn propose_fs_write_command<T>(
        &self,
        command: Command,
        decode_success: impl FnOnce(ApplySuccess) -> MetadataResult<T>,
    ) -> MetadataResult<T> {
        let started = Instant::now();
        let operation_name = command.operation_name();
        let result = match self.propose_write_command(command).await {
            Ok(success) => decode_success(success),
            Err(error) => Err(error),
        };
        record_fs_write_result(operation_name, started, &result);
        result
    }

    /// Allocate the next durable block ordinal for one exact inode and lease epoch.
    pub(super) async fn propose_block_allocation(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    ) -> MetadataResult<BlockId> {
        let started = Instant::now();
        let command = Command::AllocateBlock { inode_id, lease_epoch };
        let operation_name = command.operation_name();
        let response = match self.propose_write_command(command).await {
            Ok(response) => response,
            Err(error) => {
                observe::record_fs_op(
                    operation_name,
                    "error",
                    observe::metadata_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                return Err(error);
            }
        };
        match response {
            ApplySuccess::BlockAllocated(block_id) if block_id.inode_id == inode_id => {
                observe::record_fs_op(operation_name, "ok", "none", started.elapsed().as_secs_f64());
                Ok(block_id)
            }
            unexpected => {
                let error = unexpected_raft_apply_success("AllocateBlock", unexpected);
                observe::record_fs_op(
                    operation_name,
                    "error",
                    observe::metadata_error_kind(&error),
                    started.elapsed().as_secs_f64(),
                );
                Err(error)
            }
        }
    }

    /// Keep submitted visibility changes alive independently of the RPC waiter.
    ///
    /// The session pins unpublished blocks and namespace exclusion until apply
    /// finishes or an ordered lease fence rules out a delayed publication.
    pub(super) async fn propose_file_publication(
        &self,
        command: Command,
        mut publication: WritePublication,
    ) -> MetadataResult<ContentGeneration> {
        let (inode_id, lease_epoch, file_size, closes) = match &command {
            Command::CommitFile {
                inode_id, publication, ..
            } => (*inode_id, publication.lease_epoch, publication.target_size, true),
            Command::PublishFile {
                inode_id, publication, ..
            } => (*inode_id, publication.lease_epoch, publication.target_size, false),
            _ => unreachable!("file publication command required"),
        };
        publication.mark_submitted().map_err(MetadataError::Again)?;
        let ended_epoch = lease_epoch.checked_next();
        let operation_name = command.operation_name();
        let fence = self.propose_write_command(Command::EndWriteLease {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id,
            lease_epoch,
        });
        let proposal = self.propose_write_command(command);
        tokio::spawn(async move {
            let started = Instant::now();
            let result = match proposal.await {
                Ok(ApplySuccess::FileCommitted {
                    inode_id: returned,
                    lease_epoch,
                    generation,
                }) if closes && returned == inode_id && Some(lease_epoch) == ended_epoch => Ok(generation),
                Ok(ApplySuccess::FilePublished {
                    inode_id: returned,
                    generation,
                }) if !closes && returned == inode_id => Ok(generation),
                Ok(unexpected) => Err(unexpected_raft_apply_success(operation_name, unexpected)),
                Err(error) => Err(error),
            };
            let result = match result {
                Ok(generation) => {
                    if closes {
                        publication.complete_commit();
                        Ok(generation)
                    } else {
                        publication
                            .complete_sync(generation, file_size)
                            .map(|()| generation)
                            .map_err(MetadataError::Internal)
                    }
                }
                Err(error) => {
                    // Transport or apply-worker failure may leave the mutation
                    // queued. Only ordered authority may release its GC pin.
                    if matches!(fence.await,
                        Ok(ApplySuccess::WriteLeaseEnded { inode_id: returned, lease_epoch })
                            if returned == inode_id && Some(lease_epoch) >= ended_epoch)
                    {
                        publication.complete_commit();
                    }
                    Err(error)
                }
            };
            record_fs_write_result(operation_name, started, &result);
            result
        })
        .await
        .map_err(|error| MetadataError::Internal(format!("file publication task failed: {error}")))?
    }

    /// Own the proposal dependencies so submitted publication can outlive its RPC waiter.
    fn propose_write_command(
        &self,
        command: Command,
    ) -> impl std::future::Future<Output = MetadataResult<ApplySuccess>> + Send + 'static {
        let raft_node = self.raft_node.clone();
        let metrics = self.metrics.clone();
        async move {
            let raft_node = raft_node.ok_or_else(|| MetadataError::Internal("Raft node not available".to_string()))?;
            if let Some(metrics) = &metrics {
                metrics.fs_raft_appends_total.fetch_add(1, Ordering::Relaxed);
                match &command {
                    Command::CreateFile { .. } => {
                        metrics.fs_raft_appends_create.fetch_add(1, Ordering::Relaxed);
                    }
                    Command::CreateDirectory { .. } => {
                        metrics.fs_raft_appends_mkdir.fetch_add(1, Ordering::Relaxed);
                    }
                    Command::Rename { .. } => {
                        metrics.fs_raft_appends_rename.fetch_add(1, Ordering::Relaxed);
                    }
                    Command::PublishFile { .. } | Command::CommitFile { .. } => {
                        metrics.fs_raft_appends_publish.fetch_add(1, Ordering::Relaxed);
                    }
                    Command::BootstrapNamespace { .. }
                    | Command::Delete { .. }
                    | Command::AcquireWriteLease { .. }
                    | Command::AllocateBlock { .. }
                    | Command::EndWriteLease { .. }
                    | Command::RegisterWorkerDescriptor { .. }
                    | Command::ReclaimDetachedRoots { .. } => {}
                }
            }

            raft_node.propose(command).await
        }
    }
}

fn record_fs_write_result<T>(operation_name: &'static str, started: Instant, result: &MetadataResult<T>) {
    match result {
        Ok(_) => {
            observe::record_fs_op(operation_name, "ok", "none", started.elapsed().as_secs_f64());
        }
        Err(error) => {
            observe::record_fs_op(
                operation_name,
                "error",
                observe::metadata_error_kind(error),
                started.elapsed().as_secs_f64(),
            );
        }
    }
}

/// Fail closed when Raft returns a success variant for a different command.
pub(super) fn unexpected_raft_apply_success(operation_name: &'static str, success: ApplySuccess) -> MetadataError {
    MetadataError::Internal(format!(
        "{operation_name} Raft command returned unexpected success: {success:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::{unexpected_raft_apply_success, ApplySuccess, MetadataError};

    #[test]
    fn mismatched_raft_success_fails_closed() {
        let error = unexpected_raft_apply_success("CreateFile", ApplySuccess::RaftEntryApplied);

        assert!(matches!(
            error,
            MetadataError::Internal(message)
                if message.contains("CreateFile") && message.contains("unexpected success")
        ));
    }
}
