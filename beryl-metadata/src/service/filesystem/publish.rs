// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable file visibility publication for sync and commit.

use super::{
    fs_failure_from_metadata_error, Freshness, FsFailure, FsResult, MetadataFileSystem, RequestContext, WriteHandle,
};
use crate::error::{MetadataError, MetadataResult};
use crate::inode::FilePublication;
use crate::observe;
use crate::raft::{Command, PublishMode};
use crate::session_registry::{BeginWritePublicationError, WritePublication, WriteSession};
use crate::worker::{PublishReadyConflict, PublishReadyStatus, PublishReadyTarget};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use beryl_types::ids::{InodeId, MountId};
use beryl_types::{CommittedBlock, ContentGeneration, GroupName, LeaseEpoch, WriteMode};
use std::collections::{HashMap, HashSet};

/// Caller-supplied visible content boundary, frozen before the first RPC attempt.
#[derive(Clone, Debug)]
pub(super) struct CloseWriteIntent {
    pub(super) committed_blocks: Vec<CommittedBlock>,
    pub(super) final_size: u64,
    pub(super) expected_file_size: u64,
}

impl CloseWriteIntent {
    /// Preserve the caller's frozen publication preconditions for durable replay.
    fn publication(&self, handle: WriteHandle, generation: ContentGeneration, mode: PublishMode) -> FilePublication {
        FilePublication {
            blocks: self.committed_blocks.clone(),
            target_size: self.final_size,
            expected_generation: generation,
            expected_file_size: self.expected_file_size,
            lease_epoch: handle.lease_epoch,
            mode,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SyncWriteOutput {
    pub(crate) synced_size: u64,
    pub(crate) generation: Option<ContentGeneration>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CloseWriteOutput {
    pub(crate) committed_size: u64,
}

pub(crate) struct CommitFileArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) final_size: u64,
    pub(crate) freshness: Freshness,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) publish_mode: PublishMode,
}

pub(crate) struct SyncWriteArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) target_size: u64,
    pub(crate) freshness: Freshness,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) publish_mode: PublishMode,
}

impl MetadataFileSystem {
    pub(crate) async fn commit_file(&self, ctx: &RequestContext, args: CommitFileArgs) -> FsResult<CloseWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let inode_id = args.handle.inode_id;
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.inode_id != inode_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block inode_id does not match request".to_string()),
                None,
                None,
            );
        }

        let handle = args.handle;
        let committed_block_count = args.committed_blocks.len();
        let committed_bytes: u64 = args
            .committed_blocks
            .iter()
            .fold(0u64, |sum, block| sum.saturating_add(block.len));
        let result = self
            .close_write_session(
                ctx,
                handle,
                CloseWriteIntent {
                    committed_blocks: args.committed_blocks,
                    final_size: args.final_size,
                    expected_file_size: args.expected_file_size,
                },
                args.freshness,
                args.expected_generation,
                args.publish_mode,
            )
            .await;
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "CommitFile",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = inode_id.as_raw(),
                final_size = args.final_size,
                committed_block_count,
                committed_bytes,
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "CommitFile committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CommitFile",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = inode_id.as_raw(),
                final_size = args.final_size,
                committed_block_count,
                committed_bytes,
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "CommitFile rejected"
            ),
        }
        result
    }

    pub(crate) async fn sync_write(&self, ctx: &RequestContext, args: SyncWriteArgs) -> FsResult<SyncWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let inode_id = args.handle.inode_id;
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.inode_id != inode_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block inode_id does not match request".to_string()),
                None,
                None,
            );
        }

        let handle = args.handle;
        self.sync_write_session(
            ctx,
            handle,
            CloseWriteIntent {
                committed_blocks: args.committed_blocks,
                final_size: args.target_size,
                expected_file_size: args.expected_file_size,
            },
            args.freshness,
            args.expected_generation,
            args.publish_mode,
        )
        .await
    }

    fn publish_mode_for_session(session: &WriteSession) -> PublishMode {
        match session.mode {
            WriteMode::Overwrite => PublishMode::ReplaceIfUnchanged,
            WriteMode::Append => PublishMode::AppendIfUnchanged,
        }
    }

    fn active_publish_session(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        operation: &'static str,
    ) -> Result<Option<WriteSession>, FsFailure> {
        let Some(session) = self.session_registry.get_session(inode_id) else {
            return Ok(None);
        };
        let invalid = |message| match self.session_terminal_failure::<()>(
            ctx,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
            message,
            None,
            None,
        ) {
            Err(failure) => failure,
            Ok(_) => unreachable!("session_terminal_failure always returns Err"),
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return Err(invalid(format!("{operation} client does not own inode_id={inode_id}")));
        }
        if session.lease_epoch != lease_epoch {
            return Err(invalid(format!(
                "{operation} publish precondition does not match the active session"
            )));
        }
        Ok(Some(session))
    }

    /// Freeze the current issued-target sequence before validating publication.
    fn begin_write_publication(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<WritePublication, FsFailure> {
        let publication = match self.session_registry.begin_publication(inode_id, lease_epoch) {
            Ok(publication) => publication,
            Err(BeginWritePublicationError::Session(message)) => {
                return Err(self
                    .session_terminal_failure::<()>(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!("{operation} write session is no longer current: {message}"),
                        None,
                        None,
                    )
                    .expect_err("session_terminal_failure always returns Err"));
            }
            Err(BeginWritePublicationError::AllocateBlockPending) => {
                return Err(self
                    .failure_from_error::<()>(
                        ctx,
                        MetadataError::Again(format!(
                            "{operation} cannot freeze inode_id={inode_id} while AllocateBlock is pending"
                        )),
                        None,
                        None,
                    )
                    .expect_err("failure_from_error always returns Err"));
            }
            Err(BeginWritePublicationError::PublicationInProgress) => {
                return Err(self
                    .failure_from_error::<()>(
                        ctx,
                        MetadataError::Again(format!(
                            "another file publication is already in progress for inode_id={inode_id}"
                        )),
                        None,
                        None,
                    )
                    .expect_err("failure_from_error always returns Err"));
            }
            Err(BeginWritePublicationError::PublicationIdExhausted) => {
                return Err(self
                    .failure_from_error::<()>(
                        ctx,
                        MetadataError::Internal("write publication identity exhausted".to_string()),
                        None,
                        None,
                    )
                    .expect_err("failure_from_error always returns Err"));
            }
        };
        let session = publication.session();
        if session.open_client_id != ctx.caller.client.client_id
            || Self::publish_mode_for_session(session) != publish_mode
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} publish precondition does not match the active session"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        Ok(publication)
    }

    /// Resolve a SyncWrite postcondition; this never proves that CommitFile ran.
    ///
    /// This is state-equivalence recovery, not historical request replay. Once
    /// the requested postcondition is visible at the next content generation,
    /// preconditions such as the original publish mode are no longer
    /// distinguishable without persisting request history.
    fn resolve_synced_state(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        intent: &CloseWriteIntent,
        expected_generation: ContentGeneration,
        mode: PublishMode,
    ) -> MetadataResult<Option<(InodeId, MountId, ContentGeneration, LeaseEpoch)>> {
        let inode = self
            .read_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        if inode.inode_id != inode_id {
            return Err(MetadataError::Internal("inode key mismatch".into()));
        }
        let file = inode.file()?;
        file.validate(inode_id)?;
        if file.lease_epoch != lease_epoch && lease_epoch.checked_next() != Some(file.lease_epoch) {
            return Err(MetadataError::LeaseFenced {
                expected: file.lease_epoch,
                got: lease_epoch,
            });
        }
        let payload = intent.publication(WriteHandle { inode_id, lease_epoch }, expected_generation, mode);
        let matches = payload.matches_visible(file)?;
        if matches
            && (expected_generation.checked_next() == Some(file.generation)
                || (file.generation == expected_generation && payload.blocks.is_empty()))
        {
            return Ok(Some((inode_id, inode.mount_id, file.generation, file.lease_epoch)));
        }
        if file.generation != expected_generation {
            return Err(MetadataError::Again("content generation changed".into()));
        }
        Ok(None)
    }

    async fn completed_publish_hints(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
        operation: &'static str,
    ) -> Result<(Option<GroupName>, Option<u64>, Option<u64>), FsFailure> {
        let (group_name, mount_epoch) = self
            .freshness_validator
            .validate_mount_epoch(ctx, freshness, mount_id)?;
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, operation)
            .await?;
        Ok((group_name, mount_epoch, route_epoch))
    }

    /// Require current writer and Worker-run evidence for every changed block.
    /// Historical full blocks are absent from the incremental publication payload.
    fn publication_ready_targets(
        &self,
        session: &WriteSession,
        blocks: &[CommittedBlock],
        expected_generation: ContentGeneration,
    ) -> MetadataResult<Vec<PublishReadyTarget>> {
        let inode = self
            .read_inode(session.inode_id)?
            .ok_or_else(|| MetadataError::NotFound("inode missing".into()))?;
        if inode.file()?.generation != expected_generation {
            return Err(MetadataError::StaleState("content generation changed".into()));
        }
        let issued: HashMap<_, _> = session
            .issued_targets
            .iter()
            .map(|target| (target.block_id, target))
            .collect();
        blocks
            .iter()
            .map(|block| {
                let target = issued
                    .get(&block.block_id)
                    .ok_or_else(|| MetadataError::InvalidArgument("block was not issued to this writer".into()))?;
                if target.fencing_token.epoch != session.lease_epoch {
                    return Err(MetadataError::InvalidArgument("target writer epoch mismatch".into()));
                }
                Ok(PublishReadyTarget {
                    target: (*target).clone(),
                    effective_len: block.len,
                })
            })
            .collect()
    }

    fn publish_ready_refresh_failure(
        &self,
        ctx: &RequestContext,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: &GroupName,
        epochs: (Option<u64>, Option<u64>),
        worker_resolve_required: bool,
    ) -> FsFailure {
        match self.refresh_metadata_failure_with_hint::<()>(
            ctx,
            kind,
            message,
            Some(group_name.clone()),
            epochs.0,
            epochs.1,
            Some(RefreshHint {
                worker_resolve_required,
                ..Default::default()
            }),
        ) {
            Err(failure) => failure,
            Ok(_) => unreachable!("refresh_metadata_failure_with_hint always returns Err"),
        }
    }

    fn publish_ready_conflict_failure(
        &self,
        ctx: &RequestContext,
        conflict: PublishReadyConflict,
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> FsFailure {
        match conflict {
            PublishReadyConflict::MissingWriteEndpoint { block_id } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!("block {block_id} has no metadata-authorized write endpoint"),
                group_name,
                (mount_epoch, route_epoch),
                true,
            ),
            PublishReadyConflict::WorkerRunMismatch {
                block_id,
                worker_id,
                expected,
                current,
            } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                format!(
                    "worker run changed before publishing block {block_id}: worker_id={}, expected={expected}, current={current:?}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                true,
            ),
            PublishReadyConflict::EndpointMismatch { block_id, worker_id } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!(
                    "worker endpoint changed before publishing block {block_id}: worker_id={}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                true,
            ),
            PublishReadyConflict::LeaseEpochMismatch {
                block_id,
                worker_id,
                expected,
                reported,
            } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::Fencing),
                format!(
                    "worker reported the wrong writer epoch for block {block_id}: worker_id={}, expected={expected}, reported={reported}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                false,
            ),
            PublishReadyConflict::UnreadableBlock {
                block_id,
                worker_id,
                state,
            } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::Corrupt),
                format!(
                    "worker reported an unreadable block before publication: block_id={block_id}, worker_id={}, state={state:?}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                false,
            ),
        }
    }

    /// Wait until every new target has current Ready evidence or the request
    /// deadline expires.
    ///
    /// The watch receiver is created before the first snapshot check, so a
    /// report applied between checking and awaiting remains observable. No
    /// WorkerManager lock is held across the await.
    async fn wait_for_publish_ready(
        &self,
        ctx: &RequestContext,
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        targets: &[PublishReadyTarget],
    ) -> Result<(), FsFailure> {
        if targets.is_empty() {
            return Ok(());
        }
        let Some(worker_manager) = self.worker_manager.as_ref() else {
            return Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "worker observations are unavailable for file publication",
                group_name,
                (mount_epoch, route_epoch),
                false,
            ));
        };

        let mut observations = worker_manager.subscribe_publication_observations();
        loop {
            let pending_block_id = match worker_manager.check_publish_ready(group_name, targets) {
                PublishReadyStatus::Ready => return Ok(()),
                PublishReadyStatus::Pending { block_id } => block_id,
                PublishReadyStatus::Conflict(conflict) => {
                    return Err(self.publish_ready_conflict_failure(
                        ctx,
                        conflict,
                        group_name,
                        mount_epoch,
                        route_epoch,
                    ));
                }
            };

            let remaining = ctx.caller.deadline.remaining();
            if remaining.is_zero() {
                return Err(self.publish_ready_refresh_failure(
                    ctx,
                    ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    format!("deadline expired while waiting for Ready report for block {pending_block_id}"),
                    group_name,
                    (mount_epoch, route_epoch),
                    false,
                ));
            }
            match tokio::time::timeout(remaining, observations.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(self
                        .failure_from_error_with_route_epoch::<()>(
                            ctx,
                            MetadataError::Internal("worker publication observation channel closed".to_string()),
                            Some(group_name.clone()),
                            mount_epoch,
                            route_epoch,
                        )
                        .expect_err("failure_from_error_with_route_epoch always returns Err"));
                }
                Err(_) => {
                    return Err(self.publish_ready_refresh_failure(
                        ctx,
                        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                        format!("deadline expired while waiting for Ready report for block {pending_block_id}"),
                        group_name,
                        (mount_epoch, route_epoch),
                        false,
                    ));
                }
            }
        }
    }

    /// Perform the non-waiting Ready recheck immediately before proposal.
    fn require_publish_ready(
        &self,
        ctx: &RequestContext,
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        targets: &[PublishReadyTarget],
    ) -> Result<(), FsFailure> {
        if targets.is_empty() {
            return Ok(());
        }
        let Some(worker_manager) = self.worker_manager.as_ref() else {
            return Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "worker observations are unavailable for file publication",
                group_name,
                (mount_epoch, route_epoch),
                false,
            ));
        };
        match worker_manager.check_publish_ready(group_name, targets) {
            PublishReadyStatus::Ready => Ok(()),
            PublishReadyStatus::Pending { block_id } => Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!("Ready evidence changed before publishing block {block_id}"),
                group_name,
                (mount_epoch, route_epoch),
                false,
            )),
            PublishReadyStatus::Conflict(conflict) => {
                Err(self.publish_ready_conflict_failure(ctx, conflict, group_name, mount_epoch, route_epoch))
            }
        }
    }

    /// Reject a new publication after the caller's deadline has expired.
    ///
    /// Durable replay is resolved before this guard. This check protects the
    /// final proposal boundary after all asynchronous authority and Ready
    /// revalidation has completed.
    fn require_publish_deadline(
        &self,
        ctx: &RequestContext,
        group_name: Option<&GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> Result<(), FsFailure> {
        if !ctx.caller.deadline.has_passed() {
            return Ok(());
        }
        match group_name {
            Some(group_name) => Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "deadline expired before file publication",
                group_name,
                (mount_epoch, route_epoch),
                false,
            )),
            None => Err(self
                .failure_from_error_with_route_epoch::<()>(
                    ctx,
                    MetadataError::Again("deadline expired before file publication".to_string()),
                    None,
                    mount_epoch,
                    route_epoch,
                )
                .expect_err("expired publication deadline must fail")),
        }
    }

    /// Revalidate leader-local session and lease state after an asynchronous
    /// Ready wait. A caller may proceed only with the same publication
    /// preconditions that were used to select the target set.
    async fn revalidate_publish_session(
        &self,
        ctx: &RequestContext,
        publication: &WritePublication,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<WriteSession, FsFailure> {
        let expected = publication.session();
        if let Some(failure) = self.session_write_admission_failure(ctx, expected.inode_id) {
            return Err(self
                .failure_from_admission::<()>(failure)
                .expect_err("failure_from_admission always returns Err"));
        }
        let current = publication.revalidate().map_err(|message| {
            self.session_terminal_failure::<()>(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("{operation} write publication changed while waiting for Ready block reports: {message}"),
                None,
                None,
            )
            .expect_err("session_terminal_failure always returns Err")
        })?;
        if current.open_client_id != ctx.caller.client.client_id
            || Self::publish_mode_for_session(&current) != publish_mode
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} publish precondition does not match the active session"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        if current.inode_id != expected.inode_id
            || current.mount_id != expected.mount_id
            || current.base_size != expected.base_size
            || current.generation != expected.generation
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} write session changed while waiting for Ready block reports"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        if self
            .session_registry
            .validate_session(current.inode_id, current.lease_epoch)
            .is_err()
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("{operation} lease expired while waiting for Ready block reports"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        Ok(current)
    }

    async fn sync_write_session(
        &self,
        ctx: &RequestContext,
        handle: WriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_generation: ContentGeneration,
        publish_mode: PublishMode,
    ) -> FsResult<SyncWriteOutput> {
        let inode_id = handle.inode_id;
        let lease_epoch = handle.lease_epoch;
        let active_session = match self.active_publish_session(ctx, inode_id, lease_epoch, "SyncWrite") {
            Ok(session) => session,
            Err(failure) => return Err(failure),
        };
        match self.resolve_synced_state(inode_id, lease_epoch, &intent, expected_generation, publish_mode) {
            Ok(Some((_inode_id, mount_id, generation, _stored_lease_epoch))) => {
                let publication = if let Some(session) = &active_session {
                    Some(self.begin_write_publication(
                        ctx,
                        inode_id,
                        lease_epoch,
                        Self::publish_mode_for_session(session),
                        "SyncWrite",
                    )?)
                } else {
                    None
                };
                if publication.as_ref().is_some_and(|publication| {
                    let session = publication.session();
                    session.generation != expected_generation && session.generation != generation
                }) {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        "SyncWrite content generation does not match the active session".to_string(),
                        None,
                        None,
                    );
                }
                let (group_name, mount_epoch, route_epoch) = self
                    .completed_publish_hints(ctx, freshness, mount_id, "SyncWrite")
                    .await?;
                if let Some(publication) = publication {
                    if let Err(message) = publication.complete_sync(generation, intent.final_size) {
                        return self.failure_from_error(ctx, MetadataError::Internal(message), group_name, mount_epoch);
                    }
                }
                return self.success_with_route_epoch(
                    SyncWriteOutput {
                        synced_size: intent.final_size,
                        generation: Some(generation),
                    },
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
            Ok(None) => {}
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        }
        let publication = match active_session {
            Some(_) => match self.begin_write_publication(ctx, inode_id, lease_epoch, publish_mode, "SyncWrite") {
                Ok(publication) => publication,
                Err(failure) => return Err(failure),
            },
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={}", inode_id),
                    None,
                    None,
                );
            }
        };
        let session = publication.session().clone();
        if session.generation != expected_generation {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                "SyncWrite publish precondition does not match the active session".to_string(),
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
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "SyncWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        for block in &intent.committed_blocks {
            if block.block_id.inode_id != session.inode_id {
                return self.failure_from_error_with_route_epoch(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "SyncWrite committed block inode_id {} does not match write handle inode_id {}",
                        block.block_id.inode_id, session.inode_id
                    )),
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
        }

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={}: expected {}, got {}",
                    inode_id, session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .session_registry
            .validate_session(session.inode_id, lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={}", inode_id,),
                group_name,
                mount_epoch,
            );
        }

        let intent = CloseWriteIntent {
            committed_blocks: intent.committed_blocks.clone(),
            final_size: intent.final_size,
            expected_file_size: intent.expected_file_size,
        };
        let blocks = match Self::validate_committed_blocks(&intent, &session) {
            Ok(blocks) => blocks,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name, mount_epoch));
            }
        };
        let worker_lookup_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "SyncWrite")?;
        let new_targets = match self.publication_ready_targets(&session, &intent.committed_blocks, expected_generation)
        {
            Ok(targets) => targets,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name, mount_epoch));
            }
        };
        self.wait_for_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)
            .await?;

        let revalidated_freshness = Freshness {
            mount_epoch,
            route_epoch,
        };
        let (revalidated_group_name, revalidated_mount_epoch) =
            self.freshness_validator
                .validate_mount_epoch(ctx, revalidated_freshness, session.mount_id)?;
        if revalidated_group_name.as_ref() != Some(&worker_lookup_group_name) || revalidated_mount_epoch != mount_epoch
        {
            return self.failure_from_error_with_route_epoch(
                ctx,
                MetadataError::StaleState("SyncWrite mount authority changed during Ready wait".to_string()),
                revalidated_group_name,
                revalidated_mount_epoch,
                route_epoch,
            );
        }
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(ctx, revalidated_freshness, group_name.clone(), mount_epoch, "SyncWrite")
            .await?;
        let session = self
            .revalidate_publish_session(ctx, &publication, publish_mode, "SyncWrite")
            .await?;
        self.require_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)?;

        let routed = match self.route_ctx_for_write_with_error_hints(
            ctx,
            &[session.inode_id],
            revalidated_freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };
        self.require_publish_deadline(
            ctx,
            Some(&worker_lookup_group_name),
            Some(routed.mount_epoch),
            route_epoch,
        )?;

        let command = Command::PublishFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id: session.inode_id,
            publication: FilePublication {
                blocks,
                target_size: intent.final_size,
                expected_generation,
                expected_file_size: intent.expected_file_size,
                lease_epoch,
                mode: publish_mode,
            },
        };
        let generation = match self.propose_file_publication(command, publication).await {
            Ok(generation) => generation,
            Err(error) => {
                return self.failure_from_error(ctx, error, Some(routed.group_name.clone()), Some(routed.mount_epoch))
            }
        };

        self.success_with_route_epoch(
            SyncWriteOutput {
                synced_size: intent.final_size,
                generation: Some(generation),
            },
            Some(routed.group_name.clone()),
            Some(routed.mount_epoch),
            route_epoch,
        )
    }

    fn invalid_publication_failure(
        &self,
        ctx: &RequestContext,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsFailure {
        fs_failure_from_metadata_error(
            ctx,
            MetadataError::InvalidArgument(message.into()),
            group_name,
            mount_epoch,
            None,
        )
    }

    /// Validate the ordered changed tail and new blocks against this session's
    /// issued targets. Raft repeats complete-layout and prefix validation at apply.
    fn validate_committed_blocks(
        intent: &CloseWriteIntent,
        session: &WriteSession,
    ) -> MetadataResult<Vec<CommittedBlock>> {
        if intent.expected_file_size != session.base_size {
            return Err(MetadataError::InvalidArgument(
                "expected file size does not match session".into(),
            ));
        }
        let count = crate::inode::FileData::block_count(intent.final_size, session.layout.block_size)?;
        let capacity = u64::from(session.layout.block_size);
        let start = if session.mode == WriteMode::Overwrite {
            0
        } else if intent.final_size == session.base_size && intent.committed_blocks.is_empty() {
            count
        } else {
            usize::try_from(session.base_size / capacity)
                .map_err(|_| MetadataError::InvalidArgument("block ordinal overflows".into()))?
        };
        if (session.mode == WriteMode::Append && intent.final_size < session.base_size)
            || start > count
            || intent.committed_blocks.len() != count - start
        {
            return Err(MetadataError::InvalidArgument(
                "publication does not cover the target length".into(),
            ));
        }
        let issued: HashMap<_, _> = session
            .issued_targets
            .iter()
            .map(|target| (target.block_id, target))
            .collect();
        let mut seen = HashSet::with_capacity(intent.committed_blocks.len());
        for (index, block) in intent.committed_blocks.iter().enumerate() {
            let target = issued
                .get(&block.block_id)
                .ok_or_else(|| MetadataError::InvalidArgument("block was not issued to this writer".into()))?;
            let offset = (start + index) as u64 * capacity;
            let len = (intent.final_size - offset).min(capacity);
            if block.block_id.inode_id != session.inode_id
                || !seen.insert(block.block_id)
                || target.file_offset != offset
                || block.len != len
            {
                return Err(MetadataError::InvalidArgument(
                    "invalid ordered publication block".into(),
                ));
            }
        }
        Ok(intent.committed_blocks.clone())
    }

    pub(super) async fn close_write_session(
        &self,
        ctx: &RequestContext,
        handle: WriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_generation: ContentGeneration,
        publish_mode: PublishMode,
    ) -> FsResult<CloseWriteOutput> {
        let inode_id = handle.inode_id;
        let lease_epoch = handle.lease_epoch;
        let mut payload = intent.publication(handle, expected_generation, publish_mode);
        // Read the receipt and its layout together before checking any soft
        // session state: a completed commit has already ended that session.
        let resolved = match self.raft_node.as_ref() {
            Some(raft) => {
                raft.read(true, |_| {
                    let inode = self
                        .read_inode(inode_id)?
                        .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
                    if inode.inode_id != inode_id {
                        return Err(MetadataError::Internal("CommitFile inode authority is corrupt".into()));
                    }
                    payload
                        .resolve_commit(&inode, ctx.caller.client.client_id, ctx.caller.client.call_id)
                        .map(|generation| generation.map(|_| inode.mount_id))
                })
                .await
            }
            None => Err(MetadataError::Internal("Raft node not available".into())),
        };
        match resolved {
            Ok(Some(mount_id)) => {
                let (group_name, mount_epoch, route_epoch) = self
                    .completed_publish_hints(ctx, freshness, mount_id, "CommitFile")
                    .await?;
                self.session_registry.remove_session_if_epoch(inode_id, lease_epoch);
                return self.success_with_route_epoch(
                    CloseWriteOutput {
                        committed_size: intent.final_size,
                    },
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
            Ok(None) => {}
            Err(error) => return self.failure_from_error(ctx, error, None, None),
        }
        let active_session = self.active_publish_session(ctx, inode_id, lease_epoch, "CommitFile")?;
        let publication = match active_session {
            Some(_) => match self.begin_write_publication(ctx, inode_id, lease_epoch, publish_mode, "CommitFile") {
                Ok(publication) => publication,
                Err(failure) => return Err(failure),
            },
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={}", inode_id),
                    None,
                    None,
                );
            }
        };
        let session = publication.session().clone();
        if session.generation != expected_generation {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                "CommitFile publish precondition does not match the active session".to_string(),
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
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "CommitFile")
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
                    "write handle epoch mismatch for inode_id={}: expected {}, got {}",
                    inode_id, session.lease_epoch, lease_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .session_registry
            .validate_session(session.inode_id, lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={}", inode_id),
                group_name,
                mount_epoch,
            );
        }

        let blocks = match Self::validate_committed_blocks(&intent, &session) {
            Ok(blocks) => blocks,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name.clone(), mount_epoch))
            }
        };
        let worker_lookup_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "CommitFile")?;
        let new_targets = match self.publication_ready_targets(&session, &intent.committed_blocks, expected_generation)
        {
            Ok(targets) => targets,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name.clone(), mount_epoch));
            }
        };
        self.wait_for_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)
            .await?;

        let revalidated_freshness = Freshness {
            mount_epoch,
            route_epoch,
        };
        let (revalidated_group_name, revalidated_mount_epoch) =
            self.freshness_validator
                .validate_mount_epoch(ctx, revalidated_freshness, session.mount_id)?;
        if revalidated_group_name.as_ref() != Some(&worker_lookup_group_name) || revalidated_mount_epoch != mount_epoch
        {
            return self.failure_from_error_with_route_epoch(
                ctx,
                MetadataError::StaleState("CommitFile mount authority changed during Ready wait".to_string()),
                revalidated_group_name,
                revalidated_mount_epoch,
                route_epoch,
            );
        }
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(
                ctx,
                revalidated_freshness,
                group_name.clone(),
                mount_epoch,
                "CommitFile",
            )
            .await?;
        let session = self
            .revalidate_publish_session(ctx, &publication, publish_mode, "CommitFile")
            .await?;
        self.require_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)?;

        let routed = match self.route_ctx_for_write_with_error_hints(
            ctx,
            &[session.inode_id],
            revalidated_freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };
        self.require_publish_deadline(
            ctx,
            Some(&worker_lookup_group_name),
            Some(routed.mount_epoch),
            route_epoch,
        )?;

        payload.blocks = blocks;
        let command = Command::CommitFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id,
            client_id: ctx.caller.client.client_id,
            call_id: ctx.caller.client.call_id,
            publication: payload,
        };
        if let Err(error) = self.propose_file_publication(command, publication).await {
            return self.failure_from_error(ctx, error, Some(routed.group_name.clone()), Some(routed.mount_epoch));
        }

        self.success_with_route_epoch(
            CloseWriteOutput {
                committed_size: intent.final_size,
            },
            Some(routed.group_name.clone()),
            Some(routed.mount_epoch),
            route_epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeKind;
    use crate::service::filesystem::tests::*;
    use beryl_common::error::rpc::MetadataErrorKind;
    use beryl_common::Deadline;

    async fn open_write_with_target(env: &WriteFlowEnv) -> (OpenWriteOutput, LocatedBlock) {
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
            .expect("open write")
            .payload;
        let target = allocate_block_for_key(&env.filesystem, &open).await;
        (open, target)
    }

    fn target_intent(target: &LocatedBlock, expected_file_size: u64) -> CloseWriteIntent {
        CloseWriteIntent {
            committed_blocks: vec![committed_block(target.block_id, 64)],
            final_size: target.file_offset + 64,
            expected_file_size,
        }
    }

    #[tokio::test]
    async fn commit_waits_for_ready_observation_then_publishes() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let committed = vec![committed_block(target.block_id, 64)];
        let commit = commit_for_key(&env.filesystem, &open, committed, 64);
        tokio::pin!(commit);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "publication must remain pending before the Ready report"
        );
        assert_eq!(
            stored_generation(&env.storage, env.inode_id),
            ContentGeneration::default()
        );

        publish_env_write_target(&env, &target, 1);
        tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("Ready observation should wake publication")
            .expect("commit should succeed");
        assert_eq!(stored_generation(&env.storage, env.inode_id), ContentGeneration::new(1));
    }

    #[tokio::test]
    async fn deadline_expiring_after_ready_wait_does_not_publish() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_now(Duration::from_millis(40));
        let commit = env.filesystem.close_write_session(
            &ctx,
            WriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.generation,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut commit)
                .await
                .is_err(),
            "publication must first wait for Ready"
        );
        publish_env_write_target(&env, &target, 1);
        std::thread::sleep(Duration::from_millis(50));

        commit
            .await
            .expect_err("deadline expiring after the wait must still prevent publication");
        assert_eq!(
            stored_generation(&env.storage, env.inode_id),
            ContentGeneration::default()
        );
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn noop_close_checks_authority_without_advancing_exhausted_generation() {
        for case in 0..3 {
            let env = write_flow_env(0).await;
            if case == 2 {
                let mut inode = env.storage.get_inode(env.inode_id).unwrap().unwrap();
                let InodeKind::File(crate::inode::FileData { generation, .. }) = &mut inode.kind else {
                    unreachable!()
                };
                *generation = ContentGeneration::new(u64::MAX);
                env.storage.put_inode(&inode).unwrap();
            }
            let open = env
                .filesystem
                .open_write_inode(
                    &request_context(),
                    "/file".into(),
                    env.inode_id,
                    vec![env.inode_id],
                    WriteMode::Overwrite,
                    Freshness::default(),
                )
                .await
                .unwrap()
                .payload;
            let mut ctx = request_context();
            if case == 0 {
                ctx.caller.deadline = Deadline::from_unix_ms(0);
            } else if case == 1 {
                env.filesystem
                    .session_registry()
                    .remove_session_if_epoch(open.inode_id, open.lease_epoch)
                    .unwrap();
            }
            let result = env
                .filesystem
                .close_write_session(
                    &ctx,
                    WriteHandle {
                        inode_id: open.inode_id,
                        lease_epoch: open.lease_epoch,
                    },
                    CloseWriteIntent {
                        committed_blocks: Vec::new(),
                        final_size: 0,
                        expected_file_size: 0,
                    },
                    Freshness::default(),
                    open.generation,
                    PublishMode::ReplaceIfUnchanged,
                )
                .await;
            assert_eq!(result.is_ok(), case == 2);
            let inode = env.storage.get_inode(open.inode_id).unwrap().unwrap();
            assert_eq!(stored_generation(&env.storage, env.inode_id), open.generation);
            assert!(
                matches!(inode.kind, InodeKind::File(crate::inode::FileData { lease_epoch: epoch, last_commit, .. })
                if epoch.as_raw() == open.lease_epoch.as_raw() + u64::from(case == 2)
                    && last_commit.is_some() == (case == 2))
            );
        }
    }

    #[tokio::test]
    async fn submitted_publication_survives_cancelled_waiter() {
        use std::future::Future;
        use std::task::Poll;
        for closes in [false, true] {
            let env = write_flow_env(0).await;
            let (open, target) = open_write_with_target(&env).await;
            publish_env_write_target(&env, &target, 1);
            let registry = env.filesystem.session_registry();
            let publication = registry.begin_publication(open.inode_id, open.lease_epoch).unwrap();
            let payload = target_intent(&target, 0).publication(
                WriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                open.generation,
                PublishMode::ReplaceIfUnchanged,
            );
            let command = if closes {
                Command::CommitFile {
                    proposed_at_ms: 1,
                    inode_id: open.inode_id,
                    client_id: publication.session().open_client_id,
                    call_id: beryl_types::CallId::new(),
                    publication: payload,
                }
            } else {
                Command::PublishFile {
                    proposed_at_ms: 1,
                    inode_id: open.inode_id,
                    publication: crate::inode::FilePublication {
                        blocks: payload.blocks,
                        target_size: payload.target_size,
                        expected_generation: payload.expected_generation,
                        expected_file_size: payload.expected_file_size,
                        lease_epoch: payload.lease_epoch,
                        mode: payload.mode,
                    },
                }
            };
            {
                let mut waiter = Box::pin(env.filesystem.propose_file_publication(command, publication));
                // The current-thread executor cannot run the completion task until
                // this yields, so the waiter is cancelled strictly before apply.
                std::future::poll_fn(|cx| {
                    assert!(waiter.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            assert!(registry.get_session_identity(open.inode_id).is_some());
            assert_eq!(
                stored_generation(&env.storage, open.inode_id),
                ContentGeneration::default()
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    match registry.get_session(open.inode_id) {
                        None if closes => break,
                        Some(session) if !closes && session.generation == ContentGeneration::new(1) => break,
                        _ => tokio::task::yield_now().await,
                    }
                }
            })
            .await
            .unwrap();
            let inode = env.storage.get_inode(open.inode_id).unwrap().unwrap();
            assert_eq!(inode.len(), 64);
            assert!(matches!(inode.kind,
                InodeKind::File(crate::inode::FileData { last_commit, lease_epoch: epoch, .. })
                if last_commit.is_some() == closes
                    && epoch.as_raw() == open.lease_epoch.as_raw() + u64::from(closes)));
        }
    }

    #[tokio::test]
    async fn authority_changes_during_ready_wait_prevent_commit() {
        for change in 0..3 {
            let env = write_flow_env(0).await;
            let (open, target) = open_write_with_target(&env).await;
            let ctx = request_context();
            let commit = env.filesystem.close_write_session(
                &ctx,
                WriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                target_intent(&target, open.base_size),
                Freshness::default(),
                open.generation,
                PublishMode::ReplaceIfUnchanged,
            );
            tokio::pin!(commit);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut commit)
                    .await
                    .is_err(),
                "Ready has not been reported"
            );
            let expected = match change {
                0 => {
                    env.filesystem
                        .session_registry()
                        .remove_session_if_epoch(open.inode_id, open.lease_epoch)
                        .unwrap();
                    MetadataErrorKind::SessionInvalid
                }
                1 => {
                    let session = env.filesystem.write_session_for_inode(open.inode_id).unwrap();
                    let table = env.filesystem.mount_table();
                    let mut mount = table.get_mount(session.mount_id).unwrap().unwrap();
                    mount.mount_epoch += 1;
                    table.upsert(mount).unwrap();
                    MetadataErrorKind::MountEpochMismatch
                }
                _ => {
                    env.filesystem.raft_node().shutdown().await.unwrap();
                    MetadataErrorKind::NotLeader
                }
            };
            publish_env_write_target(&env, &target, 1);
            let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
                .await
                .unwrap()
                .expect_err("changed authority must fail closed");
            assert_eq!(failure.error.kind, ErrorKind::Metadata(expected));
            assert_eq!(
                stored_generation(&env.storage, env.inode_id),
                ContentGeneration::default()
            );
        }
    }
}
