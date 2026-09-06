// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata operation execution, retry, and authority-state ownership.

use crate::api::path::NamespacePathBuf;
use crate::api::{DeleteOptions, FileStatus};
use crate::config::ClientConfig;
use crate::error::{
    invalid_response, side_effect_response_body_mismatch, ClientError, ClientErrorKind, ClientResult, RefreshHint,
};
use crate::metadata::{
    AllocateBlockResult, ListStatusPage, MetadataTransport, OpenedFile, ReadLayout, ValidatedMetadataResponse,
};
use crate::metrics;
use crate::metrics::{ClientMetric, ClientMetricLabels};
use crate::runtime::context::{AttemptContext, ClientIdentity, Operation, OperationContext, OperationDeadline};
use crate::runtime::refresh::MetadataTargets;
use crate::runtime::{retry_decision, transport_outcome_is_ambiguous, RetryDecision};
use crate::session::write_session::{CommitFilePlan, SyncWritePlan, WriteSession};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_proto::common::ByteRangeProto;
use beryl_proto::metadata::get_block_locations_request_proto::Target;
use beryl_proto::metadata::{
    AbortFileWriteRequestProto, AbortFileWriteResponseProto, AllocateBlockRequestProto, CommitFileRequestProto,
    CommitFileResponseProto, CreateDirectoryRequestProto, CreateDirectoryResponseProto, CreateFileRequestProto,
    DeleteOptionsProto, DeleteRequestProto, FileTypeProto, GetBlockLocationsRequestProto, GetStatusRequestProto,
    GetStatusResponseProto, ListStatusRequestProto, ListStatusResponseProto, MsyncRequestProto, OpenFileRequestProto,
    OpenWriteModeProto, OpenWriteRequestProto, OpenWriteResponseProto, RenameRequestProto, RenewLeaseRequestProto,
    SyncWriteRequestProto,
};
use beryl_types::{
    BlockId, ClientId, ContentGeneration, FileLayout, FileType, GroupName, InodeId, WriteHandle, WriteMode,
};
use std::fmt::{Debug, Formatter, Result};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tonic::Status;

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 2_000;
const MAX_SERVER_RETRY_AFTER_MS: u64 = 5_000;

/// Owns Metadata operation identity, retry policy, authority state, and the
/// transport used for each selected-endpoint attempt.
#[derive(Clone)]
pub(crate) struct MetadataClient {
    /// Stable process-local identity reused when creating logical operations.
    identity: ClientIdentity,
    /// Sole Metadata network and wire-validation seam.
    transport: Arc<dyn MetadataTransport>,
    /// Client-side route and monotonic authority state learned from Metadata.
    metadata_targets: MetadataTargets,
    /// Bounded retry and absolute operation-timeout configuration.
    max_attempts: usize,
    operation_timeout_ms: u64,
}

impl MetadataClient {
    /// Creates the Metadata owner from validated client-wide dependencies.
    pub(crate) fn new(
        identity: ClientIdentity,
        transport: Arc<dyn MetadataTransport>,
        metadata_targets: MetadataTargets,
        config: &ClientConfig,
    ) -> ClientResult<Self> {
        Ok(Self {
            identity,
            transport,
            metadata_targets,
            max_attempts: config.max_attempts(),
            operation_timeout_ms: config.operation_timeout_ms(),
        })
    }

    /// Starts one absolute deadline shared by all work in a public operation.
    pub(crate) fn operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(self.operation_timeout_ms)
    }

    fn operation(
        &self,
        operation: Operation,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<OperationContext> {
        OperationContext::new_with_identity(&self.identity, operation, route_path, deadline)
    }

    /// Returns the current status for a normalized namespace path.
    pub(crate) async fn get_status(&self, path: NamespacePathBuf) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let deadline = self.operation_deadline();
        let operation = self.operation(Operation::GetStatus, Some(path.clone()), deadline)?;
        let response = self
            .execute_metadata(
                operation,
                GetStatusRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |transport, ctx, req| async move { transport.get_status(ctx, req).await },
            )
            .await?;
        file_status_from_response(path, response)
    }

    /// Returns one bounded Metadata-owned directory page.
    pub(crate) async fn list_status_page(
        &self,
        path: NamespacePathBuf,
        cursor: Option<Vec<u8>>,
        page_size: Option<u32>,
    ) -> ClientResult<ListStatusPage> {
        let path = path.into_string();
        let operation = self.operation(Operation::ListStatus, Some(path.clone()), self.operation_deadline())?;
        let response = self
            .execute_metadata(
                operation,
                ListStatusRequestProto {
                    header: None,
                    path: path.clone(),
                    cursor: cursor.unwrap_or_default(),
                    limit: page_size.unwrap_or(0),
                },
                |transport, ctx, req| async move { transport.list_status(ctx, req).await },
            )
            .await?;
        list_status_page_from_response(path, response)
    }

    /// Creates one directory or ensures the recursive directory chain according
    /// to the existing retry contract.
    pub(crate) async fn mkdirs(&self, path: NamespacePathBuf, create_parent: bool) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let kind = if create_parent {
            Operation::CreateDirectoryRecursive
        } else {
            Operation::CreateDirectory
        };
        let operation = self.operation(kind, Some(path.clone()), self.operation_deadline())?;
        let request = CreateDirectoryRequestProto {
            header: None,
            path: path.clone(),
            recursive: create_parent,
        };
        let response = self
            .execute_mutation_metadata(operation.clone(), request, |transport, ctx, req| async move {
                transport.create_directory(ctx, req).await
            })
            .await?;
        directory_status_from_response(path, response).map_err(|error| error.with_operation_context(&operation))
    }

    /// Encodes the explicit delete contract and submits it without transport replay.
    ///
    /// Namespace deletion is side-effecting, so an ambiguous transport outcome
    /// is surfaced to the caller instead of replaying the mutation. Physical
    /// block reclamation remains asynchronous after Metadata commits the
    /// namespace change.
    pub(crate) async fn delete(&self, path: NamespacePathBuf, options: DeleteOptions) -> ClientResult<()> {
        let path = path.into_string();
        let operation = self.operation(Operation::Delete, Some(path.clone()), self.operation_deadline())?;
        self.execute_mutation_metadata(
            operation,
            DeleteRequestProto {
                header: None,
                path,
                options: Some(DeleteOptionsProto {
                    recursive: options.recursive,
                }),
            },
            |transport, ctx, req| async move { transport.delete(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    /// Renames a namespace entry without replaying an ambiguous transport result.
    pub(crate) async fn rename(&self, src: NamespacePathBuf, dst: NamespacePathBuf) -> ClientResult<()> {
        let src = src.into_string();
        let dst = dst.into_string();
        let operation = self.operation(Operation::Rename, Some(src.clone()), self.operation_deadline())?;
        self.execute_mutation_metadata(
            operation,
            RenameRequestProto {
                header: None,
                src_path: src,
                dst_path: dst,
                flags: 0,
            },
            |transport, ctx, req| async move { transport.rename(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    /// Captures inode, content generation, and length for subsequent authorized reads.
    pub(crate) async fn open_file(&self, path: NamespacePathBuf) -> ClientResult<OpenedFile> {
        let path = path.into_string();
        let operation = self.operation(Operation::OpenFile, Some(path.clone()), self.operation_deadline())?;
        let response = self
            .execute_metadata(
                operation,
                OpenFileRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |transport, ctx, req| async move { transport.open_file(ctx, req).await },
            )
            .await?;
        if response.inode_id == 0 {
            return Err(invalid_response(
                "OpenFile",
                "OpenFileResponseProto.inode_id must be non-zero",
            ));
        }
        let generation = response
            .generation
            .ok_or_else(|| invalid_response("OpenFile", "OpenFileResponseProto.generation missing"))?;
        Ok(OpenedFile::new(
            path,
            InodeId::new(response.inode_id),
            ContentGeneration::new(generation),
            response.file_size,
        ))
    }

    /// Reads an authoritative layout using the bounded read step's identity.
    pub(crate) async fn read_layout_for_inode(
        &self,
        operation: OperationContext,
        inode_id: InodeId,
        offset: u64,
        len: u32,
    ) -> ClientResult<ReadLayout> {
        self.execute_metadata(
            operation,
            GetBlockLocationsRequestProto {
                header: None,
                target: Some(Target::InodeId(inode_id.as_raw())),
                range: Some(ByteRangeProto { offset, len }),
            },
            |transport, ctx, req| async move { transport.read_layout(ctx, req).await },
        )
        .await
    }

    /// Atomically creates a file and validates Metadata's initial write session.
    pub(crate) async fn create_file(&self, path: NamespacePathBuf) -> ClientResult<WriteSession> {
        let path = path.into_string();
        let create_operation = self.operation(Operation::CreateFile, Some(path.clone()), self.operation_deadline())?;
        let create = self
            .execute_mutation_metadata(
                create_operation.clone(),
                CreateFileRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |transport, ctx, req| async move { transport.create_file(ctx, req).await },
            )
            .await?;
        let layout = create.layout.ok_or_else(|| {
            side_effect_response_body_mismatch("CreateFile", "CreateFileResponseProto.layout missing")
                .with_operation_context(&create_operation)
        })?;
        let layout = FileLayout::try_from(layout).map_err(|err| {
            side_effect_response_body_mismatch("CreateFile", format!("CreateFileResponseProto.layout invalid: {err}"))
                .with_operation_context(&create_operation)
        })?;
        let write_handle = create.write_handle.ok_or_else(|| {
            side_effect_response_body_mismatch("CreateFile", "CreateFileResponseProto.write_handle missing")
                .with_operation_context(&create_operation)
        })?;
        let write_handle = WriteHandle::try_from(write_handle).map_err(|error| {
            side_effect_response_body_mismatch("CreateFile", error).with_operation_context(&create_operation)
        })?;
        if create.expires_at_ms == 0 {
            return Err(side_effect_response_body_mismatch(
                "CreateFile",
                "CreateFileResponseProto.expires_at_ms must be non-zero",
            )
            .with_operation_context(&create_operation));
        }
        WriteSession::new(
            path,
            layout,
            write_handle,
            0,
            create.expires_at_ms,
            ContentGeneration::new(create.generation),
            WriteMode::Overwrite,
        )
        .map_err(|error| {
            side_effect_response_body_mismatch("CreateFile", error).with_operation_context(&create_operation)
        })
    }

    /// Opens an append session while preserving Metadata's stored layout.
    pub(crate) async fn open_append(&self, path: NamespacePathBuf) -> ClientResult<WriteSession> {
        let path = path.into_string();
        let (operation, open) = self
            .open_write_request(&path, WriteMode::Append, self.operation_deadline())
            .await?;
        write_session_from_open_response(&operation, path, WriteMode::Append, open)
    }

    /// Retains the exact operation identity so all successful-body validation
    /// failures remain attributable to the side-effecting RPC.
    async fn open_write_request(
        &self,
        path: &str,
        mode: WriteMode,
        deadline: OperationDeadline,
    ) -> ClientResult<(OperationContext, OpenWriteResponseProto)> {
        let operation = self.operation(Operation::OpenWrite, Some(path.to_string()), deadline)?;
        let response = self
            .execute_mutation_metadata(
                operation.clone(),
                OpenWriteRequestProto {
                    header: None,
                    path: path.to_string(),
                    mode: OpenWriteModeProto::from(mode) as i32,
                },
                |transport, ctx, req| async move { transport.open_write(ctx, req).await },
            )
            .await?;
        Ok((operation, response))
    }

    /// Allocates the next Metadata-authorized block and retains its operation
    /// identity for cross-plane target validation.
    /// Retries keep the same handle, predecessor, call identity, and deadline;
    /// Metadata decides whether that predecessor still has a replayable result.
    pub(crate) async fn allocate_block(
        &self,
        path: &str,
        write_handle: WriteHandle,
        previous_block_id: Option<BlockId>,
        deadline: OperationDeadline,
    ) -> ClientResult<(OperationContext, AllocateBlockResult)> {
        let operation = self.operation(Operation::AllocateBlock, Some(path.to_string()), deadline)?;
        let result = self
            .execute_mutation_metadata(
                operation.clone(),
                AllocateBlockRequestProto {
                    header: None,
                    write_handle: Some(write_handle.into()),
                    previous_block_id: previous_block_id.map(Into::into),
                },
                |transport, ctx, req| async move { transport.allocate_block(ctx, req).await },
            )
            .await?;
        Ok((operation, result))
    }

    /// Replays only the frozen commit plan and validates its publication size
    /// under the same operation identity.
    pub(crate) async fn commit_file(&self, plan: CommitFilePlan) -> ClientResult<CommitFileResponseProto> {
        let operation = plan.operation.clone();
        let final_size = plan.final_size;
        let req = CommitFileRequestProto {
            header: None,
            write_handle: Some(plan.write_handle.into()),
            committed_blocks: plan.committed_blocks.iter().map(Into::into).collect(),
            final_size: plan.final_size,
            expected_generation: plan.expected_generation.as_raw(),
            write_mode: OpenWriteModeProto::from(plan.write_mode) as i32,
            expected_file_size: plan.expected_file_size,
        };
        let response = self
            .execute_mutation_metadata(operation.clone(), req, |transport, ctx, req| async move {
                transport.commit_file(ctx, req).await
            })
            .await?;
        if response.committed_size != final_size {
            return Err(side_effect_response_body_mismatch(
                "CommitFile",
                format!(
                    "committed_size {} does not equal final_size {final_size}",
                    response.committed_size
                ),
            )
            .with_operation_context(&operation));
        }
        Ok(response)
    }

    /// Aborts one exact write handle under its frozen operation identity.
    pub(crate) async fn abort_file_write(
        &self,
        operation: OperationContext,
        write_handle: WriteHandle,
    ) -> ClientResult<AbortFileWriteResponseProto> {
        self.execute_mutation_metadata(
            operation,
            AbortFileWriteRequestProto {
                header: None,
                write_handle: Some(write_handle.into()),
            },
            |transport, ctx, req| async move { transport.abort_file_write(ctx, req).await },
        )
        .await
    }

    /// Renews one active write lease and returns its validated nonzero expiry.
    pub(crate) async fn renew_lease(
        &self,
        path: &str,
        write_handle: WriteHandle,
        deadline: OperationDeadline,
    ) -> ClientResult<u64> {
        let operation = self.operation(Operation::RenewLease, Some(path.to_string()), deadline)?;
        let response = self
            .execute_mutation_metadata(
                operation.clone(),
                RenewLeaseRequestProto {
                    header: None,
                    write_handle: Some(write_handle.into()),
                },
                |transport, ctx, req| async move { transport.renew_lease(ctx, req).await },
            )
            .await?;
        if response.expires_at_ms == 0 {
            return Err(
                side_effect_response_body_mismatch("RenewLease", "expires_at_ms must be non-zero")
                    .with_operation_context(&operation),
            );
        }
        Ok(response.expires_at_ms)
    }

    /// Replays only the frozen sync plan and validates its publication size
    /// under the same operation identity.
    pub(crate) async fn sync_write(&self, plan: SyncWritePlan) -> ClientResult<ContentGeneration> {
        let operation = plan.operation.clone();
        let target_size = plan.target_size;
        let req = SyncWriteRequestProto {
            header: None,
            write_handle: Some(plan.write_handle.into()),
            committed_blocks: plan.committed_blocks.iter().map(Into::into).collect(),
            target_size: plan.target_size,
            expected_generation: plan.expected_generation.as_raw(),
            write_mode: OpenWriteModeProto::from(plan.write_mode) as i32,
            expected_file_size: plan.expected_file_size,
        };
        let response = self
            .execute_mutation_metadata(operation.clone(), req, |transport, ctx, req| async move {
                transport.sync_write(ctx, req).await
            })
            .await?;
        if response.synced_size != target_size {
            return Err(side_effect_response_body_mismatch(
                "SyncWrite",
                format!(
                    "synced_size {} does not equal target_size {target_size}",
                    response.synced_size
                ),
            )
            .with_operation_context(&operation));
        }
        response.generation.map(ContentGeneration::new).ok_or_else(|| {
            side_effect_response_body_mismatch("SyncWrite", "generation missing").with_operation_context(&operation)
        })
    }

    /// Returns the stable client identity used by Worker operation contexts.
    pub(crate) fn client_id(&self) -> ClientId {
        self.identity.client_id()
    }

    /// Returns the configured client name carried by operation headers.
    pub(crate) fn client_name(&self) -> &str {
        self.identity.client_name()
    }

    /// Applies a Worker-requested Metadata refresh to the same logical read.
    pub(crate) fn record_data_refresh(
        &self,
        operation: &OperationContext,
        kind: ErrorKind,
        hint: &RefreshHint,
    ) -> ClientResult<()> {
        self.metadata_targets.record_refresh(operation, kind, hint)
    }

    /// Executes a mutation under its typed replay policy and keeps ambiguity
    /// sticky until a validated success proves the final outcome.
    async fn execute_mutation_metadata<Req, T, F, Fut>(
        &self,
        operation: OperationContext,
        request: Req,
        call: F,
    ) -> ClientResult<T>
    where
        Req: Clone,
        F: FnMut(Arc<dyn MetadataTransport>, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<ValidatedMetadataResponse<T>>>,
    {
        let operation_name = operation.operation_name();
        let operation_context = operation.clone();
        let (result, saw_transport_ambiguity) = self.execute_metadata_attempts(operation, request, call).await;
        match result {
            Err(err) if saw_transport_ambiguity || err.is_outcome_unknown() || err.is_invalid_success_response() => {
                let unknown = if err.is_outcome_unknown() {
                    err.with_operation_context(&operation_context)
                } else if saw_transport_ambiguity {
                    let message = format!("{operation_name} outcome is unknown after transport ambiguity: {err}");
                    err.with_unknown_outcome(&operation_context, message)
                } else {
                    let message = format!("{operation_name} outcome is unknown after invalid success response: {err}");
                    err.with_unknown_outcome(&operation_context, message)
                };
                self.record_metric(
                    ClientMetric::UnknownOutcome,
                    ClientMetricLabels::default()
                        .with_operation(operation_name, "metadata")
                        .with_error_class("unknown_outcome")
                        .with_outcome("unknown"),
                );
                Err(unknown)
            }
            Err(err) => Err(err.with_operation_context(&operation_context)),
            Ok(value) => Ok(value),
        }
    }

    /// Executes a read-only Metadata operation and attaches its stable identity
    /// to any terminal failure.
    async fn execute_metadata<Req, T, F, Fut>(
        &self,
        operation: OperationContext,
        request: Req,
        call: F,
    ) -> ClientResult<T>
    where
        Req: Clone,
        F: FnMut(Arc<dyn MetadataTransport>, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<ValidatedMetadataResponse<T>>>,
    {
        let operation_context = operation.clone();
        self.execute_metadata_attempts(operation, request, call)
            .await
            .0
            .map_err(|error| error.with_operation_context(&operation_context))
    }

    /// Runs bounded metadata attempts and applies every validated successful
    /// authority update before returning the corresponding body.
    async fn execute_metadata_attempts<Req, T, F, Fut>(
        &self,
        operation: OperationContext,
        request: Req,
        mut call: F,
    ) -> (ClientResult<T>, bool)
    where
        Req: Clone,
        F: FnMut(Arc<dyn MetadataTransport>, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<ValidatedMetadataResponse<T>>>,
    {
        let mut target_group = match self.metadata_targets.group_for_operation(&operation) {
            Ok(group) => group,
            Err(err) => return (Err(err), false),
        };
        let mut saw_transport_ambiguity = false;
        for attempt_index in 0..self.max_attempts {
            let attempt = attempt_index as u32;
            let endpoint = match self.metadata_targets.endpoint_for_group(&target_group, attempt) {
                Ok(endpoint) => endpoint,
                Err(err) => return (Err(err), saw_transport_ambiguity),
            };
            let mut ctx = match AttemptContext::for_metadata(&operation, target_group.clone(), attempt) {
                Ok(ctx) => ctx.with_metadata_endpoint(&endpoint),
                Err(err) => return (Err(err), saw_transport_ambiguity),
            };
            ctx = self.metadata_targets.enrich_attempt_context(&operation, ctx);
            if let Some(watermark) = self.metadata_targets.state_watermark_proto(&target_group) {
                ctx = ctx.with_state(vec![watermark]);
            }

            let result = self
                .metadata_rpc_with_deadline(&operation, call(Arc::clone(&self.transport), ctx, request.clone()))
                .await;
            let err = match result {
                Ok(response) => {
                    let (authority, body) = response.into_parts();
                    if let Err(err) = self.metadata_targets.apply_authority_update(&operation, authority) {
                        return (
                            Err(ClientError::invalid_response(
                                operation.operation_name(),
                                format!("invalid Metadata authority update: {err}"),
                            )),
                            saw_transport_ambiguity,
                        );
                    }
                    return (Ok(body), saw_transport_ambiguity);
                }
                Err(err) => err,
            };
            let decision = retry_decision(&err, operation.retry_safety());
            saw_transport_ambiguity |= transport_outcome_is_ambiguous(&err, operation.retry_safety());
            self.record_error_metric(&operation, &err);
            let has_next = attempt_index + 1 < self.max_attempts;

            match (decision, has_next) {
                (RetryDecision::Retry, true) => {
                    if err.is_retryable_transport() && !err.is_definitely_before_side_effect() {
                        self.metadata_targets.record_transport_failure(&target_group, &endpoint);
                    }
                    self.record_retry(&operation, &err);
                    let delay = server_retry_delay(&err).unwrap_or_else(|| backoff_delay(attempt_index));
                    if let Err(err) = self.sleep_with_deadline(&operation, delay).await {
                        return (Err(err), saw_transport_ambiguity);
                    }
                }
                (RetryDecision::RefreshMetadata(kind), true) => {
                    let hint = refresh_hint_from_error(&err);
                    if let Err(err) = self.metadata_targets.record_refresh(&operation, kind, &hint) {
                        return (Err(err), saw_transport_ambiguity);
                    }
                    if kind == ErrorKind::Metadata(MetadataErrorKind::StaleState) {
                        if let Err(err) = self
                            .refresh_state(&operation, target_group.clone(), attempt.saturating_add(1))
                            .await
                        {
                            return (Err(err), saw_transport_ambiguity);
                        }
                    }
                    target_group = match self.metadata_targets.group_for_operation(&operation) {
                        Ok(group) => group,
                        Err(err) => return (Err(err), saw_transport_ambiguity),
                    };
                    self.record_retry(&operation, &err);
                }
                (RetryDecision::Retry | RetryDecision::RefreshMetadata(_), false) => {
                    self.record_metric(
                        ClientMetric::RetryExhausted,
                        metadata_labels(&operation).with_error_class(err.classification_label()),
                    );
                    return (Err(err), saw_transport_ambiguity);
                }
                (RetryDecision::Return, _) if err.is_outcome_unknown() => {
                    self.record_metric(
                        ClientMetric::UnknownOutcome,
                        metadata_labels(&operation)
                            .with_error_class("unknown_outcome")
                            .with_outcome("unknown"),
                    );
                    return (Err(err), saw_transport_ambiguity);
                }
                (RetryDecision::Return, _) => return (Err(err), saw_transport_ambiguity),
            }
        }
        (
            Err(ClientError::metadata(format!(
                "{} exhausted attempts",
                operation.operation_name()
            ))),
            saw_transport_ambiguity,
        )
    }

    async fn refresh_state(
        &self,
        parent: &OperationContext,
        target_group: GroupName,
        attempt: u32,
    ) -> ClientResult<()> {
        let endpoint = self.metadata_targets.endpoint_for_group(&target_group, attempt)?;
        let operation = self.operation(
            Operation::Msync,
            parent.original_target_path().map(ToOwned::to_owned),
            parent.deadline().clone(),
        )?;
        let ctx = AttemptContext::for_metadata(&operation, target_group, 0)?.with_metadata_endpoint(endpoint);
        let response = self
            .metadata_rpc_with_deadline(
                &operation,
                self.transport.msync(ctx, MsyncRequestProto { header: None }),
            )
            .await?;
        let (authority, _) = response.into_parts();
        self.metadata_targets.apply_authority_update(&operation, authority)
    }

    async fn metadata_rpc_with_deadline<T, Fut>(&self, operation: &OperationContext, future: Fut) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() {
            self.record_timeout(operation);
            return Err(timeout_error("metadata", operation.operation_name()));
        }
        match tokio::time::timeout(remaining, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_timeout(operation);
                Err(timeout_error("metadata", operation.operation_name()))
            }
        }
    }

    async fn sleep_with_deadline(&self, operation: &OperationContext, delay: Duration) -> ClientResult<()> {
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() || delay >= remaining {
            self.record_timeout(operation);
            return Err(timeout_error("metadata", operation.operation_name()));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }

    fn record_retry(&self, operation: &OperationContext, error: &ClientError) {
        self.record_metric(
            ClientMetric::RetryAttempt,
            metadata_labels(operation).with_error_class(error.classification_label()),
        );
    }

    fn record_timeout(&self, operation: &OperationContext) {
        self.record_metric(
            ClientMetric::RpcTimeout,
            metadata_labels(operation)
                .with_error_class("retryable_transport")
                .with_outcome("timeout"),
        );
    }

    fn record_error_metric(&self, operation: &OperationContext, error: &ClientError) {
        let metric = if error.is_outcome_unknown() {
            Some(ClientMetric::UnknownOutcome)
        } else {
            match error.kind() {
                ClientErrorKind::InvalidResponse => Some(ClientMetric::InvalidHeader),
                ClientErrorKind::Fenced => Some(ClientMetric::FencingMismatch),
                ClientErrorKind::SessionInvalid => Some(ClientMetric::SessionInvalid),
                ClientErrorKind::SessionExpired => Some(ClientMetric::SessionExpired),
                ClientErrorKind::Unsupported => Some(ClientMetric::UnsupportedOperation),
                _ => None,
            }
        };
        if let Some(metric) = metric {
            self.record_metric(
                metric,
                metadata_labels(operation).with_error_class(error.classification_label()),
            );
        }
    }

    fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        metrics::record(metric, labels);
    }
}

impl Debug for MetadataClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("MetadataClient")
            .field("client_id", &self.identity.client_id())
            .field("client_name", &self.identity.client_name())
            .field("metadata_targets", &self.metadata_targets)
            .field("max_attempts", &self.max_attempts)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish_non_exhaustive()
    }
}

fn metadata_labels(operation: &OperationContext) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(operation.operation_name(), "metadata")
}

fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    err.refresh_hint().cloned().unwrap_or_default()
}

fn server_retry_delay(err: &ClientError) -> Option<Duration> {
    err.retry_after()
        .map(|delay| delay.min(Duration::from_millis(MAX_SERVER_RETRY_AFTER_MS)))
}

fn backoff_delay(retry_index: usize) -> Duration {
    let shift = retry_index.min(20) as u32;
    Duration::from_millis(INITIAL_BACKOFF_MS.saturating_mul(1u64 << shift).min(MAX_BACKOFF_MS))
}

fn timeout_error(target_plane: &str, operation: &str) -> ClientError {
    ClientError::from(Status::deadline_exceeded(format!(
        "{target_plane} {operation} exceeded the public operation deadline"
    )))
}

/// Converts a validated open-write response into the sole client-side session
/// state consumed by `FileWriter`.
fn write_session_from_open_response(
    operation: &OperationContext,
    path: String,
    mode: WriteMode,
    response: OpenWriteResponseProto,
) -> ClientResult<WriteSession> {
    let layout = response.layout.ok_or_else(|| {
        side_effect_response_body_mismatch("OpenWrite", "OpenWriteResponseProto.layout missing")
            .with_operation_context(operation)
    })?;
    let layout = FileLayout::try_from(layout).map_err(|err| {
        side_effect_response_body_mismatch("OpenWrite", format!("OpenWriteResponseProto.layout invalid: {err}"))
            .with_operation_context(operation)
    })?;
    let write_handle = response.write_handle.ok_or_else(|| {
        side_effect_response_body_mismatch("OpenWrite", "OpenWriteResponseProto.write_handle missing")
            .with_operation_context(operation)
    })?;
    let write_handle = WriteHandle::try_from(write_handle)
        .map_err(|error| side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation))?;
    if response.expires_at_ms == 0 {
        return Err(
            side_effect_response_body_mismatch("OpenWrite", "expires_at_ms must be non-zero")
                .with_operation_context(operation),
        );
    }
    let mut session = WriteSession::new(
        path,
        layout,
        write_handle,
        response.base_size,
        response.expires_at_ms,
        ContentGeneration::new(response.generation),
        mode,
    )
    .map_err(|error| side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation))?;
    let group = response
        .header
        .as_ref()
        .ok_or_else(|| ClientError::invalid_layout("OpenWrite header missing"))?
        .group_name
        .as_str();
    let group = beryl_types::GroupName::parse(group)
        .map_err(|error| side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation))?;
    let tail = response
        .tail_block
        .map(TryInto::try_into)
        .transpose()
        .map_err(|error: String| {
            side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation)
        })?;
    session
        .accept_open_tail(group, tail)
        .map_err(|error| side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation))?;
    Ok(session)
}

fn file_status_from_response(path: String, response: GetStatusResponseProto) -> ClientResult<FileStatus> {
    let kind = file_type_from_wire("GetStatus", response.kind)?;
    Ok(FileStatus::new(
        path,
        kind,
        response.len,
        response.create_time,
        response.modify_time,
    ))
}

fn directory_status_from_response(path: String, response: CreateDirectoryResponseProto) -> ClientResult<FileStatus> {
    Ok(FileStatus::new(
        path,
        FileType::Dir,
        0,
        response.create_time,
        response.modify_time,
    ))
}

/// Converts a successful wire page while enforcing its cursor/EOF invariant.
fn list_status_page_from_response(path: String, response: ListStatusResponseProto) -> ClientResult<ListStatusPage> {
    if response.eof != response.next_cursor.is_empty() {
        return Err(invalid_response(
            "ListStatus",
            "eof must be true exactly when next_cursor is empty",
        ));
    }
    if !response.eof && response.entries.is_empty() {
        return Err(invalid_response("ListStatus", "non-EOF page must contain entries"));
    }
    let next_cursor = if response.next_cursor.is_empty() {
        None
    } else {
        Some(response.next_cursor)
    };
    let entries = response
        .entries
        .into_iter()
        .map(|entry| {
            if entry.name.is_empty() || entry.name.contains('/') {
                return Err(invalid_response(
                    "ListStatus",
                    format!("invalid direct-child name: {:?}", entry.name),
                ));
            }
            let kind = file_type_from_wire("ListStatus", entry.kind)?;
            let parent = path.trim_end_matches('/');
            let child_path = if parent.is_empty() {
                format!("/{}", entry.name)
            } else {
                format!("{parent}/{}", entry.name)
            };
            Ok(FileStatus::new(
                child_path,
                kind,
                entry.len,
                entry.create_time,
                entry.modify_time,
            ))
        })
        .collect::<ClientResult<Vec<_>>>()?;
    Ok(ListStatusPage {
        entries,
        next_cursor,
        eof: response.eof,
    })
}

/// Rejects unknown and UNSPECIFIED wire values before they enter the public status model.
fn file_type_from_wire(operation: &'static str, raw: i32) -> ClientResult<FileType> {
    let wire =
        FileTypeProto::try_from(raw).map_err(|_| invalid_response(operation, format!("unknown inode kind: {raw}")))?;
    wire.try_into()
        .map_err(|error| invalid_response(operation, format!("invalid inode kind: {error}")))
}
