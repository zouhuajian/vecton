// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Conversion utilities between proto messages and types crate.
//!
//! This module provides bidirectional conversion between proto messages
//! and domain types defined in the types crate.

use crate::common::error_kind_proto::Kind;
use crate::common::recovery_action_proto::Action;
use crate::common::{
    BlockIdProto, ByteRangeProto, CallerContextProto, ClientIdProto, ClientInfoProto, ErrorDetailProto, ErrorKindProto,
    FailRecoveryProto, FencingTokenProto, FileLayoutProto, GroupStateWatermarkProto, InternalErrorKindProto,
    MetadataErrorKindProto, ProtocolErrorKindProto, RaftLogIdProto, RecoveryActionProto, RefreshHintProto,
    RefreshMetadataRecoveryProto, RegisterWorkerRecoveryProto, ReopenWriteSessionRecoveryProto, RequestHeaderProto,
    ResponseHeaderProto, RetryRecoveryProto, SendFullBlockReportRecoveryProto, TierProto, TraceContextProto,
    WorkerEndpointInfoProto, WorkerErrorKindProto,
};
use crate::metadata::{
    CommittedBlockProto, FileBlockLocationProto, FileTypeProto, LocatedBlockProto, OpenWriteModeProto, WriteHandleProto,
};
use ::beryl_common::Deadline;
use ::beryl_common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail,
    WorkerEndpointHint, WorkerErrorKind,
};
use ::beryl_common::header::{CallerContext, ClientInfo, RequestHeader, ResponseHeader, TraceContext};
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, BlockIndex, WorkerId};
use beryl_types::layout::{BlockFormatId, BlockShape, FileLayout};
use beryl_types::lease::{FencingToken, LeaseEpoch, WriteHandle};
use beryl_types::{
    CallId, ClientId, CommittedBlock, FileBlockLocation, FileType, GroupName, GroupStateWatermark, InodeId,
    LocatedBlock, RaftLogId, Tier, WorkerEndpointInfo, WorkerNetProtocol, WorkerRunId, WriteMode,
};

// ============================================================================
// ID Conversions
// ============================================================================

impl From<BlockId> for BlockIdProto {
    fn from(id: BlockId) -> Self {
        BlockIdProto {
            inode_id: id.inode_id.as_raw(),
            block_index: id.index.as_raw(),
        }
    }
}

impl TryFrom<BlockIdProto> for BlockId {
    type Error = String;

    fn try_from(id: BlockIdProto) -> Result<Self, Self::Error> {
        if id.inode_id == 0 {
            return Err("BlockIdProto.inode_id must be non-zero".to_string());
        }
        Ok(BlockId::new(InodeId::new(id.inode_id), BlockIndex::new(id.block_index)))
    }
}

impl From<ClientId> for ClientIdProto {
    fn from(id: ClientId) -> Self {
        let value = id.as_raw();
        ClientIdProto {
            high: (value >> 64) as u64,
            low: value as u64,
        }
    }
}

impl TryFrom<ClientIdProto> for ClientId {
    type Error = String;

    fn try_from(id: ClientIdProto) -> Result<Self, Self::Error> {
        let value = ((id.high as u128) << 64) | (id.low as u128);
        if value == 0 {
            return Err("client_id must be non-zero".to_string());
        }
        Ok(ClientId::new(value))
    }
}

/// Parse a required block id field without choosing caller error policy.
pub fn required_block_id(proto: Option<BlockIdProto>, field_name: &str) -> Result<BlockId, String> {
    proto
        .ok_or_else(|| format!("missing {field_name}"))?
        .try_into()
        .map_err(|error| format!("invalid {field_name}: {error}"))
}

/// Parse a required client id field without choosing caller error policy.
pub fn required_client_id(proto: Option<ClientIdProto>, field_name: &str) -> Result<ClientId, String> {
    proto
        .ok_or_else(|| format!("missing {field_name}"))?
        .try_into()
        .map_err(|err| format!("invalid {field_name}: {err}"))
}

/// Parse a required call UUID field without choosing caller error policy.
pub fn require_call_id(value: &str, field_name: &str) -> Result<CallId, String> {
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    CallId::parse(value).map_err(|err| format!("{field_name} {err}"))
}

impl From<ByteRange> for ByteRangeProto {
    fn from(range: ByteRange) -> Self {
        ByteRangeProto {
            offset: range.offset,
            len: range.len,
        }
    }
}

impl TryFrom<FileLayoutProto> for FileLayout {
    type Error = String;

    fn try_from(layout: FileLayoutProto) -> Result<Self, Self::Error> {
        let block_format_id = BlockFormatId::from_raw(layout.block_format_id)
            .map_err(|err| format!("FileLayoutProto.block_format_id invalid: {err}"))?;
        let layout = FileLayout::with_block_format(layout.block_size, block_format_id);
        layout
            .validate()
            .map_err(|err| format!("FileLayoutProto invalid: {err}"))?;
        Ok(layout)
    }
}

impl From<&FileLayout> for FileLayoutProto {
    fn from(layout: &FileLayout) -> Self {
        Self {
            block_size: layout.block_size,
            block_format_id: layout.block_format_id.as_raw(),
        }
    }
}

impl From<FileLayout> for FileLayoutProto {
    fn from(layout: FileLayout) -> Self {
        Self::from(&layout)
    }
}

// ============================================================================
// FS Domain Conversions
// ============================================================================

impl TryFrom<FileTypeProto> for FileType {
    type Error = String;

    fn try_from(kind: FileTypeProto) -> Result<Self, Self::Error> {
        match kind {
            FileTypeProto::FileTypeFile => Ok(Self::File),
            FileTypeProto::FileTypeDir => Ok(Self::Dir),
            FileTypeProto::FileTypeUnspecified => Err("unspecified inode kind is not a domain value".to_string()),
        }
    }
}

/// Encode a payload-free type label without exposing the persisted inode model.
impl From<FileType> for FileTypeProto {
    fn from(kind: FileType) -> Self {
        match kind {
            FileType::File => Self::FileTypeFile,
            FileType::Dir => Self::FileTypeDir,
        }
    }
}

/// Encodes session intent using the existing wire enum values.
impl From<WriteMode> for OpenWriteModeProto {
    fn from(mode: WriteMode) -> Self {
        match mode {
            WriteMode::Overwrite => Self::OpenWriteModeWrite,
            WriteMode::Append => Self::OpenWriteModeAppend,
        }
    }
}

/// Rejects absent intent instead of choosing a write policy at the wire boundary.
impl TryFrom<OpenWriteModeProto> for WriteMode {
    type Error = String;

    fn try_from(mode: OpenWriteModeProto) -> Result<Self, Self::Error> {
        match mode {
            OpenWriteModeProto::OpenWriteModeWrite => Ok(Self::Overwrite),
            OpenWriteModeProto::OpenWriteModeAppend => Ok(Self::Append),
            OpenWriteModeProto::OpenWriteModeUnspecified => Err("write mode is required".to_string()),
        }
    }
}

/// Decodes required session intent, rejecting both unspecified and unknown modes.
pub fn parse_write_mode(value: i32) -> Result<WriteMode, String> {
    OpenWriteModeProto::try_from(value)
        .map_err(|_| format!("unknown write mode: {value}"))?
        .try_into()
}

/// Encode only the file and lease identity; caller identity stays in the header.
impl From<WriteHandle> for WriteHandleProto {
    fn from(handle: WriteHandle) -> Self {
        Self {
            inode_id: handle.inode_id.as_raw(),
            write_lease_epoch: handle.lease_epoch.as_raw(),
        }
    }
}

/// Check structural identity before a handle enters domain orchestration.
impl TryFrom<WriteHandleProto> for WriteHandle {
    type Error = String;

    fn try_from(handle: WriteHandleProto) -> Result<Self, Self::Error> {
        if handle.inode_id == 0 {
            return Err("write_handle.inode_id must be non-zero".to_string());
        }
        if handle.write_lease_epoch == 0 {
            return Err("write_handle.write_lease_epoch must be non-zero".to_string());
        }
        Ok(Self {
            inode_id: InodeId::new(handle.inode_id),
            lease_epoch: LeaseEpoch::new(handle.write_lease_epoch),
        })
    }
}

impl From<FencingToken> for FencingTokenProto {
    fn from(token: FencingToken) -> Self {
        FencingTokenProto {
            block_id: Some(token.block_id.into()),
            owner: Some(token.owner.into()),
            epoch: token.epoch.as_raw(),
        }
    }
}

impl TryFrom<FencingTokenProto> for FencingToken {
    type Error = String;

    fn try_from(token: FencingTokenProto) -> Result<Self, Self::Error> {
        let block_id = required_block_id(token.block_id, "block_id in token")?;
        let owner = required_client_id(token.owner, "owner in token")?;
        Ok(FencingToken::new(block_id, owner, LeaseEpoch::new(token.epoch)))
    }
}

/// Parse a required fencing token field without choosing caller error policy.
pub fn required_fencing_token(proto: Option<FencingTokenProto>, field_name: &str) -> Result<FencingToken, String> {
    proto.ok_or_else(|| format!("missing {field_name}"))?.try_into()
}

/// Parse a required worker process-run identifier field without choosing caller error policy.
pub fn require_worker_run_id(value: &str, field_name: &str) -> Result<WorkerRunId, String> {
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    WorkerRunId::parse(value).map_err(|err| format!("{field_name} invalid: {err}"))
}

impl From<Tier> for TierProto {
    fn from(tier: Tier) -> Self {
        match tier {
            Tier::Mem => TierProto::TierMem,
            Tier::Nvme => TierProto::TierNvme,
            Tier::Ssd => TierProto::TierSsd,
            Tier::Hdd => TierProto::TierHdd,
        }
    }
}

impl TryFrom<TierProto> for Tier {
    type Error = String;

    fn try_from(tier: TierProto) -> Result<Self, Self::Error> {
        match tier {
            TierProto::TierMem => Ok(Self::Mem),
            TierProto::TierNvme => Ok(Self::Nvme),
            TierProto::TierSsd => Ok(Self::Ssd),
            TierProto::TierHdd => Ok(Self::Hdd),
            TierProto::TierUnspecified => Err("tier must be specified".to_string()),
        }
    }
}

pub fn parse_known_tier(value: i32) -> Result<Tier, String> {
    TierProto::try_from(value)
        .map_err(|_| format!("unknown tier value {value}"))?
        .try_into()
}

impl TryFrom<WorkerEndpointInfoProto> for WorkerEndpointInfo {
    type Error = String;

    fn try_from(endpoint: WorkerEndpointInfoProto) -> Result<Self, Self::Error> {
        worker_endpoint_info_from_parts(
            WorkerId::new(endpoint.worker_id),
            endpoint.endpoint,
            endpoint.worker_run_id,
        )
    }
}

/// Build a shared worker endpoint value from raw wire-shaped fields.
///
pub fn worker_endpoint_info_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_run_id: String,
) -> Result<WorkerEndpointInfo, String> {
    if worker_id.as_raw() == 0 {
        return Err("WorkerEndpointInfoProto.worker_id must be non-zero".to_string());
    }
    if endpoint.is_empty() {
        return Err("WorkerEndpointInfoProto.endpoint must not be empty".to_string());
    }
    let worker_run_id = require_worker_run_id(&worker_run_id, "WorkerEndpointInfoProto.worker_run_id")?;
    Ok(WorkerEndpointInfo {
        worker_id,
        endpoint,
        worker_net_protocol: WorkerNetProtocol::Grpc,
        worker_run_id,
    })
}

impl From<&WorkerEndpointInfo> for WorkerEndpointInfoProto {
    fn from(endpoint: &WorkerEndpointInfo) -> Self {
        Self {
            worker_id: endpoint.worker_id.as_raw(),
            endpoint: endpoint.endpoint.clone(),
            worker_run_id: endpoint.worker_run_id.to_string(),
        }
    }
}

impl From<WorkerEndpointInfo> for WorkerEndpointInfoProto {
    fn from(endpoint: WorkerEndpointInfo) -> Self {
        Self {
            worker_id: endpoint.worker_id.as_raw(),
            endpoint: endpoint.endpoint,
            worker_run_id: endpoint.worker_run_id.to_string(),
        }
    }
}

/// Validate write locations, block shape, and matching fencing identity at the wire boundary.
impl TryFrom<LocatedBlockProto> for LocatedBlock {
    type Error = String;

    fn try_from(target: LocatedBlockProto) -> Result<Self, Self::Error> {
        let block_format_id = BlockFormatId::from_raw(target.block_format_id)
            .map_err(|err| format!("LocatedBlockProto.block_format_id invalid: {err}"))?;
        BlockShape::new(block_format_id, target.block_size, target.chunk_size, target.block_size)
            .map_err(|err| format!("LocatedBlockProto invalid block shape: {err}"))?;
        if target.worker_endpoints.is_empty() {
            return Err("LocatedBlockProto.worker_endpoints must not be empty".to_string());
        }
        if target.write_offset >= target.block_size {
            return Err("LocatedBlockProto.write_offset must be below block capacity".to_string());
        }
        let tier = parse_known_tier(target.tier).map_err(|err| format!("LocatedBlockProto.tier invalid: {err}"))?;
        let block_id = required_block_id(target.block_id, "LocatedBlockProto.block_id")?;
        let fencing_token = required_fencing_token(target.fencing_token, "LocatedBlockProto.fencing_token")?;
        if fencing_token.block_id != block_id {
            return Err("LocatedBlockProto.fencing_token block_id must match block_id".to_string());
        }
        if fencing_token.owner.is_zero() || fencing_token.epoch.as_raw() == 0 {
            return Err("LocatedBlockProto.fencing_token owner and epoch must be non-zero".to_string());
        }
        let worker_endpoints = target
            .worker_endpoints
            .into_iter()
            .map(WorkerEndpointInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            block_id,
            file_offset: target.file_offset,
            write_offset: target.write_offset,
            block_size: target.block_size,
            worker_endpoints,
            fencing_token,

            chunk_size: target.chunk_size,
            block_format_id,
            tier,
        })
    }
}

impl From<&LocatedBlock> for LocatedBlockProto {
    fn from(target: &LocatedBlock) -> Self {
        Self {
            block_id: Some(target.block_id.into()),
            file_offset: target.file_offset,
            write_offset: target.write_offset,
            worker_endpoints: target.worker_endpoints.iter().map(Into::into).collect(),
            fencing_token: Some(target.fencing_token.into()),

            chunk_size: target.chunk_size,
            block_format_id: target.block_format_id.as_raw(),
            block_size: target.block_size,
            tier: TierProto::from(target.tier) as i32,
        }
    }
}

impl From<LocatedBlock> for LocatedBlockProto {
    fn from(target: LocatedBlock) -> Self {
        Self {
            block_id: Some(target.block_id.into()),
            file_offset: target.file_offset,
            write_offset: target.write_offset,
            worker_endpoints: target.worker_endpoints.into_iter().map(Into::into).collect(),
            fencing_token: Some(target.fencing_token.into()),

            chunk_size: target.chunk_size,
            block_format_id: target.block_format_id.as_raw(),
            block_size: target.block_size,
            tier: TierProto::from(target.tier) as i32,
        }
    }
}

impl TryFrom<CommittedBlockProto> for CommittedBlock {
    type Error = String;

    fn try_from(block: CommittedBlockProto) -> Result<Self, Self::Error> {
        let block_id = required_block_id(block.block_id, "CommittedBlockProto.block_id")?;
        Ok(Self {
            block_id,

            len: block.len,
        })
    }
}

impl From<&CommittedBlock> for CommittedBlockProto {
    fn from(block: &CommittedBlock) -> Self {
        Self {
            block_id: Some(block.block_id.into()),

            len: block.len,
        }
    }
}

impl From<CommittedBlock> for CommittedBlockProto {
    fn from(block: CommittedBlock) -> Self {
        Self {
            block_id: Some(block.block_id.into()),

            len: block.len,
        }
    }
}

impl TryFrom<FileBlockLocationProto> for FileBlockLocation {
    type Error = String;

    fn try_from(location: FileBlockLocationProto) -> Result<Self, Self::Error> {
        if location.len == 0 {
            return Err("FileBlockLocationProto.len must be non-zero".to_string());
        }

        let block_format_id = BlockFormatId::from_raw(location.block_format_id)
            .map_err(|err| format!("FileBlockLocationProto.block_format_id invalid: {err}"))?;
        BlockShape::new(
            block_format_id,
            location.block_size,
            location.chunk_size,
            location.effective_len,
        )
        .map_err(|err| format!("FileBlockLocationProto invalid block shape: {err}"))?;
        let block_id = required_block_id(location.block_id, "FileBlockLocationProto.block_id")?;
        let workers = location
            .workers
            .into_iter()
            .map(WorkerEndpointInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            block_id,
            file_offset: location.file_offset,
            len: location.len,
            workers,
            block_format_id,
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        })
    }
}

impl From<&FileBlockLocation> for FileBlockLocationProto {
    fn from(location: &FileBlockLocation) -> Self {
        Self {
            block_id: Some(location.block_id.into()),
            file_offset: location.file_offset,
            len: location.len,
            workers: location.workers.iter().map(Into::into).collect(),

            block_format_id: location.block_format_id.as_raw(),
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        }
    }
}

impl From<FileBlockLocation> for FileBlockLocationProto {
    fn from(location: FileBlockLocation) -> Self {
        Self {
            block_id: Some(location.block_id.into()),
            file_offset: location.file_offset,
            len: location.len,
            workers: location.workers.into_iter().map(Into::into).collect(),

            block_format_id: location.block_format_id.as_raw(),
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        }
    }
}

// ============================================================================
// RaftLogIdProto Conversions
// ============================================================================

impl From<&RaftLogId> for RaftLogIdProto {
    fn from(log_id: &RaftLogId) -> Self {
        RaftLogIdProto {
            term: log_id.term,
            leader_node_id: log_id.leader_node_id,
            index: log_id.index,
        }
    }
}

impl From<RaftLogId> for RaftLogIdProto {
    fn from(log_id: RaftLogId) -> Self {
        RaftLogIdProto {
            term: log_id.term,
            leader_node_id: log_id.leader_node_id,
            index: log_id.index,
        }
    }
}

impl From<RaftLogIdProto> for RaftLogId {
    fn from(state_id: RaftLogIdProto) -> Self {
        RaftLogId::new(state_id.term, state_id.leader_node_id, state_id.index)
    }
}

impl TryFrom<GroupStateWatermarkProto> for GroupStateWatermark {
    type Error = String;

    fn try_from(proto: GroupStateWatermarkProto) -> Result<Self, Self::Error> {
        let group_name = GroupName::parse(&proto.group_name)
            .map_err(|err| format!("invalid group_name in GroupStateWatermarkProto: {err}"))?;
        let state_id = proto
            .state_id
            .ok_or_else(|| "missing state_id in GroupStateWatermarkProto".to_string())?
            .into();
        Ok(GroupStateWatermark::new(group_name, state_id))
    }
}

impl From<&GroupStateWatermark> for GroupStateWatermarkProto {
    fn from(watermark: &GroupStateWatermark) -> Self {
        GroupStateWatermarkProto {
            state_id: Some(watermark.state_id.into()),
            group_name: watermark.group_name.to_string(),
        }
    }
}

// ============================================================================
// RequestHeaderProto / ResponseHeaderProto Conversions
// ============================================================================
//
// NOTE: This is the AUTHORITATIVE implementation of conversions between
// beryl_proto::common::RequestHeaderProto/ResponseHeaderProto and beryl_common::header types.
// All conversions MUST use these implementations.

impl TryFrom<ClientInfoProto> for ClientInfo {
    type Error = String;

    fn try_from(proto: ClientInfoProto) -> Result<Self, Self::Error> {
        let call_id = require_call_id(&proto.call_id, "call_id")?;
        let client_id = required_client_id(proto.client_id, "client_id")?;
        let client_name = if proto.client_name.is_empty() {
            None
        } else {
            Some(proto.client_name)
        };

        Ok(ClientInfo {
            call_id,
            client_id,
            client_name,
        })
    }
}

impl From<&ClientInfo> for ClientInfoProto {
    fn from(info: &ClientInfo) -> Self {
        ClientInfoProto {
            call_id: info.call_id.to_string(),
            client_id: Some(info.client_id.into()),
            client_name: info.client_name.clone().unwrap_or_default(),
        }
    }
}

impl From<TraceContextProto> for TraceContext {
    fn from(proto: TraceContextProto) -> Self {
        Self {
            traceparent: proto.traceparent.filter(|value| !value.is_empty()),
            tracestate: proto.tracestate.filter(|value| !value.is_empty()),
            baggage: proto.baggage.filter(|value| !value.is_empty()),
        }
    }
}

impl From<&TraceContext> for TraceContextProto {
    fn from(context: &TraceContext) -> Self {
        Self {
            traceparent: context.traceparent.clone(),
            tracestate: context.tracestate.clone(),
            baggage: context.baggage.clone(),
        }
    }
}

fn optional_trace_context(proto: Option<TraceContextProto>) -> TraceContext {
    proto.map(TraceContext::from).unwrap_or_default()
}

fn proto_trace_context(context: &TraceContext) -> Option<TraceContextProto> {
    if context.traceparent.is_none() && context.tracestate.is_none() && context.baggage.is_none() {
        None
    } else {
        Some(context.into())
    }
}

impl TryFrom<RequestHeaderProto> for RequestHeader {
    type Error = String;

    fn try_from(proto: RequestHeaderProto) -> Result<Self, Self::Error> {
        let client = proto.client.ok_or_else(|| "missing client".to_string())?.try_into()?;
        let deadline = Deadline::from_unix_ms(proto.deadline_ms);
        let trace_context = optional_trace_context(proto.trace_context);
        let caller_context = proto.caller_context.map(|cc| CallerContext { context: cc.context });
        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RequestHeader {
            client,
            trace_context,
            group_name: GroupName::parse_optional(&proto.group_name)
                .map_err(|err| format!("invalid header group_name: {err}"))?,
            mount_epoch: proto.mount_epoch,
            state,
            route_epoch: proto.route_epoch,
            deadline,
            caller_context,
        })
    }
}

impl From<&RequestHeader> for RequestHeaderProto {
    fn from(header: &RequestHeader) -> Self {
        RequestHeaderProto {
            client: Some((&header.client).into()),
            trace_context: proto_trace_context(&header.trace_context),
            group_name: header.group_name.as_ref().map(ToString::to_string).unwrap_or_default(),
            mount_epoch: header.mount_epoch,
            state: header.state.iter().map(GroupStateWatermarkProto::from).collect(),
            route_epoch: header.route_epoch,
            deadline_ms: header.deadline.as_unix_ms(),
            caller_context: header.caller_context.as_ref().map(|cc| CallerContextProto {
                context: cc.context.clone(),
            }),
        }
    }
}

impl TryFrom<ResponseHeaderProto> for ResponseHeader {
    type Error = String;

    fn try_from(proto: ResponseHeaderProto) -> Result<Self, Self::Error> {
        let client = proto
            .client
            .clone()
            .ok_or_else(|| "missing client".to_string())?
            .try_into()?;

        let rpc_error = proto.error.as_ref().map(rpc_error_from_proto);

        let state = proto
            .state
            .into_iter()
            .map(GroupStateWatermark::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResponseHeader {
            client,
            rpc_error,
            state,
            mount_epoch: proto.mount_epoch,
            route_epoch: proto.route_epoch,
            group_name: GroupName::parse_optional(&proto.group_name)
                .map_err(|err| format!("invalid header group_name: {err}"))?,
        })
    }
}

impl From<&ResponseHeader> for ResponseHeaderProto {
    fn from(header: &ResponseHeader) -> Self {
        let error_detail = header.rpc_error.as_ref().map(rpc_error_to_proto);

        ResponseHeaderProto {
            client: Some((&header.client).into()),
            error: error_detail,
            state: header.state.iter().map(GroupStateWatermarkProto::from).collect(),
            mount_epoch: header.mount_epoch,
            route_epoch: header.route_epoch,
            group_name: header.group_name.as_ref().map(ToString::to_string).unwrap_or_default(),
        }
    }
}

// ============================================================================
// RPC error helpers (shared between control/data-plane conversions)
// ============================================================================

fn metadata_kind_proto_to_kind(kind: MetadataErrorKindProto) -> Option<MetadataErrorKind> {
    Some(match kind {
        MetadataErrorKindProto::MetadataErrorKindUnspecified => return None,
        MetadataErrorKindProto::MetadataErrorKindNotFound => MetadataErrorKind::NotFound,
        MetadataErrorKindProto::MetadataErrorKindAlreadyExists => MetadataErrorKind::AlreadyExists,
        MetadataErrorKindProto::MetadataErrorKindNotDirectory => MetadataErrorKind::NotDirectory,
        MetadataErrorKindProto::MetadataErrorKindIsDirectory => MetadataErrorKind::IsDirectory,
        MetadataErrorKindProto::MetadataErrorKindDirectoryNotEmpty => MetadataErrorKind::DirectoryNotEmpty,
        MetadataErrorKindProto::MetadataErrorKindCrossMountRename => MetadataErrorKind::CrossMountRename,
        MetadataErrorKindProto::MetadataErrorKindBusy => MetadataErrorKind::Busy,
        MetadataErrorKindProto::MetadataErrorKindConflict => MetadataErrorKind::Conflict,
        MetadataErrorKindProto::MetadataErrorKindNotLeader => MetadataErrorKind::NotLeader,
        MetadataErrorKindProto::MetadataErrorKindStaleState => MetadataErrorKind::StaleState,
        MetadataErrorKindProto::MetadataErrorKindMountEpochMismatch => MetadataErrorKind::MountEpochMismatch,
        MetadataErrorKindProto::MetadataErrorKindRouteEpochMismatch => MetadataErrorKind::RouteEpochMismatch,
        MetadataErrorKindProto::MetadataErrorKindOwnerGroupMismatch => MetadataErrorKind::OwnerGroupMismatch,
        MetadataErrorKindProto::MetadataErrorKindGroupMismatch => MetadataErrorKind::GroupMismatch,
        MetadataErrorKindProto::MetadataErrorKindFencing => MetadataErrorKind::Fencing,
        MetadataErrorKindProto::MetadataErrorKindSessionInvalid => MetadataErrorKind::SessionInvalid,
        MetadataErrorKindProto::MetadataErrorKindSessionExpired => MetadataErrorKind::SessionExpired,
        MetadataErrorKindProto::MetadataErrorKindEpochMismatch => MetadataErrorKind::EpochMismatch,
        MetadataErrorKindProto::MetadataErrorKindResourceExhausted => MetadataErrorKind::ResourceExhausted,
    })
}

fn metadata_kind_to_proto(kind: MetadataErrorKind) -> MetadataErrorKindProto {
    match kind {
        MetadataErrorKind::NotFound => MetadataErrorKindProto::MetadataErrorKindNotFound,
        MetadataErrorKind::AlreadyExists => MetadataErrorKindProto::MetadataErrorKindAlreadyExists,
        MetadataErrorKind::NotDirectory => MetadataErrorKindProto::MetadataErrorKindNotDirectory,
        MetadataErrorKind::IsDirectory => MetadataErrorKindProto::MetadataErrorKindIsDirectory,
        MetadataErrorKind::DirectoryNotEmpty => MetadataErrorKindProto::MetadataErrorKindDirectoryNotEmpty,
        MetadataErrorKind::CrossMountRename => MetadataErrorKindProto::MetadataErrorKindCrossMountRename,
        MetadataErrorKind::Busy => MetadataErrorKindProto::MetadataErrorKindBusy,
        MetadataErrorKind::Conflict => MetadataErrorKindProto::MetadataErrorKindConflict,
        MetadataErrorKind::NotLeader => MetadataErrorKindProto::MetadataErrorKindNotLeader,
        MetadataErrorKind::StaleState => MetadataErrorKindProto::MetadataErrorKindStaleState,
        MetadataErrorKind::MountEpochMismatch => MetadataErrorKindProto::MetadataErrorKindMountEpochMismatch,
        MetadataErrorKind::RouteEpochMismatch => MetadataErrorKindProto::MetadataErrorKindRouteEpochMismatch,
        MetadataErrorKind::OwnerGroupMismatch => MetadataErrorKindProto::MetadataErrorKindOwnerGroupMismatch,
        MetadataErrorKind::GroupMismatch => MetadataErrorKindProto::MetadataErrorKindGroupMismatch,
        MetadataErrorKind::Fencing => MetadataErrorKindProto::MetadataErrorKindFencing,
        MetadataErrorKind::SessionInvalid => MetadataErrorKindProto::MetadataErrorKindSessionInvalid,
        MetadataErrorKind::SessionExpired => MetadataErrorKindProto::MetadataErrorKindSessionExpired,
        MetadataErrorKind::EpochMismatch => MetadataErrorKindProto::MetadataErrorKindEpochMismatch,
        MetadataErrorKind::ResourceExhausted => MetadataErrorKindProto::MetadataErrorKindResourceExhausted,
    }
}

fn worker_kind_proto_to_kind(kind: WorkerErrorKindProto) -> Option<WorkerErrorKind> {
    Some(match kind {
        WorkerErrorKindProto::WorkerErrorKindUnspecified => return None,
        WorkerErrorKindProto::WorkerErrorKindNotRegistered => WorkerErrorKind::NotRegistered,
        WorkerErrorKindProto::WorkerErrorKindRunMismatch => WorkerErrorKind::RunMismatch,
        WorkerErrorKindProto::WorkerErrorKindDescriptorMismatch => WorkerErrorKind::DescriptorMismatch,
        WorkerErrorKindProto::WorkerErrorKindFullReportRequired => WorkerErrorKind::FullReportRequired,
        WorkerErrorKindProto::WorkerErrorKindBlockLocationUnavailable => WorkerErrorKind::BlockLocationUnavailable,
        WorkerErrorKindProto::WorkerErrorKindNodeUnavailable => WorkerErrorKind::NodeUnavailable,
        WorkerErrorKindProto::WorkerErrorKindTimeout => WorkerErrorKind::Timeout,
        WorkerErrorKindProto::WorkerErrorKindResourceExhausted => WorkerErrorKind::ResourceExhausted,
        WorkerErrorKindProto::WorkerErrorKindConflict => WorkerErrorKind::Conflict,
        WorkerErrorKindProto::WorkerErrorKindCorrupt => WorkerErrorKind::Corrupt,
        WorkerErrorKindProto::WorkerErrorKindFencing => WorkerErrorKind::Fencing,
        WorkerErrorKindProto::WorkerErrorKindCancelled => WorkerErrorKind::Cancelled,
        WorkerErrorKindProto::WorkerErrorKindIo => WorkerErrorKind::Io,
        WorkerErrorKindProto::WorkerErrorKindNotFound => WorkerErrorKind::NotFound,
    })
}

fn worker_kind_to_proto(kind: WorkerErrorKind) -> WorkerErrorKindProto {
    match kind {
        WorkerErrorKind::NotRegistered => WorkerErrorKindProto::WorkerErrorKindNotRegistered,
        WorkerErrorKind::RunMismatch => WorkerErrorKindProto::WorkerErrorKindRunMismatch,
        WorkerErrorKind::DescriptorMismatch => WorkerErrorKindProto::WorkerErrorKindDescriptorMismatch,
        WorkerErrorKind::FullReportRequired => WorkerErrorKindProto::WorkerErrorKindFullReportRequired,
        WorkerErrorKind::BlockLocationUnavailable => WorkerErrorKindProto::WorkerErrorKindBlockLocationUnavailable,
        WorkerErrorKind::NodeUnavailable => WorkerErrorKindProto::WorkerErrorKindNodeUnavailable,
        WorkerErrorKind::Timeout => WorkerErrorKindProto::WorkerErrorKindTimeout,
        WorkerErrorKind::ResourceExhausted => WorkerErrorKindProto::WorkerErrorKindResourceExhausted,
        WorkerErrorKind::Conflict => WorkerErrorKindProto::WorkerErrorKindConflict,
        WorkerErrorKind::Corrupt => WorkerErrorKindProto::WorkerErrorKindCorrupt,
        WorkerErrorKind::Fencing => WorkerErrorKindProto::WorkerErrorKindFencing,
        WorkerErrorKind::Cancelled => WorkerErrorKindProto::WorkerErrorKindCancelled,
        WorkerErrorKind::Io => WorkerErrorKindProto::WorkerErrorKindIo,
        WorkerErrorKind::NotFound => WorkerErrorKindProto::WorkerErrorKindNotFound,
    }
}

fn protocol_kind_proto_to_kind(kind: ProtocolErrorKindProto) -> Option<ProtocolErrorKind> {
    Some(match kind {
        ProtocolErrorKindProto::ProtocolErrorKindUnspecified => return None,
        ProtocolErrorKindProto::ProtocolErrorKindInvalidHeader => ProtocolErrorKind::InvalidHeader,
        ProtocolErrorKindProto::ProtocolErrorKindInvalidArgument => ProtocolErrorKind::InvalidArgument,
        ProtocolErrorKindProto::ProtocolErrorKindPermissionDenied => ProtocolErrorKind::PermissionDenied,
        ProtocolErrorKindProto::ProtocolErrorKindUnsupported => ProtocolErrorKind::Unsupported,
        ProtocolErrorKindProto::ProtocolErrorKindCancelled => ProtocolErrorKind::Cancelled,
        ProtocolErrorKindProto::ProtocolErrorKindCorrupt => ProtocolErrorKind::Corrupt,
    })
}

fn protocol_kind_to_proto(kind: ProtocolErrorKind) -> ProtocolErrorKindProto {
    match kind {
        ProtocolErrorKind::InvalidHeader => ProtocolErrorKindProto::ProtocolErrorKindInvalidHeader,
        ProtocolErrorKind::InvalidArgument => ProtocolErrorKindProto::ProtocolErrorKindInvalidArgument,
        ProtocolErrorKind::PermissionDenied => ProtocolErrorKindProto::ProtocolErrorKindPermissionDenied,
        ProtocolErrorKind::Unsupported => ProtocolErrorKindProto::ProtocolErrorKindUnsupported,
        ProtocolErrorKind::Cancelled => ProtocolErrorKindProto::ProtocolErrorKindCancelled,
        ProtocolErrorKind::Corrupt => ProtocolErrorKindProto::ProtocolErrorKindCorrupt,
    }
}

fn internal_kind_proto_to_kind(kind: InternalErrorKindProto) -> Option<InternalErrorKind> {
    Some(match kind {
        InternalErrorKindProto::InternalErrorKindUnspecified => return None,
        InternalErrorKindProto::InternalErrorKindNodeUnavailable => InternalErrorKind::NodeUnavailable,
        InternalErrorKindProto::InternalErrorKindTimeout => InternalErrorKind::Timeout,
        InternalErrorKindProto::InternalErrorKindResourceExhausted => InternalErrorKind::ResourceExhausted,
        InternalErrorKindProto::InternalErrorKindCancelled => InternalErrorKind::Cancelled,
        InternalErrorKindProto::InternalErrorKindCorrupt => InternalErrorKind::Corrupt,
        InternalErrorKindProto::InternalErrorKindInternal => InternalErrorKind::Internal,
    })
}

fn internal_kind_to_proto(kind: InternalErrorKind) -> InternalErrorKindProto {
    match kind {
        InternalErrorKind::NodeUnavailable => InternalErrorKindProto::InternalErrorKindNodeUnavailable,
        InternalErrorKind::Timeout => InternalErrorKindProto::InternalErrorKindTimeout,
        InternalErrorKind::ResourceExhausted => InternalErrorKindProto::InternalErrorKindResourceExhausted,
        InternalErrorKind::Cancelled => InternalErrorKindProto::InternalErrorKindCancelled,
        InternalErrorKind::Corrupt => InternalErrorKindProto::InternalErrorKindCorrupt,
        InternalErrorKind::Internal => InternalErrorKindProto::InternalErrorKindInternal,
    }
}

fn error_kind_proto_to_kind(kind: Option<&ErrorKindProto>) -> Option<ErrorKind> {
    match kind.and_then(|kind| kind.kind.as_ref()) {
        Some(Kind::Metadata(kind)) => {
            let kind = MetadataErrorKindProto::try_from(*kind).ok()?;
            Some(ErrorKind::Metadata(metadata_kind_proto_to_kind(kind)?))
        }
        Some(Kind::Worker(kind)) => {
            let kind = WorkerErrorKindProto::try_from(*kind).ok()?;
            Some(ErrorKind::Worker(worker_kind_proto_to_kind(kind)?))
        }
        Some(Kind::Protocol(kind)) => {
            let kind = ProtocolErrorKindProto::try_from(*kind).ok()?;
            Some(ErrorKind::Protocol(protocol_kind_proto_to_kind(kind)?))
        }
        Some(Kind::Internal(kind)) => {
            let kind = InternalErrorKindProto::try_from(*kind).ok()?;
            Some(ErrorKind::Internal(internal_kind_proto_to_kind(kind)?))
        }
        None => None,
    }
}

fn error_kind_to_proto(kind: ErrorKind) -> ErrorKindProto {
    let kind = match kind {
        ErrorKind::Metadata(kind) => Kind::Metadata(metadata_kind_to_proto(kind) as i32),
        ErrorKind::Worker(kind) => Kind::Worker(worker_kind_to_proto(kind) as i32),
        ErrorKind::Protocol(kind) => Kind::Protocol(protocol_kind_to_proto(kind) as i32),
        ErrorKind::Internal(kind) => Kind::Internal(internal_kind_to_proto(kind) as i32),
    };
    ErrorKindProto { kind: Some(kind) }
}

fn refresh_hint_proto_to_hint(hint: Option<&RefreshHintProto>) -> RefreshHint {
    hint.map_or_else(RefreshHint::default, |hint| RefreshHint {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_name: hint.group_name.clone(),
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(|endpoint| WorkerEndpointHint {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    })
}

fn refresh_hint_to_proto(hint: &RefreshHint) -> RefreshHintProto {
    RefreshHintProto {
        leader_endpoint: hint.leader_endpoint.clone(),
        group_name: hint.group_name.clone(),
        mount_epoch: hint.mount_epoch,
        mount_prefix: hint.mount_prefix.clone(),
        route_epoch: hint.route_epoch,
        worker_endpoints: hint
            .worker_endpoints
            .iter()
            .map(|endpoint| WorkerEndpointInfoProto {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                worker_run_id: String::new(),
            })
            .collect(),
        worker_resolve_required: hint.worker_resolve_required,
    }
}

fn recovery_proto_to_action(recovery: Option<&RecoveryActionProto>) -> Option<RecoveryAction> {
    match recovery.and_then(|recovery| recovery.action.as_ref()) {
        Some(Action::Fail(_)) => Some(RecoveryAction::Fail),
        Some(Action::Retry(retry)) => Some(RecoveryAction::Retry {
            after_ms: retry.after_ms,
        }),
        Some(Action::RefreshMetadata(refresh)) => Some(RecoveryAction::RefreshMetadata {
            hint: refresh_hint_proto_to_hint(refresh.hint.as_ref()),
        }),
        Some(Action::ReopenWriteSession(reopen)) => Some(RecoveryAction::ReopenWriteSession {
            hint: refresh_hint_proto_to_hint(reopen.hint.as_ref()),
        }),
        Some(Action::RegisterWorker(_)) => Some(RecoveryAction::RegisterWorker),
        Some(Action::SendFullBlockReport(_)) => Some(RecoveryAction::SendFullBlockReport),
        None => None,
    }
}

fn recovery_action_to_proto(action: &RecoveryAction) -> RecoveryActionProto {
    let action = match action {
        RecoveryAction::Fail => Action::Fail(FailRecoveryProto {}),
        RecoveryAction::Retry { after_ms } => Action::Retry(RetryRecoveryProto { after_ms: *after_ms }),
        RecoveryAction::RefreshMetadata { hint } => Action::RefreshMetadata(RefreshMetadataRecoveryProto {
            hint: Some(refresh_hint_to_proto(hint)),
        }),
        RecoveryAction::ReopenWriteSession { hint } => Action::ReopenWriteSession(ReopenWriteSessionRecoveryProto {
            hint: Some(refresh_hint_to_proto(hint)),
        }),
        RecoveryAction::RegisterWorker => Action::RegisterWorker(RegisterWorkerRecoveryProto {}),
        RecoveryAction::SendFullBlockReport => Action::SendFullBlockReport(SendFullBlockReportRecoveryProto {}),
    };
    RecoveryActionProto { action: Some(action) }
}

/// Convert proto ErrorDetailProto into RPC error.
///
/// Missing or unknown failure facts and recovery actions fail closed as an
/// invalid header. Malformed input cannot retain a retry or refresh action
/// supplied by the wire payload.
pub fn rpc_error_from_proto(err_detail: &ErrorDetailProto) -> RpcErrorDetail {
    let (Some(kind), Some(recovery)) = (
        error_kind_proto_to_kind(err_detail.kind.as_ref()),
        recovery_proto_to_action(err_detail.recovery.as_ref()),
    ) else {
        return RpcErrorDetail::fail(
            ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader),
            "malformed RPC error detail",
        );
    };
    RpcErrorDetail {
        kind,
        recovery,
        message: err_detail.message.clone(),
    }
}

/// Convert RPC error into proto ErrorDetailProto.
pub fn rpc_error_to_proto(err: &RpcErrorDetail) -> ErrorDetailProto {
    ErrorDetailProto {
        kind: Some(error_kind_to_proto(err.kind)),
        recovery: Some(recovery_action_to_proto(&err.recovery)),
        message: err.message.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn write_domain_values_preserve_wire_identity_and_intent() {
        for (kind, raw) in [(FileType::File, 1), (FileType::Dir, 2)] {
            let wire = FileTypeProto::from(kind);
            assert_eq!(wire as i32, raw);
            assert_eq!(FileType::try_from(wire).unwrap(), kind);
        }
        assert!(FileType::try_from(FileTypeProto::FileTypeUnspecified).is_err());
        assert!(FileTypeProto::try_from(i32::MAX).is_err());
        let bytes = [8, 42, 16, 17];
        let handle = WriteHandle::try_from(WriteHandleProto::decode(bytes.as_slice()).unwrap()).unwrap();
        assert_eq!(
            handle,
            WriteHandle {
                inode_id: InodeId::new(42),
                lease_epoch: LeaseEpoch::new(17)
            }
        );
        assert_eq!(WriteHandleProto::from(handle).encode_to_vec(), bytes);
        for (inode_id, write_lease_epoch) in [(0, 17), (42, 0)] {
            assert!(
                WriteHandle::try_from(WriteHandleProto {
                    inode_id,
                    write_lease_epoch
                })
                .is_err()
            );
        }
        for (mode, wire) in [(WriteMode::Overwrite, 1), (WriteMode::Append, 2)] {
            let encoded = OpenWriteModeProto::from(mode);
            assert_eq!(encoded as i32, wire);
            assert_eq!(parse_write_mode(wire).unwrap(), mode);
        }
        for raw in [0, -1, i32::MAX] {
            assert!(parse_write_mode(raw).is_err());
        }

        let token = FencingToken::new(
            BlockId::from_u64_u32(42, u32::MAX),
            ClientId::new(9),
            LeaseEpoch::new(17),
        );
        let wire = FencingTokenProto::from(token);
        assert_eq!(wire.block_id.unwrap().block_index, u32::MAX);
        assert_eq!(wire.epoch, 17);
        assert_eq!(FencingToken::try_from(wire).unwrap(), token);
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid test WorkerRunId")
    }

    #[test]
    fn malformed_rpc_error_details_fail_closed_without_recovery() {
        let kind = |value| ErrorKindProto {
            kind: Some(Kind::Metadata(value)),
        };
        let retry = Some(RecoveryActionProto {
            action: Some(Action::Retry(RetryRecoveryProto { after_ms: Some(1) })),
        });
        let refresh = Some(RecoveryActionProto {
            action: Some(Action::RefreshMetadata(RefreshMetadataRecoveryProto {
                hint: Some(RefreshHintProto::default()),
            })),
        });
        let valid = Some(kind(MetadataErrorKindProto::MetadataErrorKindNotFound as i32));
        for (kind, recovery) in [
            (Some(kind(i32::MAX)), retry.clone()),
            (
                Some(kind(MetadataErrorKindProto::MetadataErrorKindUnspecified as i32)),
                retry,
            ),
            (None, refresh),
            (valid, None),
            (valid, Some(RecoveryActionProto { action: None })),
        ] {
            let encoded = ErrorDetailProto {
                kind,
                recovery,
                message: String::new(),
            };
            let decoded = rpc_error_from_proto(&encoded);
            assert_eq!(decoded.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
            assert_eq!(decoded.recovery, RecoveryAction::Fail);
            assert_eq!(decoded.message, "malformed RPC error detail");
        }
    }

    #[test]
    fn shared_location_conversion_rejects_malformed_required_fields() {
        let endpoint = || WorkerEndpointInfoProto {
            worker_id: 7,
            endpoint: "127.0.0.1:19101".to_string(),
            worker_run_id: test_worker_run_id().to_string(),
        };
        let block_id = BlockId::from_u64_u32(42, 3);
        let token = FencingToken::new(block_id, ClientId::new(9), LeaseEpoch::new(17));

        let mut target = LocatedBlockProto {
            write_offset: 0,
            block_id: Some(block_id.into()),
            file_offset: 128,
            worker_endpoints: Vec::new(),
            fencing_token: Some(token.into()),

            chunk_size: BlockFormatId::DURABLE_PREFIX.spec().unwrap().storage_chunk_size,
            block_format_id: BlockFormatId::DURABLE_PREFIX.as_raw(),
            block_size: 4096,
            tier: TierProto::TierHdd as i32,
        };
        let err = LocatedBlock::try_from(target.clone()).expect_err("empty target workers must fail");
        assert!(err.contains("worker_endpoints"));
        target.worker_endpoints.push(endpoint());
        let decoded = LocatedBlock::try_from(target.clone()).expect("valid allocated block");
        assert_eq!(LocatedBlockProto::from(decoded), target);
        target.write_offset = target.block_size;
        assert!(LocatedBlock::try_from(target).is_err());

        let mut location = FileBlockLocationProto {
            block_id: Some(block_id.into()),
            file_offset: 128,
            len: 4096,
            workers: Vec::new(),

            block_format_id: BlockFormatId::DURABLE_PREFIX.as_raw(),
            block_size: 4096,
            chunk_size: BlockFormatId::DURABLE_PREFIX.spec().unwrap().storage_chunk_size,
            effective_len: 4096,
        };
        let decoded_empty =
            FileBlockLocation::try_from(location.clone()).expect("empty read location workers are valid");
        assert!(decoded_empty.workers.is_empty());
        location.workers.push(endpoint());
        let decoded = FileBlockLocation::try_from(location.clone()).expect("valid read location");
        assert_eq!(FileBlockLocationProto::from(decoded), location);
    }
}
