// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Explicit conversion from worker wire messages to core domain types.

use beryl_proto::convert as proto_convert;
use beryl_proto::worker::{ReadBlockRequestProto, WriteBlockCommandProto};
use beryl_types::chunk::ByteRange;
use beryl_types::layout::BlockFormatId;
use beryl_types::{GroupName, WorkerRunId};

use crate::data::core::{ReadBlockRequest, WorkerCoreResult, WriteBlockRequest};
use crate::error::WorkerError;
use crate::store::block::ChecksumKind;

/// Converts and validates the metadata-issued facts on a read request.
pub(crate) fn proto_to_read_block_request(proto: ReadBlockRequestProto) -> WorkerCoreResult<ReadBlockRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id =
        proto_convert::required_block_id(proto.block_id, "block_id").map_err(WorkerError::InvalidArgument)?;
    let byte_range = proto
        .byte_range
        .ok_or_else(|| WorkerError::InvalidArgument("missing byte_range".to_string()))?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|error| WorkerError::InvalidArgument(format!("block_format_id invalid: {error}")))?;

    Ok(ReadBlockRequest {
        group_name,
        block_id,
        byte_range: ByteRange {
            offset: byte_range.offset,
            len: byte_range.len,
        },

        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
        effective_len: proto.effective_len,
        frame_size: proto.frame_size,
    })
}

/// Converts the command that must be the first payload of one block write.
pub(crate) fn proto_to_write_block_request(proto: WriteBlockCommandProto) -> WorkerCoreResult<WriteBlockRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id =
        proto_convert::required_block_id(proto.block_id, "block_id").map_err(WorkerError::InvalidArgument)?;
    let worker_run_id = proto
        .worker_run_id
        .parse::<WorkerRunId>()
        .map_err(|error| WorkerError::InvalidArgument(format!("worker_run_id invalid: {error}")))?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|error| WorkerError::InvalidArgument(format!("block_format_id invalid: {error}")))?;
    let tier = proto_convert::parse_known_tier(proto.tier)
        .map_err(|error| WorkerError::InvalidArgument(format!("tier invalid: {error}")))?;

    Ok(WriteBlockRequest {
        group_name,
        block_id,
        worker_run_id,
        fencing_token: proto_convert::required_fencing_token(proto.fencing_token, "fencing_token")
            .map_err(WorkerError::InvalidArgument)?,
        write_offset: proto.write_offset,
        block_size: proto.block_size,
        block_format_id,
        chunk_size: proto.chunk_size,
        checksum_kind: ChecksumKind::None,
        tier,
    })
}

fn proto_to_group_name(value: &str, field_name: &str) -> WorkerCoreResult<GroupName> {
    GroupName::parse(value).map_err(|error| WorkerError::InvalidArgument(format!("{field_name} invalid: {error}")))
}
