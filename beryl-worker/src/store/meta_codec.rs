// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Protobuf payload codec for worker-local block metadata.

use beryl_proto::common::TierProto;
use beryl_proto::convert::parse_known_tier;
use beryl_proto::worker::{
    BlockFormatProto, BlockIdentityProto, BlockMetaPayloadProto, BlockSourceProto, BlockStateProto,
    BlockVisibilityProto,
};
use beryl_types::ids::BlockId;
use beryl_types::layout::BlockFormatId;
use beryl_types::GroupName;
use prost::Message;

use super::block::{
    BlockFormat, BlockIdentity, BlockMetaPayload, BlockSource, BlockState, BlockVisibility, ChecksumKind, StoreResult,
};
use crate::error::WorkerError;

pub(super) fn encode_meta_payload(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    Ok(meta_to_proto(meta)?.encode_to_vec())
}

pub(super) fn decode_meta_payload(encoded: &[u8]) -> StoreResult<BlockMetaPayload> {
    let proto = BlockMetaPayloadProto::decode(encoded).map_err(|err| corrupt(err.to_string()))?;
    meta_from_proto(proto)
}

fn meta_to_proto(meta: &BlockMetaPayload) -> StoreResult<BlockMetaPayloadProto> {
    let chunk_size = u32::try_from(meta.format.chunk_size)
        .map_err(|_| WorkerError::InvalidArgument("chunk size does not fit block metadata format".to_string()))?;

    Ok(BlockMetaPayloadProto {
        identity: Some(BlockIdentityProto {
            block_id: Some(meta.identity.block_id.into()),
            group_name: meta.identity.group_name.to_string(),
        }),
        format: Some(BlockFormatProto {
            format_id: meta.format.format_id.as_raw(),
            block_size: meta.format.block_size,
            chunk_size,
        }),
        source: Some(BlockSourceProto {
            durable_len: meta.source.durable_len,
        }),
        visibility: Some(BlockVisibilityProto {
            block_state: block_state_to_proto(meta.visibility.block_state) as i32,
            fencing_token: Some(meta.visibility.fencing_token.into()),
        }),
        tier: TierProto::from(meta.tier) as i32,
    })
}

fn meta_from_proto(proto: BlockMetaPayloadProto) -> StoreResult<BlockMetaPayload> {
    let BlockMetaPayloadProto {
        identity,
        format,
        source,
        visibility,
        tier,
    } = proto;
    let identity = identity.ok_or_else(|| corrupt("block meta payload missing identity"))?;
    let block_id = identity
        .block_id
        .ok_or_else(|| corrupt("block meta payload missing block id"))?;
    let group_name = GroupName::parse(&identity.group_name)
        .map_err(|err| corrupt(format!("block meta payload invalid group name: {err}")))?;
    let format = format.ok_or_else(|| corrupt("block meta payload missing format"))?;
    let source = source.ok_or_else(|| corrupt("block meta payload missing source"))?;

    let visibility = visibility.ok_or_else(|| corrupt("block meta payload missing visibility"))?;
    let tier = parse_known_tier(tier).map_err(|err| corrupt(format!("block meta payload invalid tier: {err}")))?;
    Ok(BlockMetaPayload {
        visibility: BlockVisibility {
            block_state: block_state_from_proto(visibility.block_state)?,
            fencing_token: beryl_proto::convert::required_fencing_token(visibility.fencing_token, "block writer token")
                .map_err(corrupt)?,
        },
        tier,
        identity: BlockIdentity {
            block_id: BlockId::try_from(block_id)
                .map_err(|error| corrupt(format!("block meta payload invalid block id: {error}")))?,
            group_name,
        },
        format: BlockFormat {
            format_id: BlockFormatId::from_raw(format.format_id)
                .map_err(|err| corrupt(format!("unsupported block format id: {err}")))?,
            block_size: format.block_size,
            chunk_size: u64::from(format.chunk_size),
            checksum_kind: ChecksumKind::None,
        },
        source: BlockSource {
            durable_len: source.durable_len,
        },
    })
}

fn block_state_to_proto(block_state: BlockState) -> BlockStateProto {
    match block_state {
        BlockState::Deleting => BlockStateProto::BlockStateDeleting,
        BlockState::Ready => BlockStateProto::BlockStateReady,
        BlockState::Corrupt => BlockStateProto::BlockStateCorrupt,
    }
}

fn block_state_from_proto(block_state: i32) -> StoreResult<BlockState> {
    match BlockStateProto::try_from(block_state).map_err(|_| corrupt("unsupported block state"))? {
        BlockStateProto::BlockStateUnspecified => Err(corrupt("block state must be specified")),
        BlockStateProto::BlockStateDeleting => Ok(BlockState::Deleting),
        BlockStateProto::BlockStateReady => Ok(BlockState::Ready),
        BlockStateProto::BlockStateCorrupt => Ok(BlockState::Corrupt),
    }
}

fn corrupt(message: impl Into<String>) -> WorkerError {
    WorkerError::Corrupt(message.into())
}
