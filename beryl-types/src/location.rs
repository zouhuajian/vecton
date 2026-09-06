// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared read/write location value objects.

use serde::{Deserialize, Serialize};

use crate::ids::BlockId;
use crate::layout::BlockFormatId;
use crate::lease::FencingToken;
use crate::tier::Tier;
use crate::worker::WorkerEndpointInfo;

/// Metadata-issued block identity, write locations, layout, and fencing authority.
///
/// Worker locations designate where the client may write; they do not prove that
/// data exists, is durable, or has been published in the file's visible layout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocatedBlock {
    pub block_id: BlockId,
    /// Start of the block in the file, independent of its allocation index.
    pub file_offset: u64,
    /// Maximum writable capacity authorized by the persisted `FileLayout`.
    ///
    /// Workers reserve and enforce this bound before the final effective length
    /// is known, then persist it in `BlockMeta.format.block_size`.
    pub block_size: u64,
    /// Selected Worker process identities retained unchanged when allocation replays.
    pub worker_endpoints: Vec<WorkerEndpointInfo>,
    pub fencing_token: FencingToken,
    /// Block-local start of the next write: zero for allocation, the visible
    /// prefix for OpenWrite, or a locally confirmed checkpoint for continuation.
    pub write_offset: u64,

    pub chunk_size: u32,
    /// Metadata-selected Beryl block data/meta interpretation format.
    pub block_format_id: BlockFormatId,
    /// Worker-local storage tier requested for this replica.
    pub tier: Tier,
}

/// Changed tail or new block included in a Metadata content publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBlock {
    pub block_id: BlockId,
    /// Total block length, including the previously visible tail prefix.
    pub len: u64,
}

/// Metadata-authoritative readable location for one file range backed by a block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileBlockLocation {
    pub block_id: BlockId,
    pub file_offset: u64,
    pub len: u64,
    /// Metadata-issued read candidates. Empty means the authoritative layout has
    /// this block range but no live reported replica is currently eligible.
    pub workers: Vec<WorkerEndpointInfo>,

    /// Metadata-selected Beryl block data/meta interpretation format.
    pub block_format_id: BlockFormatId,
    /// Full logical block size from the persisted `FileLayout`.
    pub block_size: u64,
    /// Metadata-selected StorageChunk size for this block.
    pub chunk_size: u32,
    /// Block-local readable prefix expected by metadata.
    pub effective_len: u64,
}
