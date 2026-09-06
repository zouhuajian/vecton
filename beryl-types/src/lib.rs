#![forbid(unsafe_code)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//#![deny(missing_docs)]

//! Pure domain model.
//!
//! This crate must NOT depend on transport (gRPC/QUIC), storage engines, or OS specifics.
//! It contains only domain identifiers, layout/range/placement, block/chunk/stream/lease models,
//! and pure data structures like bitmap/range-set.

extern crate core;

pub mod chunk;
pub mod fs;
pub mod group_watermark;
pub mod ids;
pub mod layout;
pub mod lease;
pub mod location;
pub mod raft_log_id;
pub mod tier;
pub mod worker;

pub use fs::{ContentGeneration, FileType, MAX_FILE_BLOCKS, WriteMode};
pub use group_watermark::{GroupStateWatermark, MountEpoch};
pub use ids::{BlockId, BlockIndex, CallId, ClientId, GroupName, GroupNameError, InodeId, MountId, WorkerId};
pub use layout::{
    BlockFormatId, BlockFormatIdError, BlockShape, BlockShapeError, FileLayout, FileLayoutError, MAX_BLOCK_SIZE,
};
pub use lease::{FencingToken, LeaseEpoch, WriteHandle};
pub use location::{CommittedBlock, FileBlockLocation, LocatedBlock};
pub use raft_log_id::RaftLogId;
pub use tier::{Tier, TierError, TierFree};
pub use worker::{MAX_REPORT_ENTRIES, WorkerEndpointInfo, WorkerNetProtocol, WorkerRunId};
