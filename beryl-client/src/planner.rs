// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Read planning from metadata block locations to worker block reads.

use crate::error::{ClientError, ClientResult, RefreshHint as ClientRefreshHint};
use crate::metadata::ReadLayout;
use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind};
use beryl_types::{
    BlockFormatId, BlockId, BlockShape, ContentGeneration, FileBlockLocation, GroupName, InodeId, WorkerEndpointInfo,
};

/// File byte range requested by a reader after EOF truncation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RequestedReadRange {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
}

impl RequestedReadRange {
    pub(crate) fn end_file_offset(self) -> u64 {
        self.file_offset + self.len as u64
    }
}

/// A block-local worker read planned from metadata block locations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedBlockRead {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
    pub(crate) end_file_offset: u64,
    pub(crate) block_id: BlockId,
    pub(crate) block_offset: u64,

    pub(crate) block_format_id: BlockFormatId,
    pub(crate) block_size: u64,
    pub(crate) chunk_size: u32,
    pub(crate) effective_len: u64,
    pub(crate) workers: Vec<WorkerEndpointInfo>,
}

pub(crate) fn requested_range(offset: u64, len: u32, file_size: u64) -> ClientResult<Option<RequestedReadRange>> {
    if len == 0 || offset >= file_size {
        return Ok(None);
    }
    let requested_end = offset
        .checked_add(len as u64)
        .ok_or_else(|| ClientError::invalid_argument("read range offset overflow".to_string()))?;
    let end = requested_end.min(file_size);
    let effective_len = end
        .checked_sub(offset)
        .ok_or_else(|| ClientError::invalid_argument("read range end precedes offset".to_string()))?;
    let effective_len = u32::try_from(effective_len)
        .map_err(|_| ClientError::invalid_argument("read range length exceeds u32".to_string()))?;
    if effective_len == 0 {
        return Ok(None);
    }
    Ok(Some(RequestedReadRange {
        file_offset: offset,
        len: effective_len,
    }))
}

pub(crate) fn plan_block_reads(
    expected_inode_id: InodeId,
    requested_range: RequestedReadRange,
    locations: &[FileBlockLocation],
) -> ClientResult<Vec<PlannedBlockRead>> {
    let mut normalized = Vec::with_capacity(locations.len());
    for location in locations {
        if location.len == 0 {
            return Err(ClientError::invalid_layout("zero-length block location".to_string()));
        }
        let end = location
            .file_offset
            .checked_add(location.len)
            .ok_or_else(|| ClientError::invalid_layout("block location range overflow".to_string()))?;
        let block_id = location.block_id;
        if block_id.inode_id != expected_inode_id {
            return Err(ClientError::invalid_layout(format!(
                "block location inode_id {} does not match handle {}",
                block_id.inode_id.as_raw(),
                expected_inode_id.as_raw()
            )));
        }
        BlockShape::new(
            location.block_format_id,
            location.block_size,
            location.chunk_size,
            location.effective_len,
        )
        .map_err(|error| ClientError::invalid_layout(format!("invalid block shape: {error}")))?;
        if location.workers.is_empty() {
            return Err(block_location_unavailable_error(format!(
                "block location unavailable: metadata returned no worker candidates for block {} file_offset={} len={}",
                block_id, location.file_offset, location.len
            )));
        }
        if end <= requested_range.file_offset || location.file_offset >= requested_range.end_file_offset() {
            continue;
        }
        normalized.push((location.file_offset, end, block_id, location));
    }
    normalized.sort_by_key(|(start, _, block_id, _)| (*start, block_id.index.as_raw()));

    let mut block_reads = Vec::with_capacity(normalized.len());
    let mut cursor = requested_range.file_offset;
    let requested_end = requested_range.end_file_offset();
    let mut previous_end = None;

    for (start, end, block_id, location) in normalized {
        if let Some(prev_end) = previous_end {
            if start < prev_end {
                return Err(ClientError::invalid_layout(format!(
                    "layout overlap at file offset {start}"
                )));
            }
        }
        previous_end = Some(end);

        if start > cursor {
            return Err(ClientError::invalid_layout(format!(
                "layout gap at file offset {cursor}"
            )));
        }
        if end <= cursor {
            continue;
        }

        let read_start = cursor.max(start);
        let read_end = requested_end.min(end);
        if read_start >= read_end {
            continue;
        }
        let len = u32::try_from(read_end - read_start)
            .map_err(|_| ClientError::invalid_layout("planned block read length exceeds u32".to_string()))?;
        if len == 0 {
            return Err(ClientError::invalid_layout(
                "zero-length planned block read".to_string(),
            ));
        }
        block_reads.push(PlannedBlockRead {
            file_offset: read_start,
            len,
            end_file_offset: read_end,
            block_id,
            block_offset: read_start - start,
            block_format_id: location.block_format_id,
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
            workers: location.workers.clone(),
        });
        cursor = read_end;
        if cursor == requested_end {
            break;
        }
    }

    if cursor < requested_end {
        return Err(ClientError::invalid_layout(format!(
            "layout gap at file offset {cursor}"
        )));
    }
    Ok(block_reads)
}

pub(crate) fn plan_block_reads_from_layout(
    expected_inode_id: InodeId,
    expected_generation: ContentGeneration,
    expected_file_size: u64,
    requested_range: RequestedReadRange,
    response: &ReadLayout,
) -> ClientResult<(GroupName, Vec<PlannedBlockRead>)> {
    let group_name = response.group_name.clone();
    let inode_id = response.inode_id;
    if inode_id != expected_inode_id {
        return Err(ClientError::stale_handle(format!(
            "layout inode_id {} does not match handle {}",
            inode_id.as_raw(),
            expected_inode_id.as_raw()
        )));
    }
    let generation = generation_from_response(response.generation, "GetBlockLocationsResponseProto.generation")?;
    if generation != expected_generation {
        return Err(ClientError::generation_mismatch(expected_generation, generation));
    }
    if response.file_size != expected_file_size {
        return Err(ClientError::stale_handle(format!(
            "layout file size {} does not match opened length {}",
            response.file_size, expected_file_size
        )));
    }
    let block_reads = plan_block_reads(expected_inode_id, requested_range, &response.locations)?;
    Ok((group_name, block_reads))
}

/// Require Metadata visibility authority before accepting a read layout.
fn generation_from_response(value: Option<ContentGeneration>, field: &str) -> ClientResult<ContentGeneration> {
    value.ok_or_else(|| ClientError::invalid_layout(format!("{field} missing")))
}

pub(crate) fn block_location_unavailable_error(message: impl Into<String>) -> ClientError {
    let rpc_error = RpcErrorDetail::refresh_metadata(
        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        RefreshHint {
            worker_resolve_required: true,
            ..RefreshHint::default()
        },
        message,
    );
    ClientError::from_remote(
        rpc_error,
        ClientRefreshHint {
            worker_resolve_required: true,
            ..ClientRefreshHint::default()
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockId, BlockIndex, InodeId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

    #[test]
    fn planner_rejects_invalid_location_coverage_and_shape() {
        let cases = vec![
            (
                "gap",
                12,
                vec![location(10, 0, 0, 4), location(10, 1, 8, 8)],
                "layout gap",
            ),
            (
                "overlap",
                12,
                vec![location(10, 0, 0, 8), location(10, 1, 4, 8)],
                "layout overlap",
            ),
            ("zero length", 4, vec![location(10, 0, 0, 0)], "zero-length"),
        ];

        for (case, len, locations, expected) in cases {
            let requested_range = requested_range(0, len, 20)
                .expect("range planning succeeds")
                .expect("non-empty requested range");
            let err = plan_block_reads(InodeId::new(10), requested_range, &locations).expect_err("layout must fail");
            assert!(
                format!("{err}").contains(expected),
                "case {case} should mention {expected:?}, got {err}"
            );
        }

        let range = requested_range(0, 4, 4).expect("range").expect("nonempty range");
        for (generation, file_size, expected) in [
            (None, 4, "generation missing"),
            (Some(2), 4, "generation mismatch"),
            (Some(1), 5, "opened length"),
        ] {
            let layout = ReadLayout {
                group_name: GroupName::parse("root").expect("group name"),
                inode_id: InodeId::new(10),
                file_size,
                generation: generation.map(ContentGeneration::new),
                locations: vec![location(10, 0, 0, 4)],
            };
            let err = plan_block_reads_from_layout(InodeId::new(10), ContentGeneration::new(1), 4, range, &layout)
                .expect_err("opened-file authority mismatch must fail");
            assert!(err.message().contains(expected), "expected {expected:?}, got {err}");
        }
    }

    fn location(inode_id: u64, block_index: u32, file_offset: u64, len: u64) -> FileBlockLocation {
        FileBlockLocation {
            block_id: BlockId::new(InodeId::new(inode_id), BlockIndex::new(block_index)),
            file_offset,
            len,
            workers: vec![WorkerEndpointInfo {
                worker_id: WorkerId::new(1),
                endpoint: "127.0.0.1:19101".to_string(),
                worker_net_protocol: WorkerNetProtocol::Grpc,
                worker_run_id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            }],
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
            effective_len: len,
        }
    }
}
