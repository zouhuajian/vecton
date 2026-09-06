// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker data-service wire conversion and response validation helpers.

use std::time::Duration;

use beryl_common::header::{HeaderIdentity, HEADER_WORKER_DATA_ERROR_DETAIL, WORKER_DATA_ERROR_DETAIL_V1};
use beryl_proto::worker::write_block_request_proto::Payload;
use beryl_types::chunk::ByteRange;
use beryl_types::{BlockShape, GroupName, WorkerEndpointInfo};
use bytes::Bytes;
use prost::Message;

use super::WorkerWriteTarget;
use crate::error::{ClientError, ClientResult};
use crate::planner::PlannedBlockRead;
use crate::rpc_error::{invalid_header_error, validate_data_header};
use crate::runtime::AttemptContext;

pub(super) fn build_read_block_request(
    attempt: &AttemptContext,
    group_name: &GroupName,
    block_read: &PlannedBlockRead,
    worker: &WorkerEndpointInfo,
) -> ClientResult<beryl_proto::worker::ReadBlockRequestProto> {
    BlockShape::new(
        block_read.block_format_id,
        block_read.block_size,
        block_read.chunk_size,
        block_read.effective_len,
    )
    .map_err(|error| {
        ClientError::invalid_layout(format!("planned block read has invalid expected block shape: {error}"))
    })?;
    Ok(beryl_proto::worker::ReadBlockRequestProto {
        header: Some(attempt.data_header()),
        group_name: group_name.to_string(),
        block_id: Some(block_read.block_id.into()),
        byte_range: Some(
            ByteRange {
                offset: block_read.block_offset,
                len: block_read.len,
            }
            .into(),
        ),

        frame_size: default_frame_size(block_read.len),
        worker_run_id: worker.worker_run_id.to_string(),
        block_format_id: block_read.block_format_id.as_raw(),
        block_size: block_read.block_size,
        chunk_size: block_read.chunk_size,
        effective_len: block_read.effective_len,
    })
}

/// Builds the sole control payload for one block write. Fencing remains a
/// Metadata concern and is intentionally absent from the Worker wire request.
pub(super) fn build_write_block_command(
    attempt: &AttemptContext,
    target: &WorkerWriteTarget,
    worker: &WorkerEndpointInfo,
) -> ClientResult<beryl_proto::worker::WriteBlockRequestProto> {
    validate_worker_write_target(target)?;
    Ok(beryl_proto::worker::WriteBlockRequestProto {
        payload: Some(Payload::Command(Box::new(
            beryl_proto::worker::WriteBlockCommandProto {
                header: Some(attempt.data_header()),
                group_name: target.group_name.to_string(),
                block_id: Some(target.target.block_id.into()),
                worker_run_id: worker.worker_run_id.to_string(),
                block_format_id: target.target.block_format_id.as_raw(),
                block_size: target.target.block_size,
                chunk_size: target.target.chunk_size,
                fencing_token: Some(target.target.fencing_token.into()),
                write_offset: target.target.write_offset,
                tier: beryl_proto::common::TierProto::from(target.target.tier) as i32,
            },
        ))),
    })
}

/// Builds one ordered data payload without sequence or offset fields.
pub(super) fn build_write_block_data(data: Bytes) -> ClientResult<beryl_proto::worker::WriteBlockRequestProto> {
    if data.is_empty() {
        return Err(ClientError::invalid_argument(
            "WriteBlock data payload must be nonempty".to_string(),
        ));
    }
    if data.len() > beryl_proto::MAX_WORKER_DATA_FRAME_SIZE as usize {
        return Err(ClientError::invalid_argument(format!(
            "WriteBlock data exceeds maximum frame size: actual={}, maximum={}",
            data.len(),
            beryl_proto::MAX_WORKER_DATA_FRAME_SIZE
        )));
    }
    Ok(beryl_proto::worker::WriteBlockRequestProto {
        payload: Some(Payload::Data(data)),
    })
}

/// Fills one caller-owned block range and accepts only exact stream completion.
pub(super) async fn read_block_stream_into(
    attempt: &AttemptContext,
    stream: &mut tonic::codec::Streaming<beryl_proto::worker::ReadBlockChunkProto>,
    block_read: &PlannedBlockRead,
    output: &mut [u8],
) -> ClientResult<()> {
    if output.len() != block_read.len as usize {
        return Err(ClientError::invalid_layout(format!(
            "worker output length {} does not match planned block read {}",
            output.len(),
            block_read.len
        )));
    }
    let mut filled = 0usize;
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|status| parse_worker_data_status(attempt, status))?
    {
        append_read_block_chunk(output, &mut filled, chunk)?;
    }
    finish_read_block_output(filled, output.len())
}

/// Copies one nonempty chunk without crossing the caller-owned output range.
pub(super) fn append_read_block_chunk(
    output: &mut [u8],
    filled: &mut usize,
    chunk: beryl_proto::worker::ReadBlockChunkProto,
) -> ClientResult<()> {
    if chunk.data.is_empty() {
        return Err(ClientError::invalid_response(
            "ReadBlock",
            "worker read returned an empty chunk",
        ));
    }
    let remaining = output.len() - *filled;
    if chunk.data.len() > remaining {
        return Err(ClientError::invalid_response(
            "ReadBlock",
            format!(
                "worker read chunk exceeded requested block read: remaining {remaining}, got {}",
                chunk.data.len()
            ),
        ));
    }
    let end = *filled + chunk.data.len();
    output[*filled..end].copy_from_slice(&chunk.data);
    *filled = end;
    Ok(())
}

/// Accepts normal read completion only after the exact planned byte count.
pub(super) fn finish_read_block_output(filled: usize, expected_len: usize) -> ClientResult<()> {
    if filled != expected_len {
        return Err(ClientError::invalid_response(
            "ReadBlock",
            format!("worker read ended after {} bytes, expected {}", filled, expected_len),
        ));
    }
    Ok(())
}

pub(super) fn parse_worker_control_header(
    attempt: &AttemptContext,
    header: Option<&beryl_proto::worker::DataResponseHeaderProto>,
) -> ClientResult<()> {
    let Some(header) = header else {
        return Err(invalid_worker_header("worker response missing DataResponseHeader"));
    };
    let client = header
        .client
        .as_ref()
        .ok_or_else(|| invalid_worker_header("worker response invalid DataResponseHeader: missing client identity"))?;
    let client_id = beryl_proto::convert::required_client_id(client.client_id, "client_id")
        .map_err(|error| invalid_worker_header(format!("worker response invalid DataResponseHeader: {error}")))?;
    let call_id = beryl_proto::convert::require_call_id(&client.call_id, "call_id")
        .map_err(|error| invalid_worker_header(format!("worker response invalid DataResponseHeader: {error}")))?;
    let response_identity = HeaderIdentity {
        call_id,
        client_id,
        group_name: None,
    };
    let request_identity = attempt.header_identity();
    if response_identity.client_id != request_identity.client_id {
        return Err(invalid_worker_header(
            "worker response invalid DataResponseHeader: client_id mismatch",
        ));
    }
    if response_identity.call_id != request_identity.call_id {
        return Err(invalid_worker_header(
            "worker response invalid DataResponseHeader: call_id mismatch",
        ));
    }
    validate_data_header(Some(header))
}

/// Restores a structured Worker error from a marked gRPC status.
pub(super) fn parse_worker_data_status(attempt: &AttemptContext, status: tonic::Status) -> ClientError {
    match status.metadata().get(HEADER_WORKER_DATA_ERROR_DETAIL) {
        None => return ClientError::from(status),
        Some(value) => match value.to_str() {
            Ok(WORKER_DATA_ERROR_DETAIL_V1) => {}
            Ok(version) => {
                return invalid_worker_header(format!(
                    "worker status has unsupported structured error detail version: {version}"
                ));
            }
            Err(error) => {
                return invalid_worker_header(format!(
                    "worker status has invalid structured error detail version: {error}"
                ));
            }
        },
    }
    let header = match beryl_proto::worker::DataResponseHeaderProto::decode(status.details()) {
        Ok(header) => header,
        Err(error) => {
            return invalid_worker_header(format!("worker status has invalid structured error detail: {error}"));
        }
    };
    match parse_worker_control_header(attempt, Some(&header)) {
        Err(error) => error,
        Ok(()) => invalid_worker_header("worker non-OK status has no structured error"),
    }
}

pub(super) fn has_structured_worker_error(status: &tonic::Status) -> bool {
    status
        .metadata()
        .get(HEADER_WORKER_DATA_ERROR_DETAIL)
        .is_some_and(|value| value == WORKER_DATA_ERROR_DETAIL_V1)
}

pub(super) fn invalid_worker_header(message: impl Into<String>) -> ClientError {
    invalid_header_error(message)
}

pub(super) fn is_transient_worker_transport_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}

pub(super) fn build_tonic_request<T>(attempt: &AttemptContext, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(timeout) = attempt.timeout_remaining() {
        request.set_timeout(timeout.max(Duration::from_millis(1)));
    }
    request
}

pub(super) fn default_frame_size(len: u32) -> u32 {
    len.clamp(1, beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE as u32)
}

fn validate_worker_write_target(target: &WorkerWriteTarget) -> ClientResult<()> {
    if target.target.block_id.inode_id.as_raw() == 0 {
        return Err(ClientError::invalid_layout(
            "write target block_id inode_id must be non-zero".to_string(),
        ));
    }
    BlockShape::new(
        target.target.block_format_id,
        target.target.block_size,
        target.target.chunk_size,
        target.target.block_size,
    )
    .map_err(|error| ClientError::invalid_layout(format!("write target has invalid shape: {error}")))?;
    if target.target.worker_endpoints.is_empty() {
        return Err(ClientError::invalid_layout(
            "write target has no worker endpoints".to_string(),
        ));
    }
    if target.target.write_offset >= target.target.block_size {
        return Err(ClientError::invalid_layout(
            "write target offset exceeds capacity".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::build_write_block_data;

    #[test]
    fn write_block_rejects_empty_data_payload() {
        assert!(build_write_block_data(Bytes::new()).is_err());
    }
}
