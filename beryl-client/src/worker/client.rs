// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker operation orchestration above the block-local transport boundary.

use std::fmt;
use std::sync::Arc;

use super::transport::GrpcWorkerTransport;
use super::{BlockWrite, WorkerTransport, WorkerWriteTarget};
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::planner::PlannedBlockRead;
use crate::runtime::AttemptContext;
use beryl_types::{GroupName, LocatedBlock};

/// Owns file-level Worker orchestration while delegating each block-local IO
/// operation to a transport implementation.
#[derive(Clone)]
pub(crate) struct WorkerClient {
    transport: Arc<dyn WorkerTransport>,
}

impl WorkerClient {
    /// Takes ownership of the transport used for all block-local Worker IO.
    pub(crate) fn new(transport: Arc<dyn WorkerTransport>) -> Self {
        Self { transport }
    }

    /// Builds the production Worker client and its gRPC transport.
    pub(crate) fn from_config(config: &ClientConfig) -> Self {
        Self::new(Arc::new(GrpcWorkerTransport::from_config(config)))
    }

    /// Fills a caller-owned buffer from ordered Metadata-planned block ranges.
    pub(crate) async fn read_block_ranges_into(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        block_reads: &[PlannedBlockRead],
        output: &mut [u8],
    ) -> ClientResult<()> {
        let total_len = block_reads.iter().try_fold(0usize, |total, block_read| {
            total
                .checked_add(block_read.len as usize)
                .ok_or_else(|| ClientError::invalid_layout("planned read length overflow".to_string()))
        })?;
        if total_len != output.len() {
            return Err(ClientError::invalid_layout(format!(
                "planned read length {total_len} does not match output length {}",
                output.len()
            )));
        }
        let mut remaining = output;
        for block_read in block_reads {
            let expected_end = block_read
                .file_offset
                .checked_add(u64::from(block_read.len))
                .ok_or_else(|| ClientError::invalid_layout("planned block read end overflow".to_string()))?;
            if expected_end != block_read.end_file_offset {
                return Err(ClientError::invalid_layout(
                    "planned block read coverage is inconsistent".to_string(),
                ));
            }
            let (block_output, tail) = remaining.split_at_mut(block_read.len as usize);
            self.transport
                .read_block_range(attempt.clone(), group_name.clone(), block_read, block_output)
                .await?;
            remaining = tail;
        }
        Ok(())
    }

    /// Opens one Metadata-authorized block RPC and returns only after the
    /// transport has crossed Worker's block-open acknowledgement boundary.
    pub(crate) async fn open_write_block(
        &self,
        attempt: AttemptContext,
        group_name: GroupName,
        target: LocatedBlock,
        lease_expires_at_ms: u64,
    ) -> ClientResult<BlockWrite> {
        self.transport
            .open_write_block(attempt, WorkerWriteTarget { group_name, target }, lease_expires_at_ms)
            .await
    }
}

impl fmt::Debug for WorkerClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("WorkerClient").finish_non_exhaustive()
    }
}
