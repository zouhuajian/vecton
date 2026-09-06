// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Group state watermark for metadata freshness.
//!
//! This module defines types for tracking state-machine applied progress per
//! metadata Raft owner group.

use crate::GroupName;
use crate::RaftLogId;
use serde::{Deserialize, Serialize};

/// Group state watermark for a specific metadata Raft owner group.
///
/// `state_id` is the state-machine applied RaftLogId for `group_name`. It is not
/// an append index, committed index, private apply counter, route epoch,
/// mount epoch, worker process-run identity, or writer lease epoch.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupStateWatermark {
    /// Metadata Raft owner group this watermark applies to.
    pub group_name: GroupName,
    /// Applied state-machine RaftLogId that must be reached.
    pub state_id: RaftLogId,
}

impl GroupStateWatermark {
    /// Create a new GroupStateWatermark.
    pub fn new(group_name: GroupName, state_id: RaftLogId) -> Self {
        Self { group_name, state_id }
    }

    /// Check if this watermark has been reached by the given applied state ID.
    pub fn is_reached(&self, applied_state_id: &RaftLogId) -> bool {
        applied_state_id.has_reached(&self.state_id)
    }

    /// Compare two watermarks from the same group.
    /// Returns Some(Ordering) if they are from the same group, None otherwise.
    pub fn cmp_same_group(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.group_name != other.group_name {
            return None;
        }
        Some(self.state_id.cmp(&other.state_id))
    }
}

/// Mount epoch (or config version) for route consistency checking.
///
/// This ensures destructive operations are only allowed when the mount/routing
/// configuration matches the expected epoch. Mismatch indicates route change
/// and requires refresh.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MountEpoch(pub u64);

impl MountEpoch {
    /// Create a new MountEpoch.
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    /// Get the epoch value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}
