// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public namespace status values.

use beryl_types::FileType;

/// Metadata-authorized status for one namespace entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStatus {
    path: String,
    /// Namespace entry kind.
    pub kind: FileType,
    /// Visible byte length; directories report zero.
    pub len: u64,
    /// Creation time in milliseconds since Unix epoch.
    pub create_time: u64,
    /// Last content or direct-directory-member change, in Unix milliseconds.
    pub modify_time: u64,
}

impl FileStatus {
    /// Creates a status from a validated namespace path and Metadata response.
    pub(crate) fn new(path: impl Into<String>, kind: FileType, len: u64, create_time: u64, modify_time: u64) -> Self {
        Self {
            path: path.into(),
            kind,
            len,
            create_time,
            modify_time,
        }
    }

    /// Returns the full namespace path represented by this status.
    pub fn path(&self) -> &str {
        &self.path
    }
}
