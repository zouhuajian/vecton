// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! High-level API modules.

pub(crate) mod client;
mod list_status;
pub(crate) mod options;
pub(crate) mod path;
mod reader;
mod status;
mod writer;

pub use beryl_types::FileType;
pub use client::FsClient;
pub use list_status::ListStatusIterator;
pub use options::{DeleteOptions, ListStatusOptions, MkdirOptions};
pub use reader::FileReader;
pub use status::FileStatus;
pub use writer::FileWriter;
