// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public filesystem-facing facade.

use std::fmt;
use std::sync::Arc;

use super::{DeleteOptions, FileReader, FileStatus, FileWriter, ListStatusIterator, ListStatusOptions, MkdirOptions};
use crate::api::path::NamespacePathBuf;
use crate::client_inner::ClientInner;
use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult};

/// Public filesystem-facing client facade.
#[derive(Clone)]
pub struct FsClient {
    /// Shared client owner reused by this facade and the handles it opens.
    pub(crate) inner: Arc<ClientInner>,
}

impl FsClient {
    /// Creates a filesystem client after revalidating the sealed configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ClientErrorKind::InvalidConfiguration`](crate::ClientErrorKind::InvalidConfiguration)
    /// if any sealed value violates the runtime configuration invariants. It
    /// also returns an error if the client identity, root Metadata route, or
    /// transport ownership cannot be constructed.
    pub fn new(config: ClientConfig) -> ClientResult<Self> {
        Ok(Self {
            inner: Arc::new(ClientInner::from_config(config)?),
        })
    }

    /// Returns the immutable configuration used by this client.
    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }

    /// Returns Metadata-authorized status for a file or directory.
    pub async fn get_status(&self, path: &str) -> ClientResult<FileStatus> {
        let path = NamespacePathBuf::parse(path)?;
        self.inner.metadata.get_status(path).await
    }

    /// Lists the direct children of a directory using server-default paging.
    ///
    /// The first bounded page is fetched before this method returns, so path
    /// errors are reported immediately rather than during iteration.
    pub async fn list_status(&self, path: &str) -> ClientResult<ListStatusIterator> {
        self.list_status_with_options(path, ListStatusOptions::default()).await
    }

    /// Lists the direct children of a directory with explicit page sizing.
    ///
    /// Metadata retains no server-side iterator or snapshot between pages.
    /// Entries changed concurrently with iteration are therefore observed with
    /// weak consistency. Each page is bounded by `options.page_size`.
    pub async fn list_status_with_options(
        &self,
        path: &str,
        options: ListStatusOptions,
    ) -> ClientResult<ListStatusIterator> {
        if options.page_size == Some(0) {
            return Err(ClientError::invalid_argument(
                "list_status page_size must be greater than zero".to_string(),
            ));
        }
        let path = NamespacePathBuf::parse(path)?;
        let first_page = self
            .inner
            .metadata
            .list_status_page(path.clone(), None, options.page_size)
            .await?;
        Ok(ListStatusIterator::new(
            Arc::clone(&self.inner),
            path,
            options,
            first_page,
        ))
    }

    /// Ensures a directory exists, creating any missing parent directories.
    pub async fn mkdirs(&self, path: &str) -> ClientResult<FileStatus> {
        self.mkdirs_with_options(path, MkdirOptions::default()).await
    }

    /// Creates a directory using explicit parent-creation behavior.
    ///
    /// Disabling parent creation is a single namespace mutation. An ambiguous
    /// transport outcome is reported as unknown instead of being replayed.
    pub async fn mkdirs_with_options(&self, path: &str, options: MkdirOptions) -> ClientResult<FileStatus> {
        let path = NamespacePathBuf::parse(path)?;
        self.inner.metadata.mkdirs(path, options.create_parent).await
    }

    /// Delete a file or directory through the metadata client.
    ///
    /// Namespace visibility changes atomically at metadata. Physical block
    /// reclamation follows the configured metadata grace period asynchronously.
    pub async fn delete(&self, path: &str) -> ClientResult<()> {
        self.delete_with_options(path, DeleteOptions::default()).await
    }

    /// Deletes a namespace entry using explicit recursive behavior.
    ///
    /// The recursive operation remains one Metadata-authorized namespace
    /// mutation; this client does not discover or delete descendants itself.
    /// Ambiguous transport outcomes are reported as unknown and are not replayed.
    pub async fn delete_with_options(&self, path: &str, options: DeleteOptions) -> ClientResult<()> {
        let path = NamespacePathBuf::parse(path)?;
        self.inner.metadata.delete(path, options).await
    }

    /// Renames a namespace entry through Metadata.
    ///
    /// Ambiguous transport outcomes are reported as unknown and are not replayed.
    pub async fn rename(&self, src: &str, dst: &str) -> ClientResult<()> {
        let src = NamespacePathBuf::parse(src)?;
        let dst = NamespacePathBuf::parse(dst)?;
        self.inner.metadata.rename(src, dst).await
    }

    /// Opens an existing file for reads and returns a file reader.
    ///
    /// Existing files use the metadata-stored `FileLayout`; there are no
    /// public read-open options until they carry real behavior.
    pub async fn open(&self, path: &str) -> ClientResult<FileReader> {
        let path = NamespacePathBuf::parse(path)?;
        let file = self.inner.metadata.open_file(path).await?;
        Ok(FileReader::new(Arc::clone(&self.inner), file))
    }

    /// Atomically creates a file and obtains its initial write session.
    pub async fn create(&self, path: &str) -> ClientResult<FileWriter> {
        let path = NamespacePathBuf::parse(path)?;
        let response = self.inner.metadata.create_file(path).await?;
        Ok(FileWriter::new(Arc::clone(&self.inner), response))
    }

    /// Opens an append write session for an existing file.
    ///
    /// Append uses the metadata-stored `FileLayout` and does not send a new
    /// layout override.
    pub async fn append(&self, path: &str) -> ClientResult<FileWriter> {
        let path = NamespacePathBuf::parse(path)?;
        let response = self.inner.metadata.open_append(path).await?;
        Ok(FileWriter::new(Arc::clone(&self.inner), response))
    }
}

impl fmt::Debug for FsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsClient")
            .field("config", &self.inner.config)
            .field("metadata", &self.inner.metadata)
            .field("worker", &self.inner.worker)
            .finish_non_exhaustive()
    }
}
