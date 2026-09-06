// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata-owned inode state and exact file-commit completion evidence.

use crate::error::{MetadataError, MetadataResult};
use beryl_types::{
    BlockId, CallId, ClientId, CommittedBlock, ContentGeneration, FileType, InodeId, LeaseEpoch, MountId,
};
use serde::{Deserialize, Serialize};

/// File publication precondition and merge behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PublishMode {
    /// Replace content only while the expected content generation is current.
    ReplaceIfUnchanged,
    /// Append content only while the expected content generation is current.
    AppendIfUnchanged,
}

/// Frozen business payload shared by publication validation and commit replay.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FilePublication {
    pub(crate) blocks: Vec<CommittedBlock>,
    pub(crate) target_size: u64,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) mode: PublishMode,
}

/// Latest completed CommitFile, stored inside the inode's existing RocksDB value.
///
/// The visible layout supplies the block payload for exact replay verification.
/// Content mutation retires this evidence; a later commit replaces it. Lease
/// changes alone preserve it so response loss can be resolved after a new open.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileCommit {
    pub(crate) client_id: ClientId,
    pub(crate) call_id: CallId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) mode: PublishMode,
    pub(crate) committed_size: u64,
    pub(crate) generation: ContentGeneration,
}

impl FilePublication {
    /// Locate the changed tail and new blocks from the frozen pre-publication length.
    /// An empty append payload means an exact no-op, including a partial tail.
    pub(crate) fn start_index(&self, file: &FileData) -> MetadataResult<usize> {
        let count = FileData::block_count(self.target_size, file.layout.block_size)?;
        let start = match self.mode {
            PublishMode::ReplaceIfUnchanged => 0,
            PublishMode::AppendIfUnchanged => {
                if self.target_size < self.expected_file_size {
                    return Err(MetadataError::InvalidArgument(
                        "append cannot shrink visible content".into(),
                    ));
                }
                if self.blocks.is_empty() && self.target_size == self.expected_file_size {
                    count
                } else {
                    usize::try_from(self.expected_file_size / u64::from(file.layout.block_size))
                        .map_err(|_| MetadataError::InvalidArgument("block ordinal overflows".into()))?
                }
            }
        };
        if start > count || self.blocks.len() != count - start {
            return Err(MetadataError::InvalidArgument(
                "publication does not cover its target length".into(),
            ));
        }
        for (offset, block) in self.blocks.iter().enumerate() {
            let ordinal = start + offset;
            let expected_len = Self::visible_block_len(self.target_size, file.layout.block_size, ordinal);
            if block.len != expected_len {
                return Err(MetadataError::InvalidArgument(
                    "only the final block may be partial".into(),
                ));
            }
        }
        Ok(start)
    }

    fn visible_block_len(len: u64, capacity: u32, ordinal: usize) -> u64 {
        (len - ordinal as u64 * u64::from(capacity)).min(u64::from(capacity))
    }

    /// Compare a frozen publication with the current visible suffix, never with
    /// a newly captured expected generation or length.
    pub(crate) fn matches_visible(&self, file: &FileData) -> MetadataResult<bool> {
        let start = self.start_index(file)?;
        Ok(file.len == self.target_size
            && file.blocks.get(start..).is_some_and(|visible| {
                visible
                    .iter()
                    .copied()
                    .eq(self.blocks.iter().map(|block| block.block_id))
            }))
    }

    /// Build the complete fixed-block layout while preserving every published byte.
    /// The caller separately checks generation and writer authority at Raft apply.
    pub(crate) fn merged_blocks(&self, inode_id: InodeId, file: &FileData) -> MetadataResult<Vec<BlockId>> {
        file.validate(inode_id)?;
        let start = self.start_index(file)?;
        let mut blocks = match self.mode {
            PublishMode::ReplaceIfUnchanged => Vec::new(),
            PublishMode::AppendIfUnchanged => {
                if file.len != self.expected_file_size || start > file.blocks.len() {
                    return Err(MetadataError::Again(
                        "append base no longer matches visible content".into(),
                    ));
                }
                if start < file.blocks.len()
                    && self.blocks.first().map(|block| block.block_id) != file.blocks.get(start).copied()
                {
                    return Err(MetadataError::InvalidArgument(
                        "append must reuse the existing partial tail".into(),
                    ));
                }
                file.blocks[..start].to_vec()
            }
        };
        let mut seen: std::collections::HashSet<_> = blocks.iter().copied().collect();
        for block in &self.blocks {
            if block.block_id.inode_id != inode_id
                || u64::from(block.block_id.index.as_raw()) >= file.next_index
                || !seen.insert(block.block_id)
            {
                return Err(MetadataError::InvalidArgument(
                    "publication contains a duplicate or unallocated block".into(),
                ));
            }
            if self.mode == PublishMode::ReplaceIfUnchanged && file.blocks.contains(&block.block_id) {
                return Err(MetadataError::InvalidArgument(
                    "replacement requires new block identities".into(),
                ));
            }
            blocks.push(block.block_id);
        }
        Ok(blocks)
    }

    /// Confirm this exact Commit operation from one atomic inode read.
    /// Missing or superseded evidence never proves that an older call failed.
    pub(crate) fn resolve_commit(
        &self,
        inode: &Inode,
        client_id: ClientId,
        call_id: CallId,
    ) -> MetadataResult<Option<ContentGeneration>> {
        let file = inode.file()?;
        let Some(commit) = &file.last_commit else {
            return Ok(None);
        };
        if commit.client_id != client_id || commit.call_id != call_id {
            return Ok(None);
        }
        if commit.generation != file.generation
            || commit.committed_size != file.len
            || commit
                .lease_epoch
                .checked_next()
                .is_none_or(|ended| file.lease_epoch < ended)
        {
            return Err(MetadataError::Internal(
                "CommitFile evidence disagrees with its inode".into(),
            ));
        }
        if commit.lease_epoch != self.lease_epoch
            || commit.expected_generation != self.expected_generation
            || commit.expected_file_size != self.expected_file_size
            || commit.mode != self.mode
            || commit.committed_size != self.target_size
            || !self.matches_visible(file)?
        {
            return Err(MetadataError::InvalidArgument(
                "CommitFile payload changed for a completed operation".into(),
            ));
        }
        Ok(Some(commit.generation))
    }
}

/// Common inode times in milliseconds since Unix epoch.
/// Creation time is immutable; modification time tracks content or direct members.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InodeAttrs {
    pub(crate) create_time: u64,
    pub(crate) modify_time: u64,
}

impl InodeAttrs {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Initialize both timestamps when the namespace object is created.
    pub(crate) fn initialize(&mut self, now_ms: u64) {
        self.create_time = now_ms;
        self.modify_time = now_ms;
    }

    /// Keep modification time monotonic even if the proposal clock moves backwards.
    pub(crate) fn set_modify_time(&mut self, now_ms: u64) {
        self.modify_time = self.modify_time.max(now_ms);
    }
}

/// File-specific durable authority. The allocation cursor is independent of
/// visible length and is never rolled back when an unpublished block is lost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileData {
    pub(crate) layout: beryl_types::FileLayout,
    pub(crate) len: u64,
    pub(crate) generation: ContentGeneration,
    pub(crate) blocks: Vec<BlockId>,
    /// Never reused, including indexes allocated to aborted writes.
    pub(crate) next_index: u64,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) last_commit: Option<FileCommit>,
}

impl FileData {
    /// Derive the bounded inline block count without overflowing len + capacity.
    pub(crate) fn block_count(len: u64, capacity: u32) -> MetadataResult<usize> {
        if capacity == 0 {
            return Err(MetadataError::InvalidArgument("zero block capacity".into()));
        }
        let count = if len == 0 {
            0
        } else {
            (len - 1) / u64::from(capacity) + 1
        };
        if count > beryl_types::MAX_FILE_BLOCKS as u64 {
            return Err(MetadataError::ResourceExhausted(
                "file exceeds inline block limit".into(),
            ));
        }
        Ok(count as usize)
    }

    /// Validate persisted shape and identities before interpreting logical offsets.
    pub(crate) fn validate(&self, inode_id: InodeId) -> MetadataResult<()> {
        self.layout
            .validate()
            .map_err(|error| MetadataError::Internal(format!("invalid file layout: {error}")))?;
        if self.blocks.len() != Self::block_count(self.len, self.layout.block_size)? {
            return Err(MetadataError::Internal(
                "file length disagrees with its block count".into(),
            ));
        }
        let mut seen = std::collections::HashSet::with_capacity(self.blocks.len());
        if self.blocks.iter().any(|block| {
            block.inode_id != inode_id || u64::from(block.index.as_raw()) >= self.next_index || !seen.insert(*block)
        }) {
            return Err(MetadataError::Internal("file contains invalid block identities".into()));
        }
        Ok(())
    }

    /// Return the visible prefix length of an ordinal in this validated layout.
    pub(crate) fn block_len(&self, ordinal: usize) -> u64 {
        FilePublication::visible_block_len(self.len, self.layout.block_size, ordinal)
    }
}

/// Namespace object type and its only variant-specific payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InodeKind {
    File(FileData),
    Dir,
}

/// Durable namespace identity, common attributes, and object payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Inode {
    pub(crate) inode_id: InodeId,
    pub(crate) attrs: InodeAttrs,
    pub(crate) kind: InodeKind,
    /// Children inherit this namespace routing anchor from their parent.
    pub(crate) mount_id: MountId,
}

impl Inode {
    /// Visible file length; directory status has no file content.
    pub(crate) fn len(&self) -> u64 {
        match &self.kind {
            InodeKind::File(file) => file.len,
            _ => 0,
        }
    }

    /// Borrow file authority, rejecting namespace objects without file contents.
    pub(crate) fn file(&self) -> MetadataResult<&FileData> {
        match &self.kind {
            InodeKind::File(file) => Ok(file),
            _ => Err(MetadataError::IsDir(format!("inode {} is not a file", self.inode_id))),
        }
    }

    /// Mutate file authority inside the ordered Metadata mutation path.
    pub(crate) fn file_mut(&mut self) -> MetadataResult<&mut FileData> {
        match &mut self.kind {
            InodeKind::File(file) => Ok(file),
            _ => Err(MetadataError::IsDir(format!("inode {} is not a file", self.inode_id))),
        }
    }

    /// Return the public type tag without persisting a duplicate discriminator.
    pub(crate) fn file_type(&self) -> FileType {
        match self.kind {
            InodeKind::File(_) => FileType::File,
            InodeKind::Dir => FileType::Dir,
        }
    }

    /// Create an empty file with its immutable, Metadata-selected layout.
    pub(crate) fn new_file(
        inode_id: InodeId,
        attrs: InodeAttrs,
        mount_id: MountId,
        layout: beryl_types::FileLayout,
    ) -> Self {
        Self {
            inode_id,
            attrs,
            mount_id,
            kind: InodeKind::File(FileData {
                layout,
                len: 0,
                blocks: Vec::new(),
                generation: ContentGeneration::default(),
                lease_epoch: LeaseEpoch::default(),
                next_index: 0,
                last_commit: None,
            }),
        }
    }

    /// Create a directory whose members live in the namespace indexes.
    pub(crate) fn new_dir(inode_id: InodeId, attrs: InodeAttrs, mount_id: MountId) -> Self {
        Self {
            inode_id,
            attrs,
            mount_id,
            kind: InodeKind::Dir,
        }
    }
}
