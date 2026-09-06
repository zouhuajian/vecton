// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable block prefixes. Metadata owns visibility; this store owns fsynced bytes.

use super::meta_codec::{decode_meta_payload, encode_meta_payload};
use crate::error::WorkerError;
use beryl_types::layout::{BlockFormatId, BlockShape};
use beryl_types::{BlockId, FencingToken, GroupName, Tier};
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub type StoreResult<T> = Result<T, WorkerError>;
const BLOCK_META_MAGIC: [u8; 4] = *b"BRYL";
const BLOCK_META_HEADER_LEN: usize = 20;
const BLOCK_META_VERSION: u32 = 2;
const MAX_META_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

/// Fixed little-endian header for a block metadata file.
/// The header identifies the format and bounds the serialized payload.
/// Metadata bytes are not checksummed; correctness relies on atomic
/// replacement, strict decoding, and semantic validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMetaHeader {
    /// Fixed file magic used to identify Beryl block metadata.
    pub magic: [u8; 4],
    /// Version of this fixed header and serialized payload layout.
    pub version: u32,
    /// Fixed header length in bytes.
    pub header_len: u32,
    /// Serialized payload length in bytes.
    pub payload_len: u64,
}

impl BlockMetaHeader {
    pub const fn encoded_len() -> usize {
        BLOCK_META_HEADER_LEN
    }

    fn for_payload(payload_len: usize) -> StoreResult<Self> {
        let payload_len =
            u64::try_from(payload_len).map_err(|_| invalid_argument("meta payload length does not fit in u64"))?;
        let header = Self {
            magic: BLOCK_META_MAGIC,
            version: BLOCK_META_VERSION,
            header_len: BLOCK_META_HEADER_LEN as u32,
            payload_len,
        };
        header.validate()?;
        Ok(header)
    }

    fn decode(encoded: &[u8]) -> StoreResult<Self> {
        if encoded.len() != BLOCK_META_HEADER_LEN {
            return Err(corrupt("invalid meta header length"));
        }

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&encoded[0..4]);

        Ok(Self {
            magic,
            version: u32::from_le_bytes(encoded[4..8].try_into().expect("fixed header slice")),
            header_len: u32::from_le_bytes(encoded[8..12].try_into().expect("fixed header slice")),
            payload_len: u64::from_le_bytes(encoded[12..20].try_into().expect("fixed header slice")),
        })
    }

    fn encode(self) -> [u8; BLOCK_META_HEADER_LEN] {
        let mut encoded = [0u8; BLOCK_META_HEADER_LEN];
        encoded[0..4].copy_from_slice(&self.magic);
        encoded[4..8].copy_from_slice(&self.version.to_le_bytes());
        encoded[8..12].copy_from_slice(&self.header_len.to_le_bytes());
        encoded[12..20].copy_from_slice(&self.payload_len.to_le_bytes());
        encoded
    }

    fn validate(self) -> StoreResult<()> {
        if self.magic != BLOCK_META_MAGIC {
            return Err(corrupt("invalid block meta magic"));
        }
        if self.version != BLOCK_META_VERSION {
            return Err(corrupt("unsupported block meta version"));
        }
        if self.header_len != BLOCK_META_HEADER_LEN as u32 {
            return Err(corrupt("unsupported block meta header length"));
        }
        if self.payload_len == 0 {
            return Err(corrupt("block meta payload length must be non-zero"));
        }
        if self.payload_len > MAX_META_PAYLOAD_LEN as u64 {
            return Err(corrupt("block meta payload length exceeds limit"));
        }
        Ok(())
    }
}

/// Atomic local checkpoint, including writer fencing and the recoverable prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMetaPayload {
    pub identity: BlockIdentity,
    pub format: BlockFormat,
    pub source: BlockSource,
    pub visibility: BlockVisibility,
    pub tier: Tier,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockIdentity {
    pub block_id: BlockId,
    pub group_name: GroupName,
}

/// Persisted interpretation of the block, independent of runtime configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockFormat {
    pub format_id: BlockFormatId,
    pub block_size: u64,
    pub chunk_size: u64,
    pub checksum_kind: ChecksumKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChecksumKind {
    None,
}

/// Bytes covered by a completed local checkpoint, possibly ahead of Metadata visibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSource {
    pub durable_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockVisibility {
    pub block_state: BlockState,
    pub fencing_token: FencingToken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockState {
    Ready,
    Corrupt,
    Deleting,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullBlockFileStoreConfig {
    pub data_root: PathBuf,
}
impl FullBlockFileStoreConfig {
    pub fn new(data_root: PathBuf) -> Self {
        Self { data_root }
    }
}

/// Online Metadata authorization for opening a block at an exact checkpoint.
/// The caller must drain every prior writer before invoking this operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenBlockWriteRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub block_size: u64,
    pub block_format_id: BlockFormatId,
    pub chunk_size: u32,
    pub checksum_kind: ChecksumKind,
    pub tier: Tier,
    pub fencing_token: FencingToken,
    pub write_offset: u64,
    /// Current visible prefix returned by Metadata, never supplied by the client.
    pub visible_len: u64,
}

/// Checkpoints the complete prefix written by one short WriteBlock stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointBlockRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub effective_len: u64,
    pub fencing_token: FencingToken,
}

/// Exact never-reused identity authorized by Metadata for physical reclamation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReclaimBlockRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimBlockState {
    Ready,
    Deleting,
    Absent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimBlockResult {
    Deleted { effective_len: u64 },
    AlreadyAbsent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockPaths {
    pub data_path: PathBuf,
    pub meta_path: PathBuf,
    pub temp_meta_path: PathBuf,
}

/// Filesystem implementation of the durable-prefix and deletion commit points.
#[derive(Clone, Debug)]
pub struct FullBlockFileStore {
    config: FullBlockFileStoreConfig,
    // Readers and reports must not combine an old D with a newly truncated data file.
    // Clones share this directory-local gate; data frames may still append concurrently.
    checkpoint_access: Arc<RwLock<()>>,
}

impl FullBlockFileStore {
    /// Uses a prepared, durably created store root; StoreDirs owns its initialization.
    pub fn new(config: FullBlockFileStoreConfig) -> Self {
        Self {
            config,
            checkpoint_access: Arc::new(RwLock::new(())),
        }
    }

    /// Creates a zero checkpoint or reopens the exact authorized durable prefix.
    /// New epochs persist the reduced boundary before truncating uncommitted bytes.
    pub fn open_block_write(&self, req: OpenBlockWriteRequest) -> StoreResult<BlockMetaPayload> {
        let _checkpoint = self.checkpoint_access.write().expect("checkpoint access poisoned");
        BlockShape::new(req.block_format_id, req.block_size, req.chunk_size, req.block_size)
            .map_err(|e| invalid_argument(e.to_string()))?;
        validate_token(req.fencing_token, req.block_id)?;
        if req.write_offset >= req.block_size || req.visible_len > req.write_offset {
            return Err(invalid_argument("invalid authorized write prefix"));
        }
        let paths = self.paths(&req.group_name, req.block_id);
        create_dir_durable(&self.config.data_root, paths.parent_dir()?)?;
        let format = BlockFormat {
            format_id: req.block_format_id,
            block_size: req.block_size,
            chunk_size: u64::from(req.chunk_size),
            checksum_kind: req.checksum_kind,
        };
        let mut meta = match self.load_meta(&req.group_name, req.block_id) {
            Ok(mut meta) => {
                ensure_readable(&meta)?;
                if meta.format != format || meta.tier != req.tier {
                    return Err(corrupt("write authorization changed persisted block layout or tier"));
                }
                let previous = meta.visibility.fencing_token;
                if req.fencing_token.epoch < previous.epoch
                    || (req.fencing_token.epoch == previous.epoch && req.fencing_token != previous)
                {
                    return Err(fenced("write token is older than the persisted writer"));
                }
                if req.visible_len > meta.source.durable_len {
                    return Err(corrupt("visible prefix exceeds durable local bytes"));
                }
                validate_data_prefix(&paths, &meta)?;
                if req.fencing_token.epoch > previous.epoch {
                    if req.write_offset != req.visible_len {
                        return Err(invalid_argument("new writer must start at the visible prefix"));
                    }
                    meta.visibility.fencing_token = req.fencing_token;
                    meta.source.durable_len = req.visible_len;
                    write_meta(&paths, &meta)?;
                } else if req.write_offset != meta.source.durable_len {
                    return Err(invalid_argument("same writer must resume at its durable checkpoint"));
                }
                meta
            }
            Err(WorkerError::NotFound(_)) => {
                if req.write_offset != 0 || req.visible_len != 0 || paths.data_path.exists() {
                    return Err(corrupt("nonempty authorized prefix has no local metadata"));
                }
                let meta = BlockMetaPayload {
                    identity: BlockIdentity {
                        block_id: req.block_id,
                        group_name: req.group_name.clone(),
                    },
                    format,
                    source: BlockSource { durable_len: 0 },
                    visibility: BlockVisibility {
                        block_state: BlockState::Ready,
                        fencing_token: req.fencing_token,
                    },
                    tier: req.tier,
                };
                // The durable zero checkpoint identifies an interrupted data-file creation.
                write_meta(&paths, &meta)?;
                meta
            }
            Err(e) => return Err(e),
        };
        restore_prefix(&paths, &mut meta)?;
        Ok(meta)
    }

    pub fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let meta = read_meta_file(&self.paths(group_name, block_id).meta_path)?;
        validate_meta(&meta, group_name, block_id)?;
        Ok(meta)
    }

    /// Confirms the current metadata directory entry before reporting its durable prefix.
    /// A failed checkpoint rename may be visible in memory without a completed directory fsync.
    pub fn load_report_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let _checkpoint = self.checkpoint_access.read().expect("checkpoint access poisoned");
        let paths = self.paths(group_name, block_id);
        let meta = read_report_meta(&paths.meta_path)?;
        validate_meta(&meta, group_name, block_id)?;
        Ok(meta)
    }

    /// Appends under exclusive stream ownership; the checkpointed prefix is immutable.
    pub fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        let meta = self.load_meta(group_name, block_id)?;
        ensure_readable(&meta)?;
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| invalid_argument("write range overflow"))?;
        if data.is_empty() || offset < meta.source.durable_len || end > meta.format.block_size {
            return Err(invalid_argument("write crosses the durable prefix or block capacity"));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.paths(group_name, block_id).data_path)?;
        if offset != file.metadata()?.len() {
            return Err(invalid_argument("write is not at the physical append cursor"));
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&data)?;
        Ok(())
    }

    /// Orders data fsync before atomic metadata replacement and directory fsync.
    /// Any failure after IO begins has an unknown outcome and must not be acknowledged.
    pub fn checkpoint_block(&self, req: CheckpointBlockRequest) -> StoreResult<BlockMetaPayload> {
        let _checkpoint = self.checkpoint_access.write().expect("checkpoint access poisoned");
        let paths = self.paths(&req.group_name, req.block_id);
        let mut meta = self.load_meta(&req.group_name, req.block_id)?;
        ensure_readable(&meta)?;
        if meta.visibility.fencing_token != req.fencing_token {
            return Err(fenced("checkpoint writer was fenced"));
        }
        if req.effective_len < meta.source.durable_len
            || req.effective_len > meta.format.block_size
            || req.effective_len == 0
        {
            return Err(invalid_argument("invalid checkpoint length"));
        }
        let data = OpenOptions::new().read(true).write(true).open(&paths.data_path)?;
        if data.metadata()?.len() != req.effective_len {
            return Err(corrupt("checkpoint length differs from written bytes"));
        }
        data.sync_all()?;
        meta.source.durable_len = req.effective_len;
        write_meta(&paths, &meta)?;
        Ok(meta)
    }

    /// Reads within the local durable prefix; the data service separately enforces Metadata's visible range.
    pub fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        let _checkpoint = self.checkpoint_access.read().expect("checkpoint access poisoned");
        let meta = self.load_meta(group_name, block_id)?;
        ensure_readable(&meta)?;
        if offset.checked_add(len).is_none_or(|end| end > meta.source.durable_len) {
            return Err(invalid_argument("read exceeds durable prefix"));
        }
        let paths = self.paths(group_name, block_id);
        validate_data_prefix(&paths, &meta)?;
        let mut file = File::open(paths.data_path)?;
        file.seek(SeekFrom::Start(offset))?;
        let len = usize::try_from(len).map_err(|_| invalid_argument("read length overflow"))?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes)
            .map_err(|e| map_truncated_read_error(e, "durable read is truncated"))?;
        Ok(Bytes::from(bytes))
    }

    /// Reports checkpoints without changing physical files while streams are active.
    pub fn scan_group_blocks(&self, group_name: &GroupName) -> StoreResult<Vec<BlockMetaPayload>> {
        let _checkpoint = self.checkpoint_access.read().expect("checkpoint access poisoned");
        let mut result = Vec::new();
        for path in block_files(&self.group_dir(group_name).join("blocks"))? {
            if path.extension().and_then(|x| x.to_str()) != Some("meta") {
                continue;
            }
            let meta = read_report_meta(&path)?;
            let paths = self.paths(group_name, meta.identity.block_id);
            validate_meta(&meta, group_name, meta.identity.block_id)?;
            if paths.meta_path != path {
                return Err(corrupt("block identity differs from metadata path"));
            }
            if meta.visibility.block_state == BlockState::Ready {
                validate_data_prefix(&paths, &meta)?;
                result.push(meta);
            }
        }
        result.sort_by_key(|m| {
            (
                m.identity.block_id.inode_id.as_raw(),
                m.identity.block_id.index.as_raw(),
            )
        });
        Ok(result)
    }

    pub fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
        match self.load_meta(&req.group_name, req.block_id) {
            Ok(meta) if meta.visibility.block_state == BlockState::Deleting => Ok(ReclaimBlockState::Deleting),
            Ok(_) => Ok(ReclaimBlockState::Ready),
            Err(WorkerError::NotFound(_)) => {
                let paths = self.paths(&req.group_name, req.block_id);
                if paths.data_path.exists() || paths.temp_meta_path.exists() {
                    return Err(corrupt("unidentified block artifacts"));
                }
                Ok(ReclaimBlockState::Absent)
            }
            Err(e) => Err(e),
        }
    }

    /// Persists Deleting before any unlink. The caller retains exclusive block access through completion.
    pub fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
        let _checkpoint = self.checkpoint_access.write().expect("checkpoint access poisoned");
        if self.inspect_reclaim_block(req)? == ReclaimBlockState::Absent {
            return Ok(ReclaimBlockResult::AlreadyAbsent);
        }
        let paths = self.paths(&req.group_name, req.block_id);
        let mut meta = self.load_meta(&req.group_name, req.block_id)?;
        meta.visibility.block_state = BlockState::Deleting;
        write_meta(&paths, &meta)?;
        complete_deletion(&paths)?;
        Ok(ReclaimBlockResult::Deleted {
            effective_len: meta.source.durable_len,
        })
    }

    /// Restores every exact checkpoint before admission or reporting, and completes interrupted deletions.
    /// Unidentified files and short durable prefixes fail startup closed.
    pub fn recover_blocks(&self) -> StoreResult<usize> {
        let _checkpoint = self.checkpoint_access.write().expect("checkpoint access poisoned");
        let groups = self.config.data_root.join("groups");
        if !groups.exists() {
            return Ok(0);
        }
        let mut recovered = 0;
        for group in fs::read_dir(groups)? {
            let group = group?;
            if !group.file_type()?.is_dir() {
                return Err(corrupt("unexpected entry in groups directory"));
            }
            let group_name = GroupName::parse(
                group
                    .file_name()
                    .to_str()
                    .ok_or_else(|| corrupt("invalid group path"))?,
            )
            .map_err(|e| corrupt(e.to_string()))?;
            let files = block_files(&group.path().join("blocks"))?;
            // A temp metadata file is never a committed checkpoint.
            for path in files
                .iter()
                .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("tmp"))
            {
                remove_file_if_exists(path)?;
                sync_parent_dir(path.parent().expect("block path parent"))?;
            }
            for path in files
                .iter()
                .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("meta"))
            {
                let mut meta = read_meta_file(path)?;
                let paths = self.paths(&group_name, meta.identity.block_id);
                validate_meta(&meta, &group_name, meta.identity.block_id)?;
                if paths.meta_path != *path {
                    return Err(corrupt("block identity differs from metadata path"));
                }
                if meta.visibility.block_state == BlockState::Deleting {
                    complete_deletion(&paths)?;
                } else {
                    restore_prefix(&paths, &mut meta)?;
                }
                recovered += 1;
            }
            for path in block_files(&group.path().join("blocks"))? {
                match path.extension().and_then(|s| s.to_str()) {
                    Some("meta") => {}
                    Some("blk") if path.with_extension("meta").exists() => {}
                    _ => return Err(corrupt(format!("unidentified block file: {}", path.display()))),
                }
            }
        }
        Ok(recovered)
    }

    /// Discards only the unsynced suffix after a cancelled stream has no remaining IO.
    pub fn discard_unsynced_suffix(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
        let mut meta = match self.load_meta(group_name, block_id) {
            Ok(meta) => meta,
            Err(WorkerError::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        ensure_readable(&meta)?;
        restore_prefix(&self.paths(group_name, block_id), &mut meta)
    }
    pub fn paths(&self, group_name: &GroupName, block_id: BlockId) -> BlockPaths {
        let (hash_a, hash_b) = block_hash_prefix(block_id);
        let stem = format!("b_{:016x}_{:08x}", block_id.inode_id.as_raw(), block_id.index.as_raw());
        let dir = self
            .group_dir(group_name)
            .join("blocks")
            .join(format!("{hash_a:02x}"))
            .join(format!("{hash_b:02x}"));

        BlockPaths {
            data_path: dir.join(format!("{stem}.blk")),
            meta_path: dir.join(format!("{stem}.meta")),
            temp_meta_path: dir.join(format!("{stem}.meta.tmp")),
        }
    }

    fn group_dir(&self, group_name: &GroupName) -> PathBuf {
        self.config.data_root.join("groups").join(group_name.as_str())
    }
}

impl BlockPaths {
    fn parent_dir(&self) -> StoreResult<&Path> {
        self.data_path
            .parent()
            .ok_or_else(|| invalid_argument("block has no parent"))
    }
}

/// IO boundary for the ordered block lifecycle. Callers serialize writers and pin all IO against reclaim.
pub trait LocalBlockStore {
    fn open_block_write(&self, req: OpenBlockWriteRequest) -> StoreResult<BlockMetaPayload>;

    fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()>;

    fn checkpoint_block(&self, req: CheckpointBlockRequest) -> StoreResult<BlockMetaPayload>;

    fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes>;

    fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload>;

    fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState>;

    fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult>;

    fn discard_unsynced_suffix(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()>;
}

impl LocalBlockStore for FullBlockFileStore {
    fn open_block_write(&self, req: OpenBlockWriteRequest) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::open_block_write(self, req)
    }

    fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        FullBlockFileStore::write_at(self, group_name, block_id, offset, data)
    }

    fn checkpoint_block(&self, req: CheckpointBlockRequest) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::checkpoint_block(self, req)
    }

    fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        FullBlockFileStore::read_at(self, group_name, block_id, offset, len)
    }

    fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::load_meta(self, group_name, block_id)
    }

    fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
        FullBlockFileStore::inspect_reclaim_block(self, req)
    }

    fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
        FullBlockFileStore::reclaim_block(self, req)
    }

    fn discard_unsynced_suffix(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
        FullBlockFileStore::discard_unsynced_suffix(self, group_name, block_id)
    }
}

fn encode_meta(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    let payload = encode_meta_payload(meta)?;
    let header = BlockMetaHeader::for_payload(payload.len())?;
    let mut encoded = Vec::with_capacity(BlockMetaHeader::encoded_len() + payload.len());
    encoded.extend_from_slice(&header.encode());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn read_meta_file(path: &Path) -> StoreResult<BlockMetaPayload> {
    let payload = read_meta_payload(path)?;
    decode_meta_payload(&payload)
}

fn read_meta_payload(path: &Path) -> StoreResult<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut encoded_header = [0u8; BLOCK_META_HEADER_LEN];
    file.read_exact(&mut encoded_header)
        .map_err(|err| map_truncated_read_error(err, "block meta file is shorter than the header"))?;

    let header = BlockMetaHeader::decode(&encoded_header)?;
    header.validate()?;
    let payload_len = usize::try_from(header.payload_len).map_err(|_| corrupt("meta payload length is too large"))?;
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)
        .map_err(|err| map_truncated_read_error(err, "block meta payload is shorter than declared length"))?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(corrupt("block meta file has trailing bytes"));
    }
    Ok(payload)
}

fn block_hash_prefix(block_id: BlockId) -> (u8, u8) {
    let mut value = block_id.inode_id.as_raw() ^ (u64::from(block_id.index.as_raw()) << 32);
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^= value >> 33;
    ((value >> 56) as u8, (value >> 48) as u8)
}

/// Restores P to D only after writer IO is drained; D can be zero for an interrupted creation.
fn restore_prefix(paths: &BlockPaths, meta: &mut BlockMetaPayload) -> StoreResult<()> {
    ensure_readable(meta)?;
    // Failed open/checkpoint cleanup can observe a rename whose final fsync failed.
    // Reconfirm the selected D before any irreversible shortening of P.
    File::open(&paths.meta_path)?.sync_all()?;
    sync_parent_dir(paths.parent_dir()?)?;
    let data = OpenOptions::new()
        .read(true)
        .write(true)
        .create(meta.source.durable_len == 0)
        .truncate(false)
        .open(&paths.data_path)
        .map_err(|e| map_truncated_read_error(e, "durable block data is missing"))?;
    if data.metadata()?.len() < meta.source.durable_len {
        return Err(corrupt("data is shorter than durable checkpoint"));
    }
    data.set_len(meta.source.durable_len)?;
    data.sync_all()?;
    sync_parent_dir(paths.parent_dir()?)
}

fn validate_data_prefix(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    match fs::metadata(&paths.data_path) {
        Ok(data) if data.len() >= meta.source.durable_len => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && meta.source.durable_len == 0 => Ok(()),
        _ => Err(corrupt("durable prefix is missing from data file")),
    }
}

fn validate_token(token: FencingToken, block_id: BlockId) -> StoreResult<()> {
    if token.block_id != block_id || token.owner.is_zero() || token.epoch.as_raw() == 0 {
        return Err(corrupt("invalid persisted writer token"));
    }
    Ok(())
}

fn validate_meta(meta: &BlockMetaPayload, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
    if &meta.identity.group_name != group_name || meta.identity.block_id != block_id {
        return Err(corrupt("block metadata identity differs from path"));
    }
    validate_token(meta.visibility.fencing_token, block_id)?;
    let chunk_size = u32::try_from(meta.format.chunk_size).map_err(|_| corrupt("chunk size overflow"))?;
    BlockShape::new(
        meta.format.format_id,
        meta.format.block_size,
        chunk_size,
        meta.format.block_size,
    )
    .map_err(|e| corrupt(e.to_string()))?;
    if meta.source.durable_len > meta.format.block_size {
        return Err(corrupt("checkpoint exceeds block capacity"));
    }
    Ok(())
}

fn ensure_readable(meta: &BlockMetaPayload) -> StoreResult<()> {
    if meta.visibility.block_state != BlockState::Ready {
        return Err(corrupt("block is not Ready"));
    }
    Ok(())
}

/// The rename becomes an acknowledged checkpoint only after its directory is synced.
fn write_meta(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    validate_meta(meta, &meta.identity.group_name, meta.identity.block_id)?;
    let encoded = encode_meta(meta)?;
    let mut temp = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&paths.temp_meta_path)?;
    temp.write_all(&encoded)?;
    temp.sync_all()?;
    fs::rename(&paths.temp_meta_path, &paths.meta_path)?;
    sync_parent_dir(paths.parent_dir()?)
}

fn complete_deletion(paths: &BlockPaths) -> StoreResult<()> {
    remove_file_if_exists(&paths.data_path)?;
    remove_file_if_exists(&paths.temp_meta_path)?;
    sync_parent_dir(paths.parent_dir()?)?;
    remove_file_if_exists(&paths.meta_path)?;
    sync_parent_dir(paths.parent_dir()?)
}

fn block_files(root: &Path) -> StoreResult<Vec<PathBuf>> {
    if !root.try_exists()? {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for first in fs::read_dir(root)? {
        let first = first?;
        if !first.file_type()?.is_dir() {
            return Err(corrupt("invalid first block directory"));
        }
        for second in fs::read_dir(first.path())? {
            let second = second?;
            if !second.file_type()?.is_dir() {
                return Err(corrupt("invalid second block directory"));
            }
            for file in fs::read_dir(second.path())? {
                let file = file?;
                if !file.file_type()?.is_file() {
                    return Err(corrupt("invalid block artifact"));
                }
                result.push(file.path());
            }
        }
    }
    Ok(result)
}

/// Reconfirm every directory entry below the prepared root, including entries
/// left visible by an earlier mkdir whose parent fsync failed.
fn create_dir_durable(root: &Path, path: &Path) -> StoreResult<()> {
    if !root.is_dir() || !path.starts_with(root) {
        return Err(invalid_argument("block path requires a prepared store root"));
    }
    fs::create_dir_all(path)?;
    for directory in path.ancestors() {
        sync_parent_dir(directory)?;
        if directory == root {
            return Ok(());
        }
    }
    Err(invalid_argument("block directory is outside its store root"))
}

/// Report readers hold checkpoint_access, excluding metadata replacement and truncation.
fn read_report_meta(path: &Path) -> StoreResult<BlockMetaPayload> {
    let meta = read_meta_file(path)?;
    sync_parent_dir(path.parent().ok_or_else(|| corrupt("meta path has no parent"))?)?;
    Ok(meta)
}

fn sync_parent_dir(parent: &Path) -> StoreResult<()> {
    File::open(parent)?.sync_all()?;
    Ok(())
}
fn remove_file_if_exists(path: &Path) -> StoreResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
fn map_truncated_read_error(e: std::io::Error, message: &str) -> WorkerError {
    if matches!(
        e.kind(),
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::NotFound
    ) {
        corrupt(message)
    } else {
        e.into()
    }
}
fn invalid_argument(message: impl Into<String>) -> WorkerError {
    WorkerError::InvalidArgument(message.into())
}
fn corrupt(message: impl Into<String>) -> WorkerError {
    WorkerError::Corrupt(message.into())
}
fn fenced(message: impl Into<String>) -> WorkerError {
    WorkerError::RefreshMetadata {
        kind: beryl_common::error::rpc::ErrorKind::Worker(beryl_common::error::rpc::WorkerErrorKind::Fencing),
        message: message.into(),
    }
}
