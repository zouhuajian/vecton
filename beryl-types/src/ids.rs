// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Identity (ID) types.
//!
//! Design principles:
//! - IDs are pure identity: stable, cheap to copy/clone, no mutable state.
//! - IDs are shared across crates (beryl-types/beryl-metadata/beryl-worker/beryl-client/beryl-proto).
//! - IDs should serialize cleanly for wire/proto/logging.
//! - Do NOT embed layout semantics, placement, or state in IDs.

use core::fmt::{Debug, Display, Formatter, Result as FmtResult};
use serde::{Deserialize, Deserializer, Serialize};
use std::error::Error;
use std::str::FromStr;
use uuid::Uuid;

/// A strongly-typed identifier wrapper.
///
/// Domain rule: IDs are opaque. Do not encode transport/storage semantics into the value.
///
macro_rules! id_new_uint {
    ($(#[$attr:meta])* $name:ident ($ty:ty)) => {
        $(#[$attr])*
        #[repr(transparent)]
        #[derive(
            Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            Serialize, Deserialize
        )]
        #[serde(transparent)]
        pub struct $name(
            /// The raw value of this identifier.
            pub $ty
        );

        impl $name {
            /// Creates a new ID from a raw value.
            #[inline]
            pub const fn new(v: $ty) -> Self { Self(v) }

            /// Returns the inner value.
            #[inline]
            pub const fn as_raw(self) -> $ty { self.0 }
        }

        impl Debug for $name {
            #[inline]
            fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl From<$ty> for $name {
            #[inline]
            fn from(v: $ty) -> Self { Self(v) }
        }

        impl From<$name> for $ty {
            #[inline]
            fn from(v: $name) -> Self { v.0 }
        }
    };
}

/// Inode identifier (64-bit).
///
/// Inodes are the authoritative identity for filesystem objects.
/// Each mount has a root inode, and all files and directories have unique inodes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct InodeId(pub u64);

impl InodeId {
    /// Creates a new InodeId from a raw value.
    #[inline]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// Returns the inner value.
    #[inline]
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    /// Encodes as fixed-width big-endian bytes (8 bytes).
    /// Used for RocksDB key encoding.
    #[inline]
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Decodes from fixed-width big-endian bytes (8 bytes).
    #[inline]
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl Debug for InodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("InodeId").field(&self.0).finish()
    }
}

impl Display for InodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for InodeId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<InodeId> for u64 {
    #[inline]
    fn from(v: InodeId) -> Self {
        v.0
    }
}

id_new_uint!(
    /// A monotonically allocated block sequence within one file inode.
    ///
    /// This is an allocation sequence, not a position in the file. Failed allocations may
    /// leave gaps, and an allocated value is never reused.
    BlockIndex(u32)
);

impl Display for BlockIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

/// Data-plane block identity.
///
/// Blocks are addressed under the owning file's stable `InodeId`. The derived
/// order is lexicographic by inode and allocation sequence, providing a
/// deterministic traversal order without changing identity semantics.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct BlockId {
    /// The file inode this block belongs to.
    pub inode_id: InodeId,
    /// Allocation sequence within the inode; gaps do not imply holes in the file.
    pub index: BlockIndex,
}

impl<'de> Deserialize<'de> for BlockId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SerializedBlockId {
            inode_id: InodeId,
            index: BlockIndex,
        }

        let value = SerializedBlockId::deserialize(deserializer)?;
        if value.inode_id.as_raw() == 0 {
            return Err(serde::de::Error::custom("BlockId.inode_id must be non-zero"));
        }
        Ok(Self::new(value.inode_id, value.index))
    }
}

impl BlockId {
    /// Creates a new `BlockId` from an inode ID and allocation sequence.
    #[inline]
    pub const fn new(inode_id: InodeId, index: BlockIndex) -> Self {
        Self { inode_id, index }
    }

    /// Convenience for tests/logging where you already have primitive values.
    #[inline]
    pub const fn from_u64_u32(inode_id: u64, index: u32) -> Self {
        Self {
            inode_id: InodeId(inode_id),
            index: BlockIndex(index),
        }
    }
}

impl Debug for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // Concise but structured.
        write!(f, "BlockId(inode_id={}, index={})", self.inode_id.0, self.index.0)
    }
}
impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // Stable, human-friendly: "<inode>:<block>"
        write!(f, "{}:{}", self.inode_id.0, self.index.0)
    }
}

impl FromStr for BlockId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid BlockId format: expected 'inode_id:block_index', got '{}'",
                s
            ));
        }
        let inode_id = parts[0]
            .parse::<u64>()
            .map_err(|e| format!("Failed to parse inode_id: {}", e))?;
        if inode_id == 0 {
            return Err("inode_id must be non-zero".to_string());
        }
        let block_index = parts[1]
            .parse::<u32>()
            .map_err(|e| format!("Failed to parse block_index: {}", e))?;
        Ok(BlockId {
            inode_id: InodeId::new(inode_id),
            index: BlockIndex::new(block_index),
        })
    }
}

id_new_uint!(
    /// Worker identity.
    ///
    /// Stable logical worker identity. This must not be confused with
    /// `WorkerRunId`, which identifies a single worker process start.
    WorkerId(u64)
);
impl Display for WorkerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

/// Internal client runtime identity.
///
/// This is generated when a client runtime is created. Display names belong in
/// client_name and must not be used as a correctness identity.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientId(u128);

impl ClientId {
    /// Creates a new ID from a raw value.
    #[inline]
    pub const fn new(v: u128) -> Self {
        Self(v)
    }

    /// Generates a non-zero 128-bit client identity.
    pub fn generate() -> Self {
        loop {
            let value = Uuid::new_v4().as_u128();
            if value != 0 {
                return Self(value);
            }
        }
    }

    /// Returns the inner value.
    #[inline]
    pub const fn as_raw(self) -> u128 {
        self.0
    }

    /// Returns true when the identity is the invalid all-zero value.
    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Parse a non-zero client identity from its decimal wire/header value.
    pub fn parse(value: &str) -> Result<Self, String> {
        let raw = value
            .parse::<u128>()
            .map_err(|err| format!("invalid client_id: {err}"))?;
        if raw == 0 {
            return Err("client_id must be non-zero".to_string());
        }
        Ok(Self(raw))
    }
}

impl Debug for ClientId {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("ClientId")
            .field(&format_args!("0x{:032x}", self.0))
            .finish()
    }
}

impl From<u128> for ClientId {
    #[inline]
    fn from(v: u128) -> Self {
        Self(v)
    }
}

impl From<ClientId> for u128 {
    #[inline]
    fn from(v: ClientId) -> Self {
        v.0
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "0x{:032x}", self.0)
    }
}

// CallId and TxId: UUID-based identifiers for request context

/// Call ID: unique identifier for each RPC call.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallId(Uuid);

impl CallId {
    /// Generate a new CallId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Returns true when the identity is the invalid nil UUID.
    pub fn is_zero(&self) -> bool {
        self.0.is_nil()
    }

    /// Parse a non-zero call identifier from its wire/header UUID value.
    pub fn parse(value: &str) -> Result<Self, String> {
        let uuid = Uuid::parse_str(value).map_err(|err| format!("invalid call_id: {err}"))?;
        if uuid.is_nil() {
            return Err("call_id must be non-zero".to_string());
        }
        Ok(Self(uuid))
    }
}

impl Default for CallId {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for CallId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "CallId({})", self.0)
    }
}

impl Display for CallId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl FromStr for CallId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Stable metadata group identity.
///
/// Group names are identity, not display labels. Renaming a group means creating
/// a different group.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct GroupName(String);

impl GroupName {
    /// Parses and validates a metadata group name.
    pub fn parse(raw: impl AsRef<str>) -> Result<Self, GroupNameError> {
        let value = raw.as_ref().trim();
        if value.is_empty() {
            return Err(GroupNameError::Empty);
        }
        if value.len() > 63 {
            return Err(GroupNameError::TooLong);
        }
        let mut chars = value.chars();
        let first = chars.next().ok_or(GroupNameError::Empty)?;
        if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
            return Err(GroupNameError::InvalidStart);
        }
        if !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')) {
            return Err(GroupNameError::InvalidCharacter);
        }
        Ok(Self(value.to_string()))
    }

    /// Parses an optional metadata group name from a wire/config field.
    ///
    /// An empty string is treated as absent. Non-empty values must satisfy the
    /// same normalized `GroupName` contract as `parse`.
    pub fn parse_optional(raw: impl AsRef<str>) -> Result<Option<Self>, GroupNameError> {
        let value = raw.as_ref();
        if value.is_empty() {
            Ok(None)
        } else {
            Self::parse(value).map(Some)
        }
    }

    /// Returns the validated group name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for GroupName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("GroupName").field(&self.0).finish()
    }
}

impl Display for GroupName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(&self.0)
    }
}

impl FromStr for GroupName {
    type Err = GroupNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl<'de> Deserialize<'de> for GroupName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// Validation error for `GroupName`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupNameError {
    Empty,
    TooLong,
    InvalidStart,
    InvalidCharacter,
}

impl Display for GroupNameError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Empty => f.write_str("must not be empty"),
            Self::TooLong => f.write_str("must be at most 63 characters"),
            Self::InvalidStart => f.write_str("must start with lowercase ASCII letter or digit"),
            Self::InvalidCharacter => f.write_str("must contain only lowercase ASCII letters, digits, '.', '_' or '-'"),
        }
    }
}

impl Error for GroupNameError {}

id_new_uint!(
    /// Mount identity.
    ///
    /// Identifies a mount point that maps a UFS path to the metadata namespace.
    MountId(u64)
);

impl Display for MountId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}
