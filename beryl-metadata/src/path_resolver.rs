// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Path resolver: converts paths to inode IDs via mount resolution and dentry walking.
//!
//! This module provides the core path resolution logic for metadata filesystem operations.
//! It does NOT write any path indices to storage - it only reads from dentry/inode CFs.

use crate::error::{MetadataError, MetadataResult};
use crate::mount::{mount_prefix_matches_path, MountEntry, MountTable};
use crate::raft::RocksDBStorage;
use beryl_types::ids::{InodeId, MountId};
use beryl_types::GroupName;
use std::sync::Arc;

/// Maximum accepted UTF-8 path length, measured in bytes before and after normalization.
pub(crate) const MAX_PATH_BYTES: usize = 4096;
/// Maximum accepted UTF-8 path-component length, measured in bytes.
pub(crate) const MAX_PATH_COMPONENT_BYTES: usize = 255;
/// Maximum number of non-empty components in one normalized path.
pub(crate) const MAX_PATH_COMPONENTS: usize = 256;

/// Mount context: information about the mount point for a resolved path.
#[derive(Clone, Debug)]
pub struct MountContext {
    pub mount_id: MountId,
    pub mount_epoch: u64,
    pub owner_group_name: GroupName,
    pub root_inode_id: InodeId,
}

/// Provider-neutral facts produced by path resolution.
///
/// Existing-target flows require `inode_id`; parent/create flows require
/// `parent_inode_id` and `name`. Mount-root resolution has no parent/name.
#[derive(Clone, Debug)]
pub struct ResolvedPath {
    pub mount_ctx: MountContext,
    /// Canonical components below `mount_ctx.root_inode_id`.
    pub relative_components: Vec<String>,
    pub parent_inode_id: Option<InodeId>,
    pub name: Option<String>,
    pub inode_id: Option<InodeId>,
    /// Mount root through the resolved target, or through its parent when the
    /// final entry does not exist.
    pub ancestor_inode_ids: Vec<InodeId>,
}

/// Path resolver: converts paths to inode IDs.
pub struct PathResolver {
    mount_table: Arc<MountTable>,
    storage: Arc<RocksDBStorage>,
}

impl PathResolver {
    pub(crate) fn new(mount_table: Arc<MountTable>, storage: Arc<RocksDBStorage>) -> Self {
        Self { mount_table, storage }
    }

    /// Normalize a path:
    /// - Remove empty path (return error)
    /// - Remove duplicate '/' (collapse to single '/')
    /// - Remove trailing '/' (except for root '/')
    /// - Reject paths containing '\0'
    /// - Enforce fixed byte, component-length, and component-count limits
    pub fn normalize(path: &str) -> MetadataResult<String> {
        if path.is_empty() {
            return Err(MetadataError::InvalidArgument("Path cannot be empty".to_string()));
        }

        if path.len() > MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Path exceeds {MAX_PATH_BYTES} bytes"
            )));
        }

        if path.contains('\0') {
            return Err(MetadataError::InvalidArgument(
                "Path cannot contain null byte".to_string(),
            ));
        }

        // Split by '/' and filter out empty components
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        if components.len() > MAX_PATH_COMPONENTS {
            return Err(MetadataError::InvalidArgument(format!(
                "Path exceeds {MAX_PATH_COMPONENTS} components"
            )));
        }
        if let Some(component) = components
            .iter()
            .find(|component| component.len() > MAX_PATH_COMPONENT_BYTES)
        {
            return Err(MetadataError::InvalidArgument(format!(
                "Path component exceeds {MAX_PATH_COMPONENT_BYTES} bytes: {component}"
            )));
        }

        if components.is_empty() {
            // Path is "/" or all slashes
            return Ok("/".to_string());
        }

        // Rejoin with single '/'
        let normalized = format!("/{}", components.join("/"));
        if normalized.len() > MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Normalized path exceeds {MAX_PATH_BYTES} bytes"
            )));
        }

        Ok(normalized)
    }

    /// Resolve mount: find the longest matching mount prefix.
    /// Returns (mount_entry, relative_components).
    fn resolve_mount(&self, path: &str) -> MetadataResult<(MountEntry, Vec<String>)> {
        let normalized = Self::normalize(path)?;

        // Find longest matching mount prefix
        let mounts = self.mount_table.list_mounts();
        let mut best_match: Option<(MountEntry, Vec<String>)> = None;
        let mut best_prefix_len = 0;

        for mount in mounts {
            let prefix = &mount.mount_prefix;
            if mount_prefix_matches_path(prefix, &normalized) {
                let prefix_len = prefix.len();
                if prefix_len > best_prefix_len {
                    // Extract relative path components
                    let relative = if prefix_len == normalized.len() {
                        vec![]
                    } else if normalized.as_bytes()[prefix_len] == b'/' {
                        // Skip the '/' after prefix
                        normalized[prefix_len + 1..]
                            .split('/')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect()
                    } else {
                        // No '/' after prefix (shouldn't happen with normalized paths)
                        normalized[prefix_len..]
                            .split('/')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect()
                    };
                    best_match = Some((mount.clone(), relative));
                    best_prefix_len = prefix_len;
                }
            }
        }

        best_match.ok_or_else(|| MetadataError::NotFound(format!("No mount found for path: {}", normalized)))
    }

    /// Resolve path to its owning mount and mount-relative components without
    /// requiring the namespace entries to exist.
    pub(crate) fn resolve_mount_components(&self, path: &str) -> MetadataResult<(MountContext, Vec<String>)> {
        let (mount_entry, components) = self.resolve_mount(path)?;
        Ok((
            MountContext {
                mount_id: mount_entry.mount_id,
                mount_epoch: mount_entry.mount_epoch,
                owner_group_name: mount_entry.namespace_owner_group_name,
                root_inode_id: mount_entry.root_inode_id,
            },
            components,
        ))
    }

    /// Walk the dentry tree and append every visited inode to the bounded ancestor chain.
    fn walk_dentry(
        &self,
        root_inode_id: InodeId,
        components: &[String],
        ancestor_inode_ids: &mut Vec<InodeId>,
    ) -> MetadataResult<InodeId> {
        let mut current_inode_id = root_inode_id;

        for component in components {
            // Get dentry
            let child_inode_id = self.storage.get_dentry(current_inode_id, component)?.ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "Entry not found: {} (parent inode: {})",
                    component, current_inode_id
                ))
            })?;

            current_inode_id = child_inode_id;
            ancestor_inode_ids.push(child_inode_id);
        }

        Ok(current_inode_id)
    }

    /// Resolve a path into its mount, parent entry, and optional target inode.
    ///
    /// The mount root resolves directly to its root inode without a parent or
    /// terminal name. For other paths, the parent and terminal name are always
    /// populated while the target inode remains optional so create operations
    /// can resolve a path whose final entry does not exist yet.
    pub fn resolve_path(&self, path: &str) -> MetadataResult<ResolvedPath> {
        let (mount_entry, components) = self.resolve_mount(path)?;

        if components.is_empty() {
            return Ok(ResolvedPath {
                mount_ctx: MountContext {
                    mount_id: mount_entry.mount_id,
                    mount_epoch: mount_entry.mount_epoch,
                    owner_group_name: mount_entry.namespace_owner_group_name,
                    root_inode_id: mount_entry.root_inode_id,
                },
                relative_components: Vec::new(),
                parent_inode_id: None,
                name: None,
                inode_id: Some(mount_entry.root_inode_id),
                ancestor_inode_ids: vec![mount_entry.root_inode_id],
            });
        }

        // Split into parent components and name
        let (parent_components, name) = components.split_at(components.len() - 1);
        let name = name[0].clone();
        let mut ancestor_inode_ids = vec![mount_entry.root_inode_id];

        // Walk to parent directory.
        let parent_inode_id = if parent_components.is_empty() {
            mount_entry.root_inode_id
        } else {
            self.walk_dentry(mount_entry.root_inode_id, parent_components, &mut ancestor_inode_ids)?
        };

        // Verify parent is a directory
        let parent_inode = self
            .storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;

        if !parent_inode.file_type().is_dir() {
            return Err(MetadataError::NotDir(format!(
                "Parent is not a directory: {}",
                parent_inode_id
            )));
        }

        // The final entry is optional because create and rename destinations
        // are valid resolution targets before their dentry exists.
        let inode_id = self.storage.get_dentry(parent_inode_id, &name)?;
        if let Some(inode_id) = inode_id {
            ancestor_inode_ids.push(inode_id);
        }

        Ok(ResolvedPath {
            mount_ctx: MountContext {
                mount_id: mount_entry.mount_id,
                mount_epoch: mount_entry.mount_epoch,
                owner_group_name: mount_entry.namespace_owner_group_name,
                root_inode_id: mount_entry.root_inode_id,
            },
            relative_components: components,
            parent_inode_id: Some(parent_inode_id),
            name: Some(name),
            inode_id,
            ancestor_inode_ids,
        })
    }

    /// Resolve two paths for rename operation.
    /// Returns (src_resolved, dst_resolved).
    /// If paths are in different mounts, returns error (caller should convert to EXDEV).
    pub fn resolve_rename(&self, src_path: &str, dst_path: &str) -> MetadataResult<(ResolvedPath, ResolvedPath)> {
        let src_resolved = self.resolve_path(src_path)?;
        let dst_resolved = self.resolve_path(dst_path)?;

        // Check if same mount
        if src_resolved.mount_ctx.mount_id != dst_resolved.mount_ctx.mount_id {
            return Err(MetadataError::CrossMountRename(format!(
                "Cross-mount rename not allowed: src_mount={:?}, dst_mount={:?}",
                src_resolved.mount_ctx.mount_id, dst_resolved.mount_ctx.mount_id
            )));
        }

        Ok((src_resolved, dst_resolved))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_enforces_path_component_and_depth_limits() {
        let longest_component = "a".repeat(MAX_PATH_COMPONENT_BYTES);
        assert!(PathResolver::normalize(&format!("/{longest_component}")).is_ok());
        assert!(PathResolver::normalize(&format!("/{longest_component}a")).is_err());

        let deepest_path = format!("/{}", vec!["a"; MAX_PATH_COMPONENTS].join("/"));
        assert!(PathResolver::normalize(&deepest_path).is_ok());
        let too_deep_path = format!("/{}", vec!["a"; MAX_PATH_COMPONENTS + 1].join("/"));
        assert!(PathResolver::normalize(&too_deep_path).is_err());

        let component = "a".repeat(MAX_PATH_COMPONENT_BYTES);
        let longest_path = format!("/{}", vec![component.as_str(); 16].join("/"));
        assert_eq!(longest_path.len(), MAX_PATH_BYTES);
        assert!(PathResolver::normalize(&longest_path).is_ok());
        assert!(PathResolver::normalize(&format!("{longest_path}/a")).is_err());
    }
}
