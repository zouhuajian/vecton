// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{
    AppMetadataRaftState, AppRaftStateMachine, BootstrapNamespaceState, CreateFileOperationId, CreateFileReplayRecord,
    DetachedRoot, FileLayout, GroupName, Inode, InodeAllocation, InodeAttrs, InodeId, InodeKind, MetadataError,
    MetadataResult, MountId, PreparedRename, PreparedRenameOverwrite, PreparedUnlink, RecursiveMkdirEntry,
    RenameAtomicUpdate, RenameOverwriteCleanup,
};
use crate::mount::{DataIoPolicy, MountEntry, MountKind};
use beryl_types::{ContentGeneration, LeaseEpoch};

impl AppRaftStateMachine {
    pub(super) fn apply_bootstrap_namespace(
        &self,
        group_name: GroupName,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<MountEntry> {
        let state = self.storage.bootstrap_namespace_state(&group_name)?;
        if state == BootstrapNamespaceState::Conflicting {
            return Err(MetadataError::InvalidArgument(
                "metadata namespace is partially initialized or conflicts with writable root bootstrap; reformat metadata storage"
                    .to_string(),
            ));
        }

        let root_mount = MountEntry {
            mount_id: MountId::new(1),
            mount_prefix: crate::mount::ROOT_MOUNT_PREFIX.to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 1,
            namespace_owner_group_name: group_name,
            root_inode_id: crate::mount::ROOT_INODE_ID,
        };
        if state == BootstrapNamespaceState::Matching {
            self.storage.commit_applied_state(raft_state)?;
            return Ok(root_mount);
        }

        let mut attrs = InodeAttrs::new();
        attrs.initialize(proposed_at_ms);

        let root_inode = Inode::new_dir(crate::mount::ROOT_INODE_ID, attrs, MountId::new(1));
        self.storage
            .bootstrap_namespace_atomic(&root_inode, &root_mount, raft_state)?;
        Ok(root_mount)
    }

    /// Apply Mkdir command.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_mkdir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: InodeAttrs,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<(InodeId, InodeAttrs)> {
        let prepared: MetadataResult<(InodeAllocation, Inode, Inode)> = (|| {
            // Check parent exists and is a directory
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

            // Check if name already exists
            if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
                return Err(MetadataError::AlreadyExists(format!(
                    "Directory already exists: {}",
                    name
                )));
            }

            // Generate inode ID
            let allocation = self.storage.prepare_inode_allocation()?;
            let inode_id = allocation.inode_id;
            let now_ms = proposed_at_ms;

            // Initialize attrs
            attrs.initialize(now_ms);

            // Create directory inode (inherit mount_id from parent)
            let inode = Inode::new_dir(inode_id, attrs, parent_inode.mount_id);

            // Update parent directory modification time
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.set_modify_time(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok((allocation, inode, updated_parent))
        })();

        let (allocation, inode, updated_parent) = prepared?;
        let result = (inode.inode_id, inode.attrs.clone());
        self.storage
            .create_dir_atomic(allocation, parent_inode_id, &name, &inode, &updated_parent, raft_state)?;
        Ok(result)
    }

    /// Apply one recursive CreateDirectory command as a single authority batch.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_create_directory(
        &self,
        root_inode_id: InodeId,
        components: Vec<String>,
        attrs: InodeAttrs,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<(InodeId, InodeAttrs)> {
        if components.is_empty() || components.iter().any(|component| component.is_empty()) {
            return Err(MetadataError::InvalidArgument(
                "CreateDirectory requires non-empty path components".to_string(),
            ));
        }
        let mut parent = match self.storage.get_inode(root_inode_id)? {
            Some(inode) if inode.file_type().is_dir() => inode,
            Some(_) => {
                return Err(MetadataError::NotDir(format!(
                    "Root is not a directory: {root_inode_id}"
                )));
            }
            None => {
                return Err(MetadataError::NotFound(format!(
                    "Root inode not found: {root_inode_id}"
                )));
            }
        };
        let mut allocation = self.storage.prepare_inode_allocation()?;
        let mut next_raw = allocation.inode_id.as_raw();
        let mut entries = Vec::new();

        for name in components {
            if let Some(child_inode_id) = self.storage.get_dentry(parent.inode_id, &name)? {
                let child = match self.storage.get_inode(child_inode_id)? {
                    Some(inode) if inode.file_type().is_dir() => inode,
                    Some(_) => {
                        return Err(MetadataError::NotDir(format!(
                            "Path component is not a directory: {name}"
                        )));
                    }
                    None => {
                        return Err(MetadataError::NotFound(format!(
                            "Target inode not found: {child_inode_id}"
                        )));
                    }
                };
                parent = child;
                continue;
            }

            let inode_id = InodeId::new(next_raw);
            next_raw = next_raw
                .checked_add(1)
                .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
            let mut child_attrs = attrs.clone();
            child_attrs.initialize(proposed_at_ms);

            let child = Inode::new_dir(inode_id, child_attrs, parent.mount_id);
            let mut updated_parent = parent.clone();
            updated_parent
                .attrs
                .set_modify_time(Self::mutation_timestamp(&parent, proposed_at_ms));
            entries.push(RecursiveMkdirEntry {
                parent_inode_id: parent.inode_id,
                name,
                inode: child.clone(),
                updated_parent,
            });
            parent = child;
        }

        let result = (parent.inode_id, parent.attrs.clone());
        if entries.is_empty() {
            self.storage.commit_applied_state(raft_state)?;
        } else {
            allocation.next_inode_id = InodeId::new(next_raw);
            self.storage
                .create_directories_atomic(allocation, &entries, raft_state)?;
        }
        Ok(result)
    }

    /// Create a file, its initial write lease, and its replay record in one commit.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_create(
        &self,
        operation_id: CreateFileOperationId,
        request_deadline_ms: u64,
        session_expires_at_ms: u64,
        normalized_path: String,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        mut attrs: InodeAttrs,
        layout: FileLayout,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<CreateFileReplayRecord> {
        if let Some(record) = self.storage.get_create_file_replay(operation_id)? {
            if record.request_deadline_ms != request_deadline_ms
                || record.normalized_path != normalized_path
                || record.mount_id != mount_id
                || record.expected_mount_epoch != expected_mount_epoch
                || record.mount_root_inode_id != mount_root_inode_id
                || record.relative_components != relative_components
            {
                return Err(MetadataError::InvalidArgument(
                    "CreateFile operation identity was reused with a different request".to_string(),
                ));
            }
            self.validate_create_file_replay(&record, proposed_at_ms)?;
            self.storage.commit_applied_state(raft_state)?;
            return Ok(record);
        }
        if request_deadline_ms < proposed_at_ms {
            return Err(MetadataError::InvalidArgument(
                "CreateFile request deadline expired before proposal".to_string(),
            ));
        }
        if session_expires_at_ms <= proposed_at_ms {
            return Err(MetadataError::Again(
                "CreateFile write session expired before proposal".to_string(),
            ));
        }
        layout
            .validate()
            .map_err(|error| MetadataError::InvalidArgument(format!("invalid CreateFile layout: {error}")))?;
        let (parent_inode_id, name, parent_inode) = self.resolve_create_parent(
            mount_id,
            expected_mount_epoch,
            mount_root_inode_id,
            &relative_components,
        )?;
        if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
            return Err(MetadataError::AlreadyExists(format!("File already exists: {name}")));
        }

        let prepared: MetadataResult<(InodeAllocation, Inode, Inode)> = (|| {
            // Generate inode ID
            let allocation = self.storage.prepare_inode_allocation()?;
            let inode_id = allocation.inode_id;
            let now_ms = proposed_at_ms;

            // Initialize attrs
            attrs.initialize(now_ms);

            // Create the file under its single canonical inode identity.
            let mut inode = Inode::new_file(inode_id, attrs, parent_inode.mount_id, layout);
            let InodeKind::File(crate::inode::FileData { lease_epoch, .. }) = &mut inode.kind else {
                unreachable!("new file constructor must produce file authority")
            };
            *lease_epoch = LeaseEpoch::new(1);

            // Update parent directory modification time
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.set_modify_time(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok((allocation, inode, updated_parent))
        })();

        let (allocation, inode, updated_parent) = prepared?;
        let record = CreateFileReplayRecord {
            operation_id,
            request_deadline_ms,
            normalized_path,
            parent_inode_id,
            name: name.clone(),
            inode_id: inode.inode_id,
            mount_id,
            expected_mount_epoch,
            mount_root_inode_id,
            relative_components,
            lease_epoch: LeaseEpoch::new(1),
            layout,
            generation: ContentGeneration::new(0),
            expires_at_ms: session_expires_at_ms,
        };
        self.storage.create_file_atomic(
            allocation,
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            &record,
            proposed_at_ms,
            raft_state,
        )?;
        Ok(record)
    }

    /// Confirm that a durable CreateFile result still names its initial writable state.
    fn validate_create_file_replay(&self, record: &CreateFileReplayRecord, proposed_at_ms: u64) -> MetadataResult<()> {
        if record.expires_at_ms <= proposed_at_ms {
            return Err(MetadataError::Again(
                "replayed CreateFile write session has expired".to_string(),
            ));
        }
        let (parent_inode_id, name, _) = self.resolve_create_parent(
            record.mount_id,
            record.expected_mount_epoch,
            record.mount_root_inode_id,
            &record.relative_components,
        )?;
        if parent_inode_id != record.parent_inode_id || name != record.name {
            return Err(MetadataError::Again(
                "replayed CreateFile path authority changed".to_string(),
            ));
        }
        if self.storage.get_dentry(record.parent_inode_id, &record.name)? != Some(record.inode_id) {
            return Err(MetadataError::AlreadyExists(
                "replayed CreateFile target no longer names its original inode".to_string(),
            ));
        }
        let inode = self
            .storage
            .get_inode(record.inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("CreateFile inode not found: {}", record.inode_id)))?;
        if inode.inode_id != record.inode_id || inode.mount_id != record.mount_id || !inode.file_type().is_file() {
            return Err(MetadataError::Internal(
                "replayed CreateFile inode authority is corrupt".to_string(),
            ));
        }
        let InodeKind::File(crate::inode::FileData {
            blocks,
            generation,
            lease_epoch,
            next_index,
            ..
        }) = &inode.kind
        else {
            return Err(MetadataError::Internal(
                "replayed CreateFile inode payload is not a file".to_string(),
            ));
        };
        if *lease_epoch != record.lease_epoch {
            return Err(MetadataError::LeaseFenced {
                expected: *lease_epoch,
                got: record.lease_epoch,
            });
        }
        if !blocks.is_empty() || *generation != record.generation || *next_index != 0 || inode.len() != 0 {
            return Err(MetadataError::AlreadyExists(
                "replayed CreateFile result no longer owns the initial file state".to_string(),
            ));
        }
        if self.storage.get_layout(record.inode_id)? != record.layout {
            return Err(MetadataError::Internal(
                "replayed CreateFile layout authority changed".to_string(),
            ));
        }
        Ok(())
    }

    /// Revalidate the mount-relative parent path carried by CreateFile apply.
    fn resolve_create_parent(
        &self,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: &[String],
    ) -> MetadataResult<(InodeId, String, Inode)> {
        Self::validate_relative_components("CreateFile", relative_components)?;
        let mount = self
            .storage
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {mount_id:?}")))?;
        if mount.mount_epoch != expected_mount_epoch || mount.root_inode_id != mount_root_inode_id {
            return Err(MetadataError::Again(format!(
                "CreateFile mount precondition changed for {mount_id:?}"
            )));
        }
        let mut parent = self
            .storage
            .get_inode(mount_root_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount root inode not found: {mount_root_inode_id}")))?;
        if parent.inode_id != mount_root_inode_id
            || parent.mount_id != mount_id
            || !parent.file_type().is_dir()
            || !matches!(&parent.kind, InodeKind::Dir)
        {
            return Err(MetadataError::Internal(
                "CreateFile mount root authority is corrupt".to_string(),
            ));
        }
        let (name, parent_components) = relative_components
            .split_last()
            .expect("validated CreateFile path has a terminal component");
        for component in parent_components {
            let child_inode_id = self.storage.get_dentry(parent.inode_id, component)?.ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "Entry not found: {component} (parent inode: {})",
                    parent.inode_id
                ))
            })?;
            let child = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {child_inode_id}")))?;
            if child.inode_id != child_inode_id || child.mount_id != mount_id {
                return Err(MetadataError::Internal(
                    "CreateFile parent path authority is corrupt".to_string(),
                ));
            }
            if !child.file_type().is_dir() || !matches!(&child.kind, InodeKind::Dir) {
                return Err(MetadataError::NotDir(format!(
                    "Path component is not a directory: {component}"
                )));
            }
            parent = child;
        }
        Ok((parent.inode_id, name.clone(), parent))
    }

    /// Revalidate one bounded mount-relative Delete command and apply its target-specific mutation.
    ///
    /// Path resolution happens inside Raft apply so a stale leader admission
    /// cannot mutate a parent that has since become unreachable. Work is
    /// bounded by the fixed path limits and the number of mount records, never
    /// by the size of a recursive-delete subtree.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_delete(
        &self,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        expected_file_lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let (parent_inode_id, name, child_inode) = self.resolve_delete_target(
            mount_id,
            expected_mount_epoch,
            mount_root_inode_id,
            &relative_components,
        )?;
        if child_inode.inode_id != expected_inode_id {
            return Err(MetadataError::Again(format!(
                "delete target changed for {name}: expected {expected_inode_id}, current {}",
                child_inode.inode_id
            )));
        }

        if child_inode.file_type().is_dir() {
            if expected_file_lease_epoch.is_some() {
                return Err(MetadataError::Again(
                    "delete target lease precondition changed".to_string(),
                ));
            }
            if recursive {
                self.apply_detach_directory(parent_inode_id, name, child_inode.inode_id, proposed_at_ms, raft_state)
            } else {
                self.apply_delete_empty_dir(parent_inode_id, name, proposed_at_ms, raft_state)
            }
        } else {
            let current_file_lease_epoch = match &child_inode.kind {
                InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(*lease_epoch),
                _ => None,
            };
            if current_file_lease_epoch != expected_file_lease_epoch {
                return Err(MetadataError::Again(format!(
                    "delete target lease precondition changed: expected {expected_file_lease_epoch:?}, current {current_file_lease_epoch:?}"
                )));
            }
            self.apply_unlink(parent_inode_id, name, proposed_at_ms, raft_state)
        }
    }

    /// Resolve and validate the exact target named by a replicated Delete command.
    fn resolve_delete_target(
        &self,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: &[String],
    ) -> MetadataResult<(InodeId, String, Inode)> {
        Self::validate_relative_components("Delete", relative_components)?;
        let mounts = self.storage.list_mounts()?;
        let mount = mounts
            .iter()
            .find(|entry| entry.mount_id == mount_id)
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {mount_id:?}")))?;
        if mount.mount_epoch != expected_mount_epoch || mount.root_inode_id != mount_root_inode_id {
            return Err(MetadataError::Again(format!(
                "delete mount precondition changed for {mount_id:?}"
            )));
        }

        let relative_path_bytes = relative_components
            .iter()
            .try_fold(relative_components.len().saturating_sub(1), |bytes, component| {
                bytes.checked_add(component.len())
            })
            .ok_or_else(|| MetadataError::InvalidArgument("Delete path length overflow".to_string()))?;
        let target_path_bytes = if mount.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX {
            1usize.checked_add(relative_path_bytes)
        } else {
            mount
                .mount_prefix
                .len()
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(relative_path_bytes))
        }
        .ok_or_else(|| MetadataError::InvalidArgument("Delete path length overflow".to_string()))?;
        if target_path_bytes > crate::path_resolver::MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Delete path exceeds {} bytes",
                crate::path_resolver::MAX_PATH_BYTES
            )));
        }
        let relative_path = relative_components.join("/");
        let target_path = if mount.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX {
            format!("/{relative_path}")
        } else {
            format!("{}/{relative_path}", mount.mount_prefix)
        };
        if mounts.iter().any(|entry| {
            entry.mount_id != mount_id && crate::mount::mount_prefix_matches_path(&target_path, &entry.mount_prefix)
        }) {
            return Err(MetadataError::CrossMountRename(
                "delete target is a mount root or contains a nested mount".to_string(),
            ));
        }

        let mut parent = self
            .storage
            .get_inode(mount_root_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount root inode not found: {mount_root_inode_id}")))?;
        if parent.inode_id != mount_root_inode_id {
            return Err(MetadataError::Internal(format!(
                "mount root inode key {mount_root_inode_id} contains inode {}",
                parent.inode_id
            )));
        }
        if !parent.file_type().is_dir() || !matches!(&parent.kind, InodeKind::Dir) {
            return Err(MetadataError::NotDir(format!(
                "Mount root is not a directory: {mount_root_inode_id}"
            )));
        }
        if parent.mount_id != mount_id {
            return Err(MetadataError::CrossMountRename(
                "mount root inode belongs to a different mount".to_string(),
            ));
        }

        for (index, component) in relative_components.iter().enumerate() {
            let child_inode_id = self.storage.get_dentry(parent.inode_id, component)?.ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "Entry not found: {component} (parent inode: {})",
                    parent.inode_id
                ))
            })?;
            let child = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {child_inode_id}")))?;
            if child.inode_id != child_inode_id {
                return Err(MetadataError::Internal(format!(
                    "inode key {child_inode_id} contains inode {}",
                    child.inode_id
                )));
            }
            if child.mount_id != mount_id {
                return Err(MetadataError::CrossMountRename(
                    "delete path crosses mount authority".to_string(),
                ));
            }
            if index + 1 == relative_components.len() {
                if mounts.iter().any(|entry| entry.root_inode_id == child_inode_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Cannot delete mount root inode {child_inode_id}"
                    )));
                }
                return Ok((parent.inode_id, component.clone(), child));
            }
            if !child.file_type().is_dir() || !matches!(&child.kind, InodeKind::Dir) {
                return Err(MetadataError::NotDir(format!(
                    "Path component is not a directory: {component}"
                )));
            }
            parent = child;
        }

        unreachable!("Delete components are checked as non-empty")
    }

    /// Enforce the fixed replicated bound for one mount-relative namespace path.
    fn validate_relative_components(operation: &str, relative_components: &[String]) -> MetadataResult<()> {
        if relative_components.is_empty() {
            return Err(MetadataError::InvalidArgument(format!(
                "{operation} cannot target a mount root"
            )));
        }
        if relative_components.len() > crate::path_resolver::MAX_PATH_COMPONENTS {
            return Err(MetadataError::InvalidArgument(format!(
                "{operation} path exceeds {} components",
                crate::path_resolver::MAX_PATH_COMPONENTS
            )));
        }
        for component in relative_components {
            if component.is_empty() || component.contains('/') || component.contains('\0') {
                return Err(MetadataError::InvalidArgument(format!(
                    "{operation} path contains an invalid component"
                )));
            }
            if component.len() > crate::path_resolver::MAX_PATH_COMPONENT_BYTES {
                return Err(MetadataError::InvalidArgument(format!(
                    "{operation} path component exceeds {} bytes",
                    crate::path_resolver::MAX_PATH_COMPONENT_BYTES
                )));
            }
        }
        Ok(())
    }

    /// Apply Unlink command.
    pub(super) fn apply_unlink(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let prepared: MetadataResult<PreparedUnlink> = (|| {
            // Get dentry
            let child_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Entry not found: {}", name)))?;

            // Get child inode
            let child_inode = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;

            // Check it's not a directory
            if child_inode.file_type().is_dir() {
                return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name)));
            }

            // Update parent directory modification time
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.set_modify_time(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            match &child_inode.kind {
                InodeKind::File(crate::inode::FileData { .. }) => {
                    if child_inode.inode_id != child_inode_id
                        || self.storage.get_layout_optional(child_inode_id)?.is_none()
                    {
                        return Err(MetadataError::Internal(format!(
                            "file inode {child_inode_id} has corrupt identity or missing layout: value_id={}",
                            child_inode.inode_id
                        )));
                    }
                }

                InodeKind::Dir => return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name))),
            }

            Ok((child_inode_id, updated_parent))
        })();

        let (child_inode_id, updated_parent) = prepared?;
        self.storage
            .unlink_inode_atomic(parent_inode_id, &name, child_inode_id, &updated_parent, raft_state)?;
        Ok(())
    }

    /// Apply empty-directory delete command.
    pub(super) fn apply_delete_empty_dir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let prepared: MetadataResult<(InodeId, Inode)> = (|| {
            // Get dentry
            let child_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Directory not found: {}", name)))?;

            // Get child inode
            let child_inode = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;

            // Check it's a directory
            if !child_inode.file_type().is_dir() {
                return Err(MetadataError::NotDir(format!("Not a directory: {}", name)));
            }

            // Check directory is empty
            if !self.storage.is_directory_empty(child_inode_id)? {
                return Err(MetadataError::DirectoryNotEmpty(format!(
                    "Directory not empty: {}",
                    name
                )));
            }

            // Update parent directory modification time
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.set_modify_time(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok((child_inode_id, updated_parent))
        })();

        let (child_inode_id, updated_parent) = prepared?;
        self.storage
            .unlink_inode_atomic(parent_inode_id, &name, child_inode_id, &updated_parent, raft_state)?;
        Ok(())
    }

    /// Atomically hide a recursive-delete root and make it reclaimable.
    pub(super) fn apply_detach_directory(
        &self,
        parent_inode_id: InodeId,
        name: String,
        root_inode_id: InodeId,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let prepared: MetadataResult<(Inode, DetachedRoot)> = (|| {
            let current_root_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Directory not found: {name}")))?;
            if current_root_inode_id != root_inode_id {
                return Err(MetadataError::Again(format!(
                    "delete target changed for {name}: expected {root_inode_id}, current {current_root_inode_id}"
                )));
            }
            let root_inode = self
                .storage
                .get_inode(root_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Root inode not found: {root_inode_id}")))?;
            if !root_inode.file_type().is_dir() || !matches!(&root_inode.kind, InodeKind::Dir) {
                return Err(MetadataError::NotDir(format!("Not a directory: {name}")));
            }
            if root_inode.inode_id != root_inode_id || self.storage.get_layout_optional(root_inode_id)?.is_some() {
                return Err(MetadataError::Internal(format!(
                    "directory inode {root_inode_id} carries file authority"
                )));
            }
            if self.storage.get_detached_root(root_inode_id)?.is_some() {
                return Err(MetadataError::Internal(format!(
                    "inode {root_inode_id} is both reachable and already detached"
                )));
            }

            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            if !parent_inode.file_type().is_dir() || !matches!(&parent_inode.kind, InodeKind::Dir) {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {parent_inode_id}"
                )));
            }
            if parent_inode.mount_id != root_inode.mount_id {
                return Err(MetadataError::CrossMountRename(
                    "recursive delete cannot cross mount boundary".to_string(),
                ));
            }

            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.set_modify_time(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode;
            updated_parent.attrs = parent_attrs;

            Ok((
                updated_parent,
                DetachedRoot {
                    mount_id: root_inode.mount_id,
                    detached_at_ms: proposed_at_ms,
                },
            ))
        })();

        let (updated_parent, detached_root) = prepared?;
        self.storage.detach_directory_atomic(
            parent_inode_id,
            &name,
            root_inode_id,
            &updated_parent,
            detached_root,
            raft_state,
        )?;
        Ok(())
    }

    /// Apply Rename command (atomic within mount).
    // Keep the state transition inputs explicit at the apply boundary.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_rename(
        &self,
        src_parent_inode_id: InodeId,
        src_name: String,
        expected_src_inode_id: InodeId,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        expected_dst_inode_id: Option<InodeId>,
        expected_dst_lease_epoch: Option<LeaseEpoch>,
        flags: u32,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let prepared: MetadataResult<PreparedRename> = (|| {
            // Get source dentry
            let src_inode_id = self
                .storage
                .get_dentry(src_parent_inode_id, &src_name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source not found: {}", src_name)))?;
            if src_inode_id != expected_src_inode_id {
                return Err(MetadataError::Again(format!(
                    "rename source changed for {src_name}: expected {expected_src_inode_id}, current {src_inode_id}"
                )));
            }

            let current_dst_inode_id = self.storage.get_dentry(dst_parent_inode_id, &dst_name)?;
            if current_dst_inode_id != expected_dst_inode_id {
                return Err(MetadataError::Again(format!(
                    "rename destination changed for {dst_name}: expected {expected_dst_inode_id:?}, current {current_dst_inode_id:?}"
                )));
            }

            // Get source inode
            let src_inode = self
                .storage
                .get_inode(src_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source inode not found: {}", src_inode_id)))?;

            let mut overwritten_target = None;

            // Check if destination exists
            if let Some(dst_inode_id) = current_dst_inode_id {
                // NOREPLACE flag set -> fail when destination exists
                if flags & 0x1 != 0 {
                    return Err(MetadataError::AlreadyExists(format!(
                        "Destination exists and RENAME_NOREPLACE set: {}",
                        dst_name
                    )));
                }
                if src_inode_id == dst_inode_id {
                    return Ok(PreparedRename {
                        src_inode_id,
                        overwritten_target: None,
                        updated_src_parent: None,
                        updated_dst_parent: None,
                    });
                }
                // Destination exists - check if it's a directory and empty (if source is directory)
                let dst_inode = self
                    .storage
                    .get_inode(dst_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination inode disappeared".to_string()))?;
                let current_dst_lease_epoch = match &dst_inode.kind {
                    InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(*lease_epoch),
                    _ => None,
                };
                if current_dst_lease_epoch != expected_dst_lease_epoch {
                    return Err(MetadataError::Again(format!(
                        "rename destination lease epoch changed for {dst_name}: expected {expected_dst_lease_epoch:?}, current {current_dst_lease_epoch:?}"
                    )));
                }

                if src_inode.file_type().is_dir() {
                    if !dst_inode.file_type().is_dir() {
                        return Err(MetadataError::NotDir(
                            "Cannot overwrite non-directory with directory".to_string(),
                        ));
                    }
                    if !self.storage.is_directory_empty(dst_inode_id)? {
                        return Err(MetadataError::DirectoryNotEmpty(
                            "Cannot overwrite non-empty directory".to_string(),
                        ));
                    }
                } else {
                    if dst_inode.file_type().is_dir() {
                        return Err(MetadataError::IsDir("Cannot overwrite directory with file".to_string()));
                    }
                }
                overwritten_target = Some(self.prepare_rename_overwrite_target_cleanup(dst_inode_id, &dst_inode)?);
            }

            // Update parent directories modification time
            let (updated_src_parent, updated_dst_parent) = if src_parent_inode_id != dst_parent_inode_id {
                // Different parents - update both
                let src_parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Source parent disappeared".to_string()))?;
                let mut src_attrs = src_parent.attrs.clone();
                src_attrs.set_modify_time(Self::mutation_timestamp(&src_parent, proposed_at_ms));
                let mut src_parent = src_parent.clone();
                src_parent.attrs = src_attrs;
                let dst_parent = self
                    .storage
                    .get_inode(dst_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination parent disappeared".to_string()))?;
                let mut dst_attrs = dst_parent.attrs.clone();
                dst_attrs.set_modify_time(Self::mutation_timestamp(&dst_parent, proposed_at_ms));
                let mut dst_parent = dst_parent.clone();
                dst_parent.attrs = dst_attrs;
                (Some(src_parent), Some(dst_parent))
            } else {
                let parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Parent disappeared".to_string()))?;
                let mut attrs = parent.attrs.clone();
                attrs.set_modify_time(Self::mutation_timestamp(&parent, proposed_at_ms));
                let mut parent = parent.clone();
                parent.attrs = attrs;
                (Some(parent), None)
            };

            Ok(PreparedRename {
                src_inode_id,
                overwritten_target,
                updated_src_parent,
                updated_dst_parent,
            })
        })();

        let prepared = prepared?;
        self.storage.rename_atomic(
            RenameAtomicUpdate {
                src_parent_inode_id,
                src_name: &src_name,
                dst_parent_inode_id,
                dst_name: &dst_name,
                src_inode_id: prepared.src_inode_id,
                overwritten_target: prepared
                    .overwritten_target
                    .as_ref()
                    .map(|target| RenameOverwriteCleanup {
                        inode_id: target.inode_id,
                    }),
                updated_src_parent: prepared.updated_src_parent.as_ref(),
                updated_dst_parent: prepared.updated_dst_parent.as_ref(),
            },
            raft_state,
        )?;

        Ok(())
    }

    fn prepare_rename_overwrite_target_cleanup(
        &self,
        dst_inode_id: InodeId,
        dst_inode: &Inode,
    ) -> MetadataResult<PreparedRenameOverwrite> {
        match &dst_inode.kind {
            InodeKind::File(crate::inode::FileData { .. }) => {
                if dst_inode.inode_id != dst_inode_id || self.storage.get_layout_optional(dst_inode_id)?.is_none() {
                    return Err(MetadataError::Internal(format!(
                        "file inode {dst_inode_id} has corrupt identity or missing layout: value_id={}",
                        dst_inode.inode_id
                    )));
                }
                Ok(PreparedRenameOverwrite { inode_id: dst_inode_id })
            }
            InodeKind::Dir => {
                if !self.storage.is_directory_empty(dst_inode_id)? {
                    return Err(MetadataError::DirectoryNotEmpty(
                        "Cannot overwrite non-empty directory".to_string(),
                    ));
                }
                if dst_inode.inode_id != dst_inode_id || self.storage.get_layout_optional(dst_inode_id)?.is_some() {
                    return Err(MetadataError::Internal(format!(
                        "directory inode {dst_inode_id} carries invalid file authority: value_id={}",
                        dst_inode.inode_id
                    )));
                }
                Ok(PreparedRenameOverwrite { inode_id: dst_inode_id })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::response::ApplyRejectionKind;
    use crate::raft::state_machine::tests::*;
    use beryl_types::{CallId, ClientId};

    fn test_state() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine, InodeId) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, InodeAttrs::new(), MountId::new(1)))
            .unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        storage
            .put_mount(&MountEntry {
                mount_id: MountId::new(1),
                mount_prefix: crate::mount::ROOT_MOUNT_PREFIX.to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 1,
                namespace_owner_group_name: group_name("root"),
                root_inode_id: parent_inode_id,
            })
            .unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        (dir, storage, sm, parent_inode_id)
    }

    fn delete_command(
        name: &str,
        expected_inode_id: InodeId,
        lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
    ) -> Command {
        delete_path_command(vec![name.to_string()], expected_inode_id, lease_epoch, recursive)
    }

    fn delete_path_command(
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
    ) -> Command {
        Command::Delete {
            proposed_at_ms: 2,
            mount_id: MountId::new(1),
            expected_mount_epoch: 1,
            mount_root_inode_id: InodeId::new(10),
            relative_components,
            expected_inode_id,
            expected_file_lease_epoch: lease_epoch,
            recursive,
        }
    }

    fn create_file_command(
        operation_id: CreateFileOperationId,
        mount_root_inode_id: InodeId,
        components: &[&str],
    ) -> Command {
        let relative_components: Vec<_> = components.iter().map(|component| (*component).to_string()).collect();
        Command::CreateFile {
            proposed_at_ms: 1,
            operation_id,
            request_deadline_ms: 100,
            session_expires_at_ms: 100,
            normalized_path: format!("/{}", components.join("/")),
            mount_id: MountId::new(1),
            expected_mount_epoch: 1,
            mount_root_inode_id,
            relative_components,
            attrs: InodeAttrs::new(),
            layout: FileLayout::new(4096),
        }
    }

    fn create_file(sm: &AppRaftStateMachine, mount_root_inode_id: InodeId, components: &[&str]) -> InodeId {
        let command = create_file_command(
            CreateFileOperationId {
                client_id: ClientId::new(1),
                call_id: CallId::new(),
            },
            mount_root_inode_id,
            components,
        );
        expect_file_created(sm.apply(command).unwrap()).0
    }

    fn assert_delete_rejection_preserves_directory(
        storage: &RocksDBStorage,
        sm: &AppRaftStateMachine,
        parent_inode_id: InodeId,
        directory_inode_id: InodeId,
        command: Command,
        expected_rejection: ApplyRejectionKind,
    ) {
        expect_apply_rejection(sm.apply(command), expected_rejection);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(directory_inode_id)
        );
        assert!(storage.get_inode(directory_inode_id).unwrap().is_some());
        assert!(storage.get_detached_root(directory_inode_id).unwrap().is_none());
    }

    #[test]
    fn create_file_replays_one_durable_result_without_allocating_again() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let operation_id = CreateFileOperationId {
            client_id: ClientId::new(7),
            call_id: CallId::new(),
        };
        let command = create_file_command(operation_id, parent_inode_id, &["target"]);

        let first = expect_file_created(sm.apply(command.clone()).unwrap());
        let next_inode_id = storage.get_next_inode_id().unwrap();
        expect_apply_rejection(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 2,
                inode_id: first.0,
                expected_lease_epoch: LeaseEpoch::new(1),
            }),
            ApplyRejectionKind::Again,
        );

        let mut replay_command = command;
        let Command::CreateFile {
            proposed_at_ms,
            request_deadline_ms,
            session_expires_at_ms,
            ..
        } = &mut replay_command
        else {
            unreachable!("test helper must build CreateFile")
        };
        *proposed_at_ms = 2;
        *request_deadline_ms = 200;
        *session_expires_at_ms = 200;
        expect_apply_rejection(sm.apply(replay_command.clone()), ApplyRejectionKind::InvalidArgument);
        let Command::CreateFile {
            request_deadline_ms, ..
        } = &mut replay_command
        else {
            unreachable!("test helper must build CreateFile")
        };
        *request_deadline_ms = 100;
        let replay = expect_file_created(sm.apply(replay_command.clone()).unwrap());

        assert_eq!(replay, first);
        assert_eq!(storage.get_next_inode_id().unwrap(), next_inode_id);
        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(first.0));
        assert_eq!(
            storage.get_create_file_replay(operation_id).unwrap().unwrap().inode_id,
            first.0
        );
        assert_eq!(
            storage
                .get_create_file_replay_for_inode(first.0)
                .unwrap()
                .unwrap()
                .operation_id,
            operation_id
        );

        let mut mount = storage.get_mount(MountId::new(1)).unwrap().unwrap();
        mount.mount_epoch = 2;
        storage.put_mount(&mount).unwrap();
        expect_apply_rejection(sm.apply(replay_command.clone()), ApplyRejectionKind::Again);
        mount.mount_epoch = 1;
        storage.put_mount(&mount).unwrap();

        expect_apply_rejection(
            sm.apply(create_file_command(operation_id, parent_inode_id, &["other"])),
            ApplyRejectionKind::InvalidArgument,
        );
        assert_eq!(storage.get_dentry(parent_inode_id, "other").unwrap(), None);

        let Command::CreateFile { proposed_at_ms, .. } = &mut replay_command else {
            unreachable!("test helper must build CreateFile")
        };
        *proposed_at_ms = 100;
        expect_apply_rejection(sm.apply(replay_command), ApplyRejectionKind::Again);
        expect_write_lease_acquired(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 100,
                inode_id: first.0,
                expected_lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap(),
        );
    }

    #[test]
    fn delete_rejects_stale_mount_and_target_fencing_without_mutation() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["target".to_string()],
                attrs: InodeAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .0;
        let stale_commands = [
            Command::Delete {
                proposed_at_ms: 2,
                mount_id: MountId::new(1),
                expected_mount_epoch: 2,
                mount_root_inode_id: parent_inode_id,
                relative_components: vec!["target".to_string()],
                expected_inode_id: directory,
                expected_file_lease_epoch: None,
                recursive: true,
            },
            Command::Delete {
                proposed_at_ms: 2,
                mount_id: MountId::new(1),
                expected_mount_epoch: 1,
                mount_root_inode_id: InodeId::new(11),
                relative_components: vec!["target".to_string()],
                expected_inode_id: directory,
                expected_file_lease_epoch: None,
                recursive: true,
            },
            delete_command("target", InodeId::new(999), None, true),
        ];

        for command in stale_commands {
            assert_delete_rejection_preserves_directory(
                &storage,
                &sm,
                parent_inode_id,
                directory,
                command,
                ApplyRejectionKind::Again,
            );
        }
    }

    #[test]
    fn recursive_delete_atomically_detaches_root_without_removing_descendants() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["dir".to_string()],
                attrs: InodeAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .0;
        let file = create_file(&sm, parent_inode_id, &["dir", "file"]);

        expect_delete_applied(sm.apply(delete_command("dir", directory, None, true)).unwrap());

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(directory).unwrap().is_some());
        assert!(storage.get_inode(file).unwrap().is_some());
        assert!(storage.get_layout(file).is_ok());
        assert_eq!(
            storage.get_detached_root(directory).unwrap(),
            Some(DetachedRoot {
                mount_id: MountId::new(1),
                detached_at_ms: 2,
            })
        );
    }

    #[test]
    fn stale_delete_cannot_mutate_a_parent_after_it_is_detached() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inner = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["outer".to_string(), "inner".to_string()],
                attrs: InodeAttrs::new(),
                recursive: true,
            })
            .unwrap(),
        )
        .0;
        let outer = storage.get_dentry(parent_inode_id, "outer").unwrap().unwrap();
        let stale_inner_delete = delete_path_command(vec!["outer".to_string(), "inner".to_string()], inner, None, true);

        expect_delete_applied(sm.apply(delete_command("outer", outer, None, true)).unwrap());
        expect_apply_rejection(sm.apply(stale_inner_delete), ApplyRejectionKind::NotFound);

        assert_eq!(storage.get_dentry(outer, "inner").unwrap(), Some(inner));
        assert!(storage.get_detached_root(outer).unwrap().is_some());
        assert!(storage.get_detached_root(inner).unwrap().is_none());
    }

    #[test]
    fn recursive_delete_rejects_nested_mount_before_detach() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["dir".to_string()],
                attrs: InodeAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .0;
        storage
            .put_mount(&MountEntry {
                mount_id: MountId::new(2),
                mount_prefix: "/dir/nested".to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 2,
                namespace_owner_group_name: group_name("root"),
                root_inode_id: InodeId::new(200),
            })
            .unwrap();

        expect_apply_rejection(
            sm.apply(delete_command("dir", directory, None, true)),
            ApplyRejectionKind::CrossMountRename,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(directory));
        assert!(storage.get_detached_root(directory).unwrap().is_none());
    }

    #[test]
    fn delete_rejects_a_lease_acquired_after_preflight() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inode_id = create_file(&sm, parent_inode_id, &["target"]);

        expect_write_lease_acquired(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 100,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap(),
        );
        expect_apply_rejection(
            sm.apply(delete_command("target", inode_id, Some(LeaseEpoch::new(1)), false)),
            ApplyRejectionKind::Again,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(inode_id));
    }

    #[test]
    fn delete_that_linearizes_first_prevents_later_lease_acquisition() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inode_id = create_file(&sm, parent_inode_id, &["target"]);

        expect_delete_applied(
            sm.apply(delete_command("target", inode_id, Some(LeaseEpoch::new(1)), false))
                .unwrap(),
        );
        expect_apply_rejection(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 3,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(1),
            }),
            ApplyRejectionKind::NotFound,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), None);
        assert_eq!(storage.get_inode(inode_id).unwrap(), None);
    }
}
