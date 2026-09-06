// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Local metadata storage lifecycle.

use crate::config::MetadataConfig;
use crate::error::{MetadataError, MetadataResult};
use crate::mount::{DataIoPolicy, MountEntry, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::raft::{AppRaftNode, AppRaftStateMachine, ApplySuccess, Command, RocksDBStorage, StorageIdentity};
use crate::readiness::{wait_for_root_ready_with_inputs, RootReadinessGate, RootReadinessLogFields, RootReadyInputs};
use beryl_types::ids::{ClientId, MountId};
use beryl_types::{CallId, GroupName};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use uuid::Uuid;

const METADATA_MARKER_FILE: &str = "metadata.marker.json";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FormatState {
    Formatting,
    Ready,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataStorageMarker {
    pub state: FormatState,
    pub cluster_id: String,
    pub group_name: GroupName,
    pub node_id: u64,
    pub storage_uuid: String,
    pub format_version: u32,
    pub created_at_ms: u64,
    pub software_version: String,
    pub bootstrap_client_id: String,
    pub bootstrap_call_id: String,
    pub bootstrap_proposed_at_ms: u64,
}

pub fn metadata_marker_path(config: &MetadataConfig) -> PathBuf {
    config.storage_dir.join(METADATA_MARKER_FILE)
}

pub async fn format_metadata_storage(config: &MetadataConfig) -> MetadataResult<MetadataStorageMarker> {
    validate_format_config(config)?;
    let _format_lock = acquire_format_lock(config)?;
    let marker_path = metadata_marker_path(config);
    std::fs::create_dir_all(&config.storage_dir).map_err(|err| {
        MetadataError::Internal(format!(
            "failed to create beryl.metadata.storage.dir {}: {err}",
            config.storage_dir.display()
        ))
    })?;
    let mut marker = prepare_format_marker(config, &marker_path)?;
    write_marker(&marker_path, &marker)?;

    let storage = Arc::new(RocksDBStorage::create_for_format(&config.storage_dir)?);
    storage.bind_storage_identity(&storage_identity(&marker))?;
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref())?);
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
    let raft_node = Arc::new(
        AppRaftNode::new(
            config.raft.node_id,
            Arc::clone(&storage),
            state_machine,
            Arc::clone(&mount_table),
            &config.raft,
        )
        .await?,
    );

    raft_node.initialize_single_node(config.rpc_address()).await?;
    wait_for_single_node_leader(&raft_node, config.startup.root_readiness.timeout_ms).await?;

    let group_name = config.authority.group_name.clone();
    let bootstrap_result = raft_node
        .propose(Command::BootstrapNamespace {
            proposed_at_ms: marker.bootstrap_proposed_at_ms,
            group_name: group_name.clone(),
        })
        .await?;
    require_bootstrap_namespace_success(bootstrap_result, &group_name)?;
    wait_for_root_ready_with_inputs(RootReadyInputs {
        raft_node: Arc::clone(&raft_node),
        mount_table: Arc::clone(&mount_table),
        storage: Some(Arc::clone(&storage)),
        namespace_owner_group_name: group_name.clone(),
        readiness_gate: Arc::new(RootReadinessGate::new(None)),
        config: config.startup.root_readiness.clone(),
        metrics: None,
        log_fields: RootReadinessLogFields {
            cluster_id: config.cluster_id.clone(),
            group_name: group_name.to_string(),
            node_id: config.raft.node_id,
            storage_dir: config.storage_dir.display().to_string(),
        },
    })
    .await?;
    verify_root(&storage, &mount_table, &marker.group_name)?;
    raft_node.shutdown().await?;
    marker.state = FormatState::Ready;
    write_marker(&marker_path, &marker)?;
    Ok(marker)
}

pub async fn prepare_metadata_start(config: &MetadataConfig) -> MetadataResult<()> {
    validate_format_config(config)?;
    let marker_path = metadata_marker_path(config);
    if !marker_path.exists() {
        return Err(MetadataError::InvalidArgument(format!(
            "metadata storage is unformatted at {}; run `beryl --conf-dir <dir> format metadata` before start",
            config.storage_dir.display()
        )));
    }

    let marker = read_marker(&marker_path)?;
    validate_marker(config, &marker)?;
    if marker.state != FormatState::Ready {
        return Err(MetadataError::InvalidArgument(
            "metadata format is incomplete (marker state is formatting); rerun `beryl --conf-dir <dir> format metadata`, not `beryl metadata`"
                .to_string(),
        ));
    }
    let storage = RocksDBStorage::open_existing_for_start(&config.storage_dir)?;
    storage.validate_storage_identity(&storage_identity(&marker))?;
    validate_persisted_rpc_address(config, &storage)?;
    let mount_table = MountTable::load_from_storage(&storage)?;
    verify_root(&storage, &mount_table, &marker.group_name)?;
    storage.cleanup_unreferenced_generations()?;
    Ok(())
}

/// Keeps the configured public RPC address bound to the durable Raft identity.
///
/// Updating membership requires an ordered Raft transition. The current
/// single-node runtime does not implement that transition, so startup must
/// reject an address that differs from the one committed during format.
fn validate_persisted_rpc_address(config: &MetadataConfig, storage: &RocksDBStorage) -> MetadataResult<()> {
    let raft_state = storage.load_raft_state()?;
    let membership = raft_state.membership.membership();
    let mut nodes = membership.nodes();
    let Some((node_id, node)) = nodes.next() else {
        return Err(MetadataError::InvalidArgument(
            "formatted metadata storage has no Raft membership; reformat metadata storage".to_string(),
        ));
    };
    if nodes.next().is_some() || *node_id != config.raft.node_id {
        return Err(MetadataError::InvalidArgument(
            "formatted Metadata Raft membership is not the supported single local node".to_string(),
        ));
    }

    let configured = config.rpc_address();
    if node.address != configured {
        return Err(MetadataError::InvalidArgument(format!(
            "configured Metadata RPC address {configured} differs from format-time Raft membership address {}; start with the format-time beryl.metadata.host and beryl.metadata.rpc.port values",
            node.address
        )));
    }
    Ok(())
}

fn storage_identity(marker: &MetadataStorageMarker) -> StorageIdentity {
    StorageIdentity {
        storage_uuid: marker.storage_uuid.clone(),
        cluster_id: marker.cluster_id.clone(),
        group_name: marker.group_name.clone(),
        node_id: marker.node_id,
        bootstrap_client_id: marker.bootstrap_client_id.clone(),
        bootstrap_call_id: marker.bootstrap_call_id.clone(),
        bootstrap_proposed_at_ms: marker.bootstrap_proposed_at_ms,
    }
}

fn acquire_format_lock(config: &MetadataConfig) -> MetadataResult<std::fs::File> {
    let storage_name = config
        .storage_dir
        .file_name()
        .ok_or_else(|| MetadataError::InvalidArgument("beryl.metadata.storage.dir must name a directory".to_string()))?
        .to_string_lossy();
    let lock_path = config
        .storage_dir
        .with_file_name(format!(".{storage_name}.format.lock"));
    if let Some(parent) = lock_path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|error| {
            MetadataError::Internal(format!(
                "failed to create metadata storage parent {}: {error}",
                parent.display()
            ))
        })?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            MetadataError::Internal(format!(
                "failed to open metadata format lock {}: {error}",
                lock_path.display()
            ))
        })?;
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        MetadataError::ServiceUnavailable(format!(
            "metadata format is already running for {}: {error}",
            config.storage_dir.display()
        ))
    })?;
    Ok(file)
}

fn validate_format_config(config: &MetadataConfig) -> MetadataResult<()> {
    if config.cluster_id.trim().is_empty() {
        return Err(MetadataError::InvalidArgument(
            "beryl.cluster.id must not be empty".to_string(),
        ));
    }
    if config.raft.node_id == 0 {
        return Err(MetadataError::InvalidArgument(
            "internal Metadata Raft node id must be greater than zero".to_string(),
        ));
    }
    if config.storage_dir.as_os_str().is_empty() {
        return Err(MetadataError::InvalidArgument(
            "beryl.metadata.storage.dir must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_marker(config: &MetadataConfig, marker: &MetadataStorageMarker) -> MetadataResult<()> {
    if marker.format_version != FORMAT_VERSION {
        return Err(MetadataError::InvalidArgument(format!(
            "metadata marker mismatch: format_version={}, expected {}; reformat metadata storage",
            marker.format_version, FORMAT_VERSION
        )));
    }
    if marker.cluster_id != config.cluster_id {
        return Err(marker_mismatch("cluster_id", &marker.cluster_id, &config.cluster_id));
    }
    if marker.group_name != config.authority.group_name {
        return Err(marker_mismatch(
            "group_name",
            marker.group_name.as_str(),
            config.authority.group_name.as_str(),
        ));
    }
    if marker.node_id != config.raft.node_id {
        return Err(marker_mismatch(
            "node_id",
            &marker.node_id.to_string(),
            &config.raft.node_id.to_string(),
        ));
    }
    if marker.storage_uuid.trim().is_empty() {
        return Err(MetadataError::InvalidArgument(
            "metadata marker storage_uuid must not be empty".to_string(),
        ));
    }
    ClientId::parse(&marker.bootstrap_client_id).map_err(|error| {
        MetadataError::InvalidArgument(format!("metadata marker bootstrap_client_id is invalid: {error}"))
    })?;
    CallId::parse(&marker.bootstrap_call_id).map_err(|error| {
        MetadataError::InvalidArgument(format!("metadata marker bootstrap_call_id is invalid: {error}"))
    })?;
    if marker.bootstrap_proposed_at_ms == 0 {
        return Err(MetadataError::InvalidArgument(
            "metadata marker bootstrap_proposed_at_ms must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn prepare_format_marker(
    config: &MetadataConfig,
    marker_path: &std::path::Path,
) -> MetadataResult<MetadataStorageMarker> {
    recover_unpublished_formatting_marker(config, marker_path)?;
    if marker_path.exists() {
        let marker = read_marker(marker_path)?;
        validate_marker(config, &marker)?;
        return match marker.state {
            FormatState::Formatting => Ok(marker),
            FormatState::Ready => Err(MetadataError::AlreadyExists(format!(
                "metadata storage is already formatted at {}; start normally or remove the directory only if you intend to reset local metadata",
                config.storage_dir.display()
            ))),
        };
    }

    if storage_dir_has_entries(&config.storage_dir)? {
        return Err(marker_missing_error(config));
    }
    let proposed_at_ms = now_ms();
    Ok(MetadataStorageMarker {
        state: FormatState::Formatting,
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.clone(),
        node_id: config.raft.node_id,
        storage_uuid: Uuid::new_v4().to_string(),
        format_version: FORMAT_VERSION,
        created_at_ms: proposed_at_ms,
        software_version: env!("CARGO_PKG_VERSION").to_string(),
        bootstrap_client_id: ClientId::generate().as_raw().to_string(),
        bootstrap_call_id: CallId::new().to_string(),
        bootstrap_proposed_at_ms: proposed_at_ms,
    })
}

fn recover_unpublished_formatting_marker(config: &MetadataConfig, marker_path: &std::path::Path) -> MetadataResult<()> {
    if marker_path.exists() {
        return Ok(());
    }
    let temp_path = marker_path.with_extension("json.tmp");
    if !temp_path.exists() {
        return Ok(());
    }
    match read_marker(&temp_path) {
        Ok(marker) => {
            validate_marker(config, &marker)?;
            if marker.state != FormatState::Formatting {
                return Err(MetadataError::InvalidArgument(
                    "unpublished metadata marker is not in formatting state".to_string(),
                ));
            }
            std::fs::rename(&temp_path, marker_path).map_err(|error| {
                MetadataError::Internal(format!(
                    "failed to recover metadata marker {}: {error}",
                    marker_path.display()
                ))
            })?;
            sync_parent_directory(marker_path)
        }
        Err(error) => {
            let only_temp = std::fs::read_dir(&config.storage_dir)
                .map_err(|read_error| {
                    MetadataError::Internal(format!(
                        "failed to inspect metadata storage {}: {read_error}",
                        config.storage_dir.display()
                    ))
                })?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect::<Vec<_>>()
                == vec![temp_path.clone()];
            if !only_temp {
                return Err(error);
            }
            std::fs::remove_file(&temp_path).map_err(|remove_error| {
                MetadataError::Internal(format!(
                    "failed to remove incomplete metadata marker {}: {remove_error}",
                    temp_path.display()
                ))
            })?;
            sync_parent_directory(marker_path)
        }
    }
}

fn storage_dir_has_entries(path: &std::path::Path) -> MetadataResult<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if !path.is_dir() {
        return Err(MetadataError::InvalidArgument(format!(
            "beryl.metadata.storage.dir {} exists but is not a directory",
            path.display()
        )));
    }
    let mut entries = std::fs::read_dir(path).map_err(|err| {
        MetadataError::Internal(format!(
            "failed to read beryl.metadata.storage.dir {}: {err}",
            path.display()
        ))
    })?;
    Ok(entries
        .next()
        .transpose()
        .map_err(|err| {
            MetadataError::Internal(format!(
                "failed to read beryl.metadata.storage.dir {}: {err}",
                path.display()
            ))
        })?
        .is_some())
}

fn marker_missing_error(config: &MetadataConfig) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "beryl.metadata.storage.dir {} is non-empty but metadata marker missing at {}; refusing to attach existing files to a new metadata identity; clean the directory manually before formatting",
        config.storage_dir.display(),
        metadata_marker_path(config).display()
    ))
}

fn marker_mismatch(field: &str, actual: &str, expected: &str) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "metadata marker mismatch for {field}: marker={actual}, config={expected}"
    ))
}

fn read_marker(path: &std::path::Path) -> MetadataResult<MetadataStorageMarker> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| MetadataError::Internal(format!("failed to read metadata marker {}: {err}", path.display())))?;
    serde_json::from_str(&raw).map_err(|err| {
        MetadataError::InvalidArgument(format!(
            "metadata marker {} is malformed or unsupported: {err}; reformat metadata storage",
            path.display()
        ))
    })
}

fn write_marker(path: &std::path::Path, marker: &MetadataStorageMarker) -> MetadataResult<()> {
    let payload = serde_json::to_vec_pretty(marker)
        .map_err(|err| MetadataError::Internal(format!("failed to encode metadata marker: {err}")))?;
    let temp_path = path.with_extension("json.tmp");
    let mut file = std::fs::File::create(&temp_path).map_err(|err| {
        MetadataError::Internal(format!(
            "failed to create metadata marker {}: {err}",
            temp_path.display()
        ))
    })?;
    use std::io::Write;
    file.write_all(&payload).map_err(|err| {
        MetadataError::Internal(format!(
            "failed to write metadata marker {}: {err}",
            temp_path.display()
        ))
    })?;
    file.sync_all().map_err(|err| {
        MetadataError::Internal(format!("failed to sync metadata marker {}: {err}", temp_path.display()))
    })?;
    std::fs::rename(&temp_path, path).map_err(|err| {
        MetadataError::Internal(format!("failed to publish metadata marker {}: {err}", path.display()))
    })?;
    sync_parent_directory(path)
}

fn sync_parent_directory(path: &std::path::Path) -> MetadataResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| MetadataError::Internal("metadata marker has no parent directory".to_string()))?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|err| {
            MetadataError::Internal(format!("failed to sync metadata directory {}: {err}", parent.display()))
        })?;
    Ok(())
}

fn verify_root(storage: &RocksDBStorage, mount_table: &MountTable, group_name: &GroupName) -> MetadataResult<()> {
    let root = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .ok_or_else(|| MetadataError::ServiceUnavailable("root mount missing after metadata format".to_string()))?;
    if !root_mount_matches_bootstrap_contract(&root, group_name) {
        return Err(MetadataError::InvalidArgument(
            "root mount exists but violates root invariants".to_string(),
        ));
    }
    let inode = storage
        .get_inode(ROOT_INODE_ID)?
        .ok_or_else(|| MetadataError::ServiceUnavailable("root inode missing after metadata format".to_string()))?;
    if inode.inode_id != ROOT_INODE_ID
        || !inode.file_type().is_dir()
        || !matches!(inode.kind, crate::inode::InodeKind::Dir)
        || inode.mount_id != root.mount_id
    {
        return Err(MetadataError::InvalidArgument(
            "root inode exists but violates bootstrap invariants".to_string(),
        ));
    }
    Ok(())
}

/// Require the committed bootstrap response to identify the exact writable root.
fn require_bootstrap_namespace_success(result: ApplySuccess, group_name: &GroupName) -> MetadataResult<()> {
    let ApplySuccess::MountUpserted(root) = result else {
        return Err(MetadataError::Internal(format!(
            "BootstrapNamespace returned an unexpected success: {result:?}"
        )));
    };
    if !root_mount_matches_bootstrap_contract(&root, group_name) {
        return Err(MetadataError::Internal(format!(
            "BootstrapNamespace returned an invalid root mount: {root:?}"
        )));
    }
    Ok(())
}

/// Check the immutable identity and shape of the internal writable root mount.
fn root_mount_matches_bootstrap_contract(root: &MountEntry, group_name: &GroupName) -> bool {
    root.root_inode_id == ROOT_INODE_ID
        && root.mount_kind == MountKind::Internal
        && root.ufs_uri.is_none()
        && root.data_io_policy == DataIoPolicy::Allow
        && root.mount_id == MountId::new(1)
        && root.mount_prefix == ROOT_MOUNT_PREFIX
        && root.mount_epoch == 1
        && root.namespace_owner_group_name == *group_name
}

async fn wait_for_single_node_leader(raft_node: &AppRaftNode, timeout_ms: u64) -> MetadataResult<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    while tokio::time::Instant::now() < deadline {
        if raft_node.is_leader() {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }
    Err(MetadataError::ServiceUnavailable(
        "single-node raft did not become leader before readiness timeout".to_string(),
    ))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn format_lock_has_single_owner() {
        let dir = TempDir::new().unwrap();
        let config = MetadataConfig {
            storage_dir: dir.path().join("metadata"),
            ..MetadataConfig::default()
        };

        let first = acquire_format_lock(&config).unwrap();
        let error = acquire_format_lock(&config).expect_err("second formatter must be rejected");
        assert!(error.to_string().contains("already running"), "{error}");

        drop(first);
        acquire_format_lock(&config).expect("format lock must be released when its owner exits");
    }

    fn lifecycle_config(dir: &TempDir) -> MetadataConfig {
        MetadataConfig {
            storage_dir: dir.path().join("metadata"),
            ..MetadataConfig::default()
        }
    }

    #[tokio::test]
    async fn metadata_format_creates_root_namespace_through_raft_path() {
        let dir = TempDir::new().unwrap();
        let config = lifecycle_config(&dir);
        format_metadata_storage(&config).await.unwrap();

        prepare_metadata_start(&config).await.unwrap();

        let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
        let mount_table = MountTable::load_from_storage(&storage).unwrap();
        assert!(storage.get_inode(ROOT_INODE_ID).unwrap().is_some());
        let mounts = mount_table.list_mounts();
        assert_eq!(mounts.len(), 1);
        let root = &mounts[0];
        assert_eq!(root.mount_id, MountId::new(1));
        assert_eq!(root.mount_prefix, ROOT_MOUNT_PREFIX);
        assert_eq!(root.mount_kind, MountKind::Internal);
        assert_eq!(root.data_io_policy, DataIoPolicy::Allow);
        assert_eq!(root.namespace_owner_group_name, GroupName::parse("root").unwrap());
    }

    #[tokio::test]
    async fn metadata_start_rejects_root_without_data_io() {
        let dir = TempDir::new().unwrap();
        let config = lifecycle_config(&dir);
        format_metadata_storage(&config).await.unwrap();
        let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
        let mut root = MountTable::load_from_storage(&storage)
            .unwrap()
            .list_mounts()
            .into_iter()
            .find(|mount| mount.mount_prefix == ROOT_MOUNT_PREFIX)
            .expect("root mount after format");
        root.data_io_policy = DataIoPolicy::Forbid;
        storage.put_mount(&root).unwrap();
        drop(storage);

        let err = prepare_metadata_start(&config)
            .await
            .expect_err("root must be writable for data IO on start");
        let message = err.to_string();

        assert!(message.contains("root mount exists"), "{message}");
        assert!(message.contains("violates root invariants"), "{message}");
    }
}
