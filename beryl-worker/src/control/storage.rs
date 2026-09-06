// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Local worker storage lifecycle.

use crate::config::WorkerConfig;
use crate::WorkerError;
use beryl_types::ids::WorkerId;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use super::identity::{resolve_existing_worker_id, resolve_worker_id};

const WORKER_STORAGE_INFO_FILE: &str = "worker.storage.json";
const WORKER_STORAGE_INFO_TEMP_SUFFIX: &str = ".tmp";
const FORMAT_VERSION: u32 = 2;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerStorageInfo {
    pub cluster_id: String,
    pub worker_id: u64,
    pub storage_uuid: String,
    pub format_version: u32,
    pub created_at_ms: u64,
    pub software_version: String,
}

pub fn worker_storage_info_path(config: &WorkerConfig) -> PathBuf {
    config
        .identity_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join(WORKER_STORAGE_INFO_FILE)
}

pub fn prepare_worker_start(config: &WorkerConfig) -> Result<WorkerId, WorkerError> {
    validate_start_config(config)?;
    let info_path = worker_storage_info_path(config);
    if info_path.exists() {
        let info = read_info(&info_path)?;
        let worker_id = validate_info(config, &info)?;
        return Ok(worker_id);
    }
    reject_partial_info_marker(&info_path)?;

    if store_dirs_have_entries(config)? {
        return Err(info_missing_error(config));
    }

    let worker_id = resolve_worker_id(config).map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;
    let info = WorkerStorageInfo {
        cluster_id: config.cluster_id.clone(),
        worker_id: worker_id.as_raw(),
        storage_uuid: Uuid::new_v4().to_string(),
        format_version: FORMAT_VERSION,
        created_at_ms: now_ms(),
        software_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    write_info(&info_path, &info)?;
    Ok(worker_id)
}

fn validate_start_config(config: &WorkerConfig) -> Result<(), WorkerError> {
    if config.cluster_id.trim().is_empty() {
        return Err(WorkerError::InvalidArgument(
            "beryl.cluster.id must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_info(config: &WorkerConfig, info: &WorkerStorageInfo) -> Result<WorkerId, WorkerError> {
    if info.format_version != FORMAT_VERSION {
        return Err(WorkerError::InvalidArgument(format!(
            "worker storage info mismatch: format_version={}, expected {}",
            info.format_version, FORMAT_VERSION
        )));
    }
    if info.cluster_id != config.cluster_id {
        return Err(info_mismatch("cluster_id", &info.cluster_id, &config.cluster_id));
    }
    let worker_id = resolve_existing_worker_id(config).map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;
    if info.worker_id != worker_id.as_raw() {
        return Err(info_mismatch(
            "worker_id",
            &info.worker_id.to_string(),
            &worker_id.as_raw().to_string(),
        ));
    }
    Ok(worker_id)
}

fn store_dirs_have_entries(config: &WorkerConfig) -> Result<bool, WorkerError> {
    for dir in config.store.dirs.values() {
        if storage_dir_has_entries(&dir.path)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn storage_dir_has_entries(path: &Path) -> Result<bool, WorkerError> {
    if !path.exists() {
        return Ok(false);
    }
    if !path.is_dir() {
        return Err(WorkerError::InvalidArgument(format!(
            "beryl.worker.storage.dirs path {} exists but is not a directory",
            path.display()
        )));
    }
    let mut entries = fs::read_dir(path).map_err(|err| {
        WorkerError::Internal(format!(
            "failed to read beryl.worker.storage.dirs path {}: {err}",
            path.display()
        ))
    })?;
    Ok(entries
        .next()
        .transpose()
        .map_err(|err| {
            WorkerError::Internal(format!(
                "failed to read beryl.worker.storage.dirs path {}: {err}",
                path.display()
            ))
        })?
        .is_some())
}

fn info_missing_error(config: &WorkerConfig) -> WorkerError {
    WorkerError::InvalidArgument(format!(
        "beryl.worker.storage.dirs contains non-empty paths but WorkerStorageInfo is missing at {}; refusing to take over unknown local data",
        worker_storage_info_path(config).display()
    ))
}

fn info_mismatch(field: &str, actual: &str, expected: &str) -> WorkerError {
    WorkerError::InvalidArgument(format!(
        "worker storage info mismatch for {field}: info={actual}, config={expected}"
    ))
}

fn read_info(path: &Path) -> Result<WorkerStorageInfo, WorkerError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        WorkerError::Internal(format!("failed to read worker storage info {}: {err}", path.display()))
    })?;
    serde_json::from_str(&raw).map_err(|err| {
        WorkerError::InvalidArgument(format!(
            "worker storage info {} is malformed or unsupported: {err}; clean worker storage before starting",
            path.display()
        ))
    })
}

fn write_info(path: &Path, info: &WorkerStorageInfo) -> Result<(), WorkerError> {
    let payload = serde_json::to_vec_pretty(info)
        .map_err(|err| WorkerError::Internal(format!("failed to encode worker storage info: {err}")))?;
    let parent = info_parent_dir(path);
    fs::create_dir_all(parent).map_err(|err| {
        WorkerError::Internal(format!(
            "failed to create worker storage info parent {}: {err}",
            parent.display()
        ))
    })?;
    let temp_path = worker_storage_info_temp_path(path)?;
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(|err| {
                WorkerError::Internal(format!(
                    "failed to create worker storage info temp marker {}: {err}",
                    temp_path.display()
                ))
            })?;
        file.write_all(&payload).map_err(|err| {
            WorkerError::Internal(format!(
                "failed to write worker storage info temp marker {}: {err}",
                temp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            WorkerError::Internal(format!(
                "failed to fsync worker storage info temp marker {}: {err}",
                temp_path.display()
            ))
        })?;
    }
    fs::rename(&temp_path, path).map_err(|err| {
        WorkerError::Internal(format!(
            "failed to rename worker storage info temp marker {} to {}: {err}",
            temp_path.display(),
            path.display()
        ))
    })?;
    sync_info_parent_dir(parent)
}

fn reject_partial_info_marker(path: &Path) -> Result<(), WorkerError> {
    let temp_path = worker_storage_info_temp_path(path)?;
    if temp_path.try_exists().map_err(|err| {
        WorkerError::Internal(format!(
            "failed to inspect worker storage info temp marker {}: {err}",
            temp_path.display()
        ))
    })? {
        return Err(WorkerError::InvalidArgument(format!(
            "partial worker storage info temp marker {} exists without final marker {}; refusing to start",
            temp_path.display(),
            path.display()
        )));
    }
    Ok(())
}

fn worker_storage_info_temp_path(path: &Path) -> Result<PathBuf, WorkerError> {
    let file_name = path.file_name().ok_or_else(|| {
        WorkerError::InvalidArgument(format!("worker storage info path {} has no file name", path.display()))
    })?;
    let mut temp_name = OsString::from(file_name);
    temp_name.push(WORKER_STORAGE_INFO_TEMP_SUFFIX);
    Ok(path.with_file_name(temp_name))
}

fn info_parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sync_info_parent_dir(parent: &Path) -> Result<(), WorkerError> {
    File::open(parent).and_then(|file| file.sync_all()).map_err(|err| {
        WorkerError::Internal(format!(
            "failed to fsync worker storage info parent {}: {err}",
            parent.display()
        ))
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
