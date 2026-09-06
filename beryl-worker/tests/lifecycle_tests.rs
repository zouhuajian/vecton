// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_worker::config::WorkerConfig;
use beryl_worker::control::{prepare_worker_start, worker_storage_info_path, MetadataRegistrar};
use tempfile::TempDir;

fn worker_storage_info_temp_path_for_test(config: &WorkerConfig) -> std::path::PathBuf {
    let info_path = worker_storage_info_path(config);
    let file_name = info_path.file_name().unwrap().to_string_lossy();
    info_path.with_file_name(format!("{file_name}.tmp"))
}

fn prepare_start_descriptor(config: &WorkerConfig) -> Result<(), String> {
    let worker_id = prepare_worker_start(config).map_err(|err| err.to_string())?;
    MetadataRegistrar::descriptor_from_config(config, worker_id)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

fn write_config(dir: &TempDir, cluster_id: &str, _group_name: &str) -> std::path::PathBuf {
    let worker_dir = dir.path().join("worker");
    let store_dir = worker_dir.join("hdd0");
    let identity_path = worker_dir.join("worker.identity");
    let config_path = dir.path().join("worker.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
beryl.cluster.id: "{cluster_id}"
beryl.worker.host: "127.0.0.1"
beryl.worker.bind-host: "127.0.0.1"
beryl.worker.rpc.port: 19090
beryl.worker.http.port: 19091
beryl.worker.identity-file: "{}"
beryl.worker.storage.dirs:
  hdd0:
    path: "{}"
    tier: hdd
    capacity: 10GiB
beryl.worker.storage.reserved-space: 1GiB
beryl.worker.metadata.addresses:
  - "127.0.0.1:18080"
beryl.logging.format: compact
beryl.logging.output: stderr
beryl.logging.level: "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
"#,
            identity_path.display(),
            store_dir.display()
        ),
    )
    .unwrap();
    config_path
}

#[test]
fn worker_start_refuses_worker_id_mismatch_without_rewriting_storage() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    prepare_worker_start(&config).unwrap();
    let mut info: serde_json::Value =
        serde_json::from_slice(&std::fs::read(worker_storage_info_path(&config)).unwrap()).unwrap();
    let original_worker_id = info["worker_id"].as_u64().unwrap();
    info["worker_id"] = serde_json::Value::from(original_worker_id + 1);
    let info_payload = serde_json::to_vec_pretty(&info).unwrap();
    std::fs::write(worker_storage_info_path(&config), &info_payload).unwrap();
    let identity_before = std::fs::read(&config.identity_path).unwrap();

    let err = prepare_start_descriptor(&config).unwrap_err();

    assert!(err.contains("worker storage info mismatch"));
    assert!(err.contains("worker_id"));
    assert_eq!(std::fs::read(worker_storage_info_path(&config)).unwrap(), info_payload);
    assert_eq!(std::fs::read(&config.identity_path).unwrap(), identity_before);
}

#[test]
fn worker_start_refuses_partial_storage_info_temp_without_final_marker() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let info_path = worker_storage_info_path(&config);
    let temp_path = worker_storage_info_temp_path_for_test(&config);
    std::fs::create_dir_all(temp_path.parent().unwrap()).unwrap();
    std::fs::write(&temp_path, br#"{"cluster_id":"cluster-a""#).unwrap();

    let err = prepare_worker_start(&config).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("partial worker storage info"));
    assert!(message.contains(&temp_path.display().to_string()));
    assert!(temp_path.exists());
    assert!(!info_path.exists());
    assert!(!config.identity_path.exists());
}

#[test]
fn worker_start_rejects_non_current_storage_versions_without_rewriting_them() {
    for unsupported_version in [0, 1, 3, u32::MAX] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, "cluster-a", "root");
        let config = WorkerConfig::load(&config_path).unwrap();
        let worker_id = prepare_worker_start(&config).unwrap();
        let info_path = worker_storage_info_path(&config);
        let unsupported_info = format!(
            r#"{{
  "cluster_id": "cluster-a",
  "worker_id": {},
  "storage_uuid": "storage-a",
  "format_version": {},
  "created_at_ms": 1,
  "software_version": "test"
}}"#,
            worker_id.as_raw(),
            unsupported_version
        );
        std::fs::write(&info_path, unsupported_info.as_bytes()).unwrap();

        let error = prepare_worker_start(&config).unwrap_err();
        let message = error.to_string();

        assert!(
            message.contains(&format!("format_version={unsupported_version}")),
            "{message}"
        );
        assert!(message.contains("expected 2"), "{message}");
        assert_eq!(std::fs::read(&info_path).unwrap(), unsupported_info.as_bytes());
    }
}

#[test]
fn worker_start_refuses_non_empty_unknown_store_dirs_without_creating_identity() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let store_dir = &config.store.dirs["hdd0"].path;
    std::fs::create_dir_all(store_dir).unwrap();
    std::fs::write(store_dir.join("old-block-file"), b"stale").unwrap();

    let err = prepare_worker_start(&config).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("beryl.worker.storage.dirs"));
    assert!(message.contains("WorkerStorageInfo is missing"));
    assert!(!worker_storage_info_path(&config).exists());
    assert!(!config.identity_path.exists());
}
