// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB-backed Raft state machine store (openraft `RaftStateMachine` + snapshot I/O).

use super::durable_raft_write_options;
use crate::error::{MetadataError, MetadataResult};
use crate::mount::{MountEntry, MountTable, MountTableState};
use crate::observe;
use crate::raft::response::{ApplySuccess, RaftApplyResult};
use crate::raft::storage::snapshot::{
    decode_snapshot, is_node_local_meta_key, snapshot_file_in_use, IncomingSnapshotToken, SnapshotCodecError,
    SnapshotFile, SnapshotIdentity, SnapshotInstallTracker, SnapshotWriter,
};
use crate::raft::storage::{RocksDBStorage, StorageIdentity, STATE_CFS};
use crate::raft::types::{from_openraft_log_id, AppMetadataRaftState, MetadataNode, MetadataRaftTypeConfig};
use crate::raft::{AppRaftStateMachine, MetadataReadView};
use crate::state::RouteEpoch;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, SnapshotSignature};
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use parking_lot::{Mutex, RwLock};
use rocksdb::{
    ColumnFamily, Direction, IteratorMode, ReadOptions, Snapshot as DbSnapshot, WriteBatch, WriteOptions, DB,
};
use sha2::{Digest, Sha256};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Error, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinError;
use tracing::info;
use uuid::Uuid;

const SNAPSHOT_BATCH_BYTES: usize = 2 * 1024 * 1024;
const CF_META: &str = "meta";
const CF_MOUNTS: &str = "mounts";
const CF_RAFT_LOG: &str = "raft_log";
const CF_RAFT_STATE: &str = "raft_state";
const CF_RAFT_SNAPSHOT: &str = "raft_snapshot";
const STORAGE_IDENTITY_KEY: &[u8] = b"storage_identity";
const RAFT_STATE_KEY: &[u8] = b"raft_state";
const SNAPSHOT_META_KEY: &[u8] = b"snapshot_meta";

/// Bridges openraft state machine callbacks to the application state machine and RocksDB.
pub(crate) struct StateMachineStorage {
    storage: Arc<RocksDBStorage>,
    state_machine: Arc<AppRaftStateMachine>,
    state: Arc<RwLock<AppMetadataRaftState>>,
    read_view: Arc<MetadataReadView>,
    snapshot_install: Arc<SnapshotInstallTracker>,
}

impl StateMachineStorage {
    pub(crate) fn new_with_tracker(
        storage: Arc<RocksDBStorage>,
        state_machine: Arc<AppRaftStateMachine>,
        state: Arc<RwLock<AppMetadataRaftState>>,
        read_view: Arc<MetadataReadView>,
        snapshot_install: Arc<SnapshotInstallTracker>,
    ) -> MetadataResult<Self> {
        clean_stale_snapshot_tmp(&storage)?;
        let current_snapshot = current_snapshot_path(&storage)?;
        cleanup_obsolete_snapshot_files(&storage, current_snapshot.as_deref())?;

        Ok(Self {
            storage,
            state_machine,
            state,
            read_view,
            snapshot_install,
        })
    }
}

impl RaftStateMachine<MetadataRaftTypeConfig> for StateMachineStorage {
    type SnapshotBuilder = AppSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, MetadataNode>), StorageError<u64>> {
        let state = self.state.read();
        let last_applied = state.last_applied_log_id;
        let membership = state.membership.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftApplyResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MetadataRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut results = Vec::new();

        for entry in &entries {
            let log_id = *entry.get_log_id();
            let apply_started = Instant::now();
            let _generation = self.storage.pin_generation().map_err(|error| StorageError::IO {
                source: StorageIOError::<u64>::apply(log_id, AnyError::new(&error)),
            })?;

            match entry.payload {
                EntryPayload::Normal(ref cmd) => {
                    let mut current = self.state.write();
                    let mut next = current.clone();
                    next.last_applied_log_id = Some(log_id);
                    let applied = match self.state_machine.apply_committed(cmd.clone(), &next) {
                        Ok(result) => result,
                        Err(e) => {
                            observe::record_raft_apply(
                                "error",
                                observe::metadata_error_kind(e.as_inner()),
                                apply_started.elapsed().as_secs_f64(),
                            );
                            return Err(StorageError::IO {
                                source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                            });
                        }
                    };
                    if let Err(e) = self.read_view.publish_routing(applied.routing_delta) {
                        observe::record_raft_apply(
                            "error",
                            observe::metadata_error_kind(&e),
                            apply_started.elapsed().as_secs_f64(),
                        );
                        return Err(StorageError::IO {
                            source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                        });
                    }
                    *current = next;

                    results.push(applied.response);
                    observe::record_raft_apply("ok", "none", apply_started.elapsed().as_secs_f64());
                }
                EntryPayload::Membership(ref membership) => {
                    let mut current = self.state.write();
                    let mut next = current.clone();
                    next.membership = StoredMembership::new(Some(log_id), membership.clone());
                    next.last_applied_log_id = Some(log_id);
                    if let Err(e) = self.storage.commit_applied_state(&next) {
                        observe::record_raft_apply("error", "storage", apply_started.elapsed().as_secs_f64());
                        return Err(StorageError::IO {
                            source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                        });
                    }
                    *current = next;

                    results.push(Ok(ApplySuccess::RaftEntryApplied));
                    observe::record_raft_apply("ok", "none", apply_started.elapsed().as_secs_f64());
                }
                EntryPayload::Blank => {
                    let mut current = self.state.write();
                    let mut next = current.clone();
                    next.last_applied_log_id = Some(log_id);
                    if let Err(e) = self.storage.commit_applied_state(&next) {
                        observe::record_raft_apply("error", "storage", apply_started.elapsed().as_secs_f64());
                        return Err(StorageError::IO {
                            source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                        });
                    }
                    *current = next;

                    results.push(Ok(ApplySuccess::RaftEntryApplied));
                    observe::record_raft_apply("ok", "none", apply_started.elapsed().as_secs_f64());
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AppSnapshotBuilder {
            storage: Arc::clone(&self.storage),
            _state_machine: Arc::clone(&self.state_machine),
            _state: Arc::clone(&self.state),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<MetadataRaftTypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>> {
        let tmp_path = temp_snapshot_path(&self.storage, &format!("incoming-{}", Uuid::new_v4()));
        let token = self.snapshot_install.begin().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;
        let file = SnapshotFile::create_incoming(tmp_path, token)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
            })?;
        Ok(Box::new(file))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, MetadataNode>,
        snapshot: Box<<MetadataRaftTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        let started = Instant::now();
        let snapshot_path = snapshot.path().to_path_buf();
        let (std_file, incoming_token) = match snapshot.into_std_with_token().await {
            Ok(received) => received,
            Err(error) => {
                observe::record_raft_snapshot("install", "receive", "error", 0, started.elapsed().as_secs_f64());
                return Err(StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&error)),
                });
            }
        };
        let storage = Arc::clone(&self.storage);
        let read_view = Arc::clone(&self.read_view);
        let meta = meta.clone();
        let signature = meta.signature();
        let installed = tokio::task::spawn_blocking(move || {
            install_snapshot_generation(&storage, &read_view, &meta, snapshot_path, std_file, incoming_token)
        })
        .await;
        let installed = match installed {
            Ok(Ok(installed)) => installed,
            Ok(Err(error)) => {
                observe::record_raft_snapshot("install", "generation", "error", 0, started.elapsed().as_secs_f64());
                return Err(snapshot_write_error(Some(signature), &error));
            }
            Err(error) => {
                observe::record_raft_snapshot("install", "join", "error", 0, started.elapsed().as_secs_f64());
                return Err(snapshot_join_error(Some(signature), error));
            }
        };
        observe::record_raft_snapshot(
            "install",
            "complete",
            "ok",
            installed.bytes,
            started.elapsed().as_secs_f64(),
        );
        info!(
            snapshot_id = %installed.meta.snapshot_id,
            last_log = ?installed.meta.last_log_id,
            bytes = installed.bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "Installed snapshot"
        );

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<MetadataRaftTypeConfig>>, StorageError<u64>> {
        let meta_data = match self.storage.get_snapshot_meta().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
        })? {
            Some(m) => m,
            None => return Ok(None),
        };

        let meta: SnapshotMeta<u64, MetadataNode> =
            serde_json::from_slice(&meta_data).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
            })?;

        let path = snapshot_file_path(&self.storage, &meta.snapshot_id);
        if !path.exists() {
            let error = MetadataError::Internal(format!(
                "snapshot metadata {} references missing file {}",
                meta.snapshot_id,
                path.display()
            ));
            return Err(snapshot_read_error(Some(meta.signature()), &error));
        }

        let file = SnapshotFile::open_read(path).await.map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;

        Ok(Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(file),
        }))
    }
}

/// Snapshot builder for Raft.
pub(crate) struct AppSnapshotBuilder {
    storage: Arc<RocksDBStorage>,
    _state_machine: Arc<AppRaftStateMachine>,
    _state: Arc<RwLock<AppMetadataRaftState>>,
}

impl RaftSnapshotBuilder<MetadataRaftTypeConfig> for AppSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetadataRaftTypeConfig>, StorageError<u64>> {
        let started = Instant::now();
        let storage = Arc::clone(&self.storage);
        let built = tokio::task::spawn_blocking(move || build_snapshot_generation(&storage)).await;
        let built = match built {
            Ok(Ok(built)) => built,
            Ok(Err(error)) => {
                observe::record_raft_snapshot("build", "generation", "error", 0, started.elapsed().as_secs_f64());
                return Err(snapshot_write_error(None, &error));
            }
            Err(error) => {
                observe::record_raft_snapshot("build", "join", "error", 0, started.elapsed().as_secs_f64());
                return Err(snapshot_join_error(None, error));
            }
        };
        observe::record_raft_snapshot("build", "complete", "ok", built.bytes, started.elapsed().as_secs_f64());
        info!(
            snapshot_id = %built.meta.snapshot_id,
            last_log = ?built.meta.last_log_id,
            bytes = built.bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "Built snapshot"
        );

        let file_for_send = SnapshotFile::open_read(built.path)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_snapshot(Some(built.meta.signature()), AnyError::new(&e)),
            })?;

        Ok(Snapshot {
            meta: built.meta,
            snapshot: Box::new(file_for_send),
        })
    }
}

fn snapshot_file_path(storage: &RocksDBStorage, snapshot_id: &str) -> PathBuf {
    storage
        .snapshot_dir()
        .join(format!("snapshot-{}.snap", snapshot_id_hash(snapshot_id)))
}

fn temp_snapshot_path(storage: &RocksDBStorage, snapshot_id: &str) -> PathBuf {
    storage
        .snapshot_dir()
        .join(format!("snapshot-{}.snap.tmp", snapshot_id_hash(snapshot_id)))
}

fn snapshot_id_hash(snapshot_id: &str) -> String {
    let digest = Sha256::digest(snapshot_id.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn clean_stale_snapshot_tmp(storage: &RocksDBStorage) -> MetadataResult<()> {
    let dir = storage.snapshot_dir();
    if !dir.exists() {
        return Ok(());
    }

    for entry in
        fs::read_dir(dir).map_err(|e| MetadataError::Internal(format!("Failed to list snapshot dir: {}", e)))?
    {
        let entry = entry.map_err(|e| MetadataError::Internal(format!("{}", e)))?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(".snap.tmp") {
                fs::remove_file(&path).map_err(|error| {
                    MetadataError::Internal(format!(
                        "failed to remove stale snapshot temp file {}: {error}",
                        path.display()
                    ))
                })?;
                observe::record_raft_storage_cleanup("stale_snapshot", 1);
            }
        }
    }
    Ok(())
}

fn format_snapshot_id(last_log_id: Option<LogId<u64>>) -> String {
    let suffix = Uuid::new_v4();
    match last_log_id {
        Some(log_id) => format!("{}-{}-{}", log_id.leader_id.term, log_id.index, suffix),
        None => format!("bootstrap-{}", suffix),
    }
}

struct SnapshotArtifact {
    meta: SnapshotMeta<u64, MetadataNode>,
    path: PathBuf,
    bytes: u64,
}

fn build_snapshot_generation(storage: &RocksDBStorage) -> MetadataResult<SnapshotArtifact> {
    storage.with_pinned_snapshot(|db, snapshot| {
        let raft_state = load_raft_state_from_snapshot(db, snapshot)?;
        let storage_identity = load_storage_identity_from_snapshot(db, snapshot)?;
        let snapshot_id = format_snapshot_id(raft_state.last_applied_log_id);
        let meta = SnapshotMeta {
            last_log_id: raft_state.last_applied_log_id,
            last_membership: raft_state.membership.clone(),
            snapshot_id,
        };
        let identity =
            SnapshotIdentity::current(storage_identity.group_name, meta.last_log_id.map(from_openraft_log_id));
        let temporary_path = temp_snapshot_path(storage, &meta.snapshot_id);
        let final_path = snapshot_file_path(storage, &meta.snapshot_id);

        let result = write_snapshot_v2(db, snapshot, &identity, &temporary_path).and_then(|()| {
            if final_path.exists() {
                return Err(MetadataError::Internal(format!(
                    "snapshot path already exists: {}",
                    final_path.display()
                )));
            }
            fs::rename(&temporary_path, &final_path).map_err(|error| {
                MetadataError::Internal(format!("publish snapshot {}: {error}", final_path.display()))
            })?;
            sync_directory(storage.snapshot_dir().as_path())?;
            persist_snapshot_meta(db, &meta)?;
            cleanup_obsolete_snapshot_files(storage, Some(&final_path))?;
            let bytes = fs::metadata(&final_path)
                .map_err(|error| MetadataError::Internal(format!("stat snapshot {}: {error}", final_path.display())))?
                .len();
            Ok(SnapshotArtifact {
                meta,
                path: final_path,
                bytes,
            })
        });
        if result.is_err() && temporary_path.exists() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    })
}

fn write_snapshot_v2(
    db: &DB,
    snapshot: &DbSnapshot<'_>,
    identity: &SnapshotIdentity,
    path: &Path,
) -> MetadataResult<()> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| MetadataError::Internal(format!("create snapshot {}: {error}", path.display())))?;
    let mut writer = SnapshotWriter::new(BufWriter::new(file), identity).map_err(local_codec_error)?;
    for cf_name in STATE_CFS {
        let cf = required_cf(db, cf_name)?;
        writer.start_column_family(cf_name).map_err(local_codec_error)?;
        for item in snapshot.iterator_cf_opt(cf, ReadOptions::default(), IteratorMode::Start) {
            let (key, value) = item
                .map_err(|error| MetadataError::Internal(format!("read {cf_name} while building snapshot: {error}")))?;
            if !is_node_local_meta_key(cf_name, &key) {
                writer.write_record(&key, &value).map_err(local_codec_error)?;
            }
        }
        writer.end_column_family().map_err(local_codec_error)?;
    }
    let buffer = writer.finish().map_err(local_codec_error)?;
    let file = buffer
        .into_inner()
        .map_err(|error| MetadataError::Internal(format!("flush snapshot {}: {error}", path.display())))?;
    file.sync_all()
        .map_err(|error| MetadataError::Internal(format!("sync snapshot {}: {error}", path.display())))
}

fn install_snapshot_generation(
    storage: &RocksDBStorage,
    read_view: &MetadataReadView,
    meta: &SnapshotMeta<u64, MetadataNode>,
    incoming_path: PathBuf,
    mut incoming_file: File,
    incoming_token: Option<IncomingSnapshotToken>,
) -> MetadataResult<SnapshotArtifact> {
    let boundary = meta.last_log_id.ok_or_else(|| {
        MetadataError::InvalidArgument("incoming metadata snapshot has no last applied log id".to_string())
    })?;
    incoming_file
        .sync_all()
        .map_err(|error| MetadataError::Internal(format!("sync incoming snapshot: {error}")))?;
    incoming_file
        .seek(SeekFrom::Start(0))
        .map_err(|error| MetadataError::Internal(format!("rewind incoming snapshot: {error}")))?;

    let local_identity = storage.storage_identity()?;
    let expected = SnapshotIdentity::current(local_identity.group_name.clone(), Some(from_openraft_log_id(boundary)));
    let staged = storage.create_staged_generation()?;
    decode_into_staged(staged.db(), &mut incoming_file, &expected)?;
    super::schema::validate_detached_root_records(staged.db())?;
    let routing = load_mount_replacement(staged.db())?;
    let route_epoch = load_route_epoch(staged.db())?;
    drop(incoming_file);

    let final_path = snapshot_file_path(storage, &meta.snapshot_id);
    publish_received_snapshot_file(&incoming_path, &final_path)?;
    let bytes = fs::metadata(&final_path)
        .map_err(|error| MetadataError::Internal(format!("stat snapshot {}: {error}", final_path.display())))?
        .len();

    let published_state = Arc::new(Mutex::new(None::<AppMetadataRaftState>));
    let before_state = Arc::clone(&published_state);
    let after_state = Arc::clone(&published_state);
    let meta_for_db = meta.clone();
    let publish_result = storage.publish_staged_generation_with(
        staged,
        move |old_db, staged_db| {
            let old_state = load_raft_state(old_db)?;
            if old_state
                .last_applied_log_id
                .is_some_and(|current| current.index > boundary.index)
                || old_state
                    .last_purged_log_id
                    .is_some_and(|current| current.index > boundary.index)
            {
                return Err(MetadataError::InvalidArgument(format!(
                    "refusing stale snapshot at index {} over local raft state {:?}",
                    boundary.index, old_state.last_applied_log_id
                )));
            }
            copy_storage_identity(old_db, staged_db, &local_identity)?;
            copy_log_suffix(old_db, staged_db, boundary.index)?;
            let mut next = old_state;
            next.last_applied_log_id = Some(boundary);
            next.last_purged_log_id = Some(boundary);
            next.membership = meta_for_db.last_membership.clone();
            if next.committed.is_none_or(|committed| committed.index < boundary.index) {
                next.committed = Some(boundary);
            }
            persist_installed_protocol_state(staged_db, &next, &meta_for_db)?;
            *before_state.lock() = Some(next);
            Ok(())
        },
        move |new_db| {
            let mut next = after_state
                .lock()
                .take()
                .ok_or_else(|| MetadataError::Internal("snapshot state was not prepared before switch".to_string()))?;
            if let Some(token) = incoming_token {
                if let Some(deferred) = token.complete()? {
                    if deferred.index > boundary.index {
                        return Err(MetadataError::Internal(format!(
                            "deferred purge index {} exceeds installed snapshot boundary {}",
                            deferred.index, boundary.index
                        )));
                    }
                    if next
                        .last_purged_log_id
                        .is_none_or(|current| current.index < deferred.index)
                    {
                        next.last_purged_log_id = Some(deferred);
                        purge_logs_and_persist_state(new_db, deferred.index, &next)?;
                    }
                }
            }
            read_view.install_generation(routing, route_epoch, next);
            Ok(())
        },
    );
    if let Err(error) = publish_result {
        if let Ok(current) = current_snapshot_path(storage) {
            let _ = cleanup_obsolete_snapshot_files(storage, current.as_deref());
        }
        return Err(error);
    }
    if let Err(error) = storage.cleanup_retired_generations() {
        tracing::warn!(error = %error, "deferred cleanup of retired metadata generation");
    }
    if let Err(error) = cleanup_obsolete_snapshot_files(storage, Some(&final_path)) {
        tracing::warn!(error = %error, "deferred cleanup of obsolete metadata snapshots");
    }

    Ok(SnapshotArtifact {
        meta: meta.clone(),
        path: final_path,
        bytes,
    })
}

fn decode_into_staged(db: &DB, reader: impl Read, expected: &SnapshotIdentity) -> MetadataResult<()> {
    let mut batch = WriteBatch::default();
    decode_snapshot(BufReader::new(reader), expected, |cf_name, key, value| {
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| SnapshotCodecError::Invalid(format!("staged generation is missing {cf_name}")))?;
        batch.put_cf(cf, key, value);
        if batch.size_in_bytes() >= SNAPSHOT_BATCH_BYTES {
            db.write(std::mem::take(&mut batch)).map_err(|error| {
                SnapshotCodecError::Io(Error::other(format!("write staged snapshot batch: {error}")))
            })?;
        }
        Ok(())
    })
    .map_err(incoming_codec_error)?;
    if !batch.is_empty() {
        db.write(batch)
            .map_err(|error| MetadataError::Internal(format!("write final staged snapshot batch: {error}")))?;
    }
    Ok(())
}

fn load_mount_replacement(db: &DB) -> MetadataResult<MountTableState> {
    let cf = required_cf(db, CF_MOUNTS)?;
    let mut mounts = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (_, value) = item.map_err(|error| MetadataError::Internal(format!("read staged mounts: {error}")))?;
        let entry: MountEntry = bincode::serde::decode_from_slice(&value, bincode::config::standard())
            .map_err(|error| MetadataError::InvalidArgument(format!("invalid mount in snapshot: {error}")))?
            .0;
        mounts.push(entry);
    }
    MountTable::build_replacement(mounts)
}

fn load_route_epoch(db: &DB) -> MetadataResult<RouteEpoch> {
    let cf = required_cf(db, CF_META)?;
    let bytes = db
        .get_cf(cf, b"route_epoch")
        .map_err(|error| MetadataError::Internal(format!("read staged route epoch: {error}")))?
        .ok_or_else(|| MetadataError::InvalidArgument("snapshot route epoch is missing".to_string()))?;
    let epoch: u64 = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid route epoch in snapshot: {error}")))?
        .0;
    if epoch == 0 {
        return Err(MetadataError::InvalidArgument(
            "snapshot route epoch must be non-zero".to_string(),
        ));
    }
    Ok(RouteEpoch::new(epoch))
}

fn load_raft_state_from_snapshot(db: &DB, snapshot: &DbSnapshot<'_>) -> MetadataResult<AppMetadataRaftState> {
    let cf = required_cf(db, CF_RAFT_STATE)?;
    match snapshot
        .get_cf_opt(cf, RAFT_STATE_KEY, ReadOptions::default())
        .map_err(|error| MetadataError::Internal(format!("read raft state for snapshot: {error}")))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| MetadataError::Internal(format!("decode raft state for snapshot: {error}"))),
        None => Ok(AppMetadataRaftState::default()),
    }
}

fn load_storage_identity_from_snapshot(db: &DB, snapshot: &DbSnapshot<'_>) -> MetadataResult<StorageIdentity> {
    let cf = required_cf(db, CF_META)?;
    let bytes = snapshot
        .get_cf_opt(cf, STORAGE_IDENTITY_KEY, ReadOptions::default())
        .map_err(|error| MetadataError::Internal(format!("read storage identity for snapshot: {error}")))?
        .ok_or_else(|| MetadataError::InvalidArgument("storage identity is missing".to_string()))?;
    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map(|decoded: (StorageIdentity, usize)| decoded.0)
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid storage identity: {error}")))
}

fn load_raft_state(db: &DB) -> MetadataResult<AppMetadataRaftState> {
    let cf = required_cf(db, CF_RAFT_STATE)?;
    match db
        .get_cf(cf, RAFT_STATE_KEY)
        .map_err(|error| MetadataError::Internal(format!("read local raft state: {error}")))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| MetadataError::Internal(format!("decode local raft state: {error}"))),
        None => Ok(AppMetadataRaftState::default()),
    }
}

fn copy_storage_identity(old_db: &DB, staged_db: &DB, expected: &StorageIdentity) -> MetadataResult<()> {
    let old_meta = required_cf(old_db, CF_META)?;
    let staged_meta = required_cf(staged_db, CF_META)?;
    let bytes = old_db
        .get_cf(old_meta, STORAGE_IDENTITY_KEY)
        .map_err(|error| MetadataError::Internal(format!("read local storage identity: {error}")))?
        .ok_or_else(|| MetadataError::InvalidArgument("local storage identity is missing".to_string()))?;
    let actual: StorageIdentity = bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid local storage identity: {error}")))?
        .0;
    if &actual != expected {
        return Err(MetadataError::Internal(
            "local storage identity changed during snapshot installation".to_string(),
        ));
    }
    staged_db
        .put_cf(staged_meta, STORAGE_IDENTITY_KEY, bytes)
        .map_err(|error| MetadataError::Internal(format!("copy local storage identity: {error}")))
}

fn copy_log_suffix(old_db: &DB, staged_db: &DB, boundary_index: u64) -> MetadataResult<()> {
    let old_cf = required_cf(old_db, CF_RAFT_LOG)?;
    let staged_cf = required_cf(staged_db, CF_RAFT_LOG)?;
    let start = boundary_index
        .checked_add(1)
        .ok_or_else(|| MetadataError::Internal("snapshot boundary index overflow".to_string()))?;
    let start_key = format!("{start:020}");
    let mut expected = start;
    let mut batch = WriteBatch::default();
    for item in old_db.iterator_cf(old_cf, IteratorMode::From(start_key.as_bytes(), Direction::Forward)) {
        let (key, value) = item.map_err(|error| MetadataError::Internal(format!("copy raft log suffix: {error}")))?;
        let index = std::str::from_utf8(&key)
            .map_err(|error| MetadataError::Internal(format!("invalid raft log key: {error}")))?
            .parse::<u64>()
            .map_err(|error| MetadataError::Internal(format!("invalid raft log index: {error}")))?;
        if index != expected {
            return Err(MetadataError::Internal(format!(
                "raft log suffix is not contiguous: expected {expected}, found {index}"
            )));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| MetadataError::Internal("raft log index overflow".to_string()))?;
        batch.put_cf(staged_cf, key, value);
    }
    if !batch.is_empty() {
        staged_db
            .write(batch)
            .map_err(|error| MetadataError::Internal(format!("write raft log suffix: {error}")))?;
    }
    Ok(())
}

fn persist_installed_protocol_state(
    db: &DB,
    state: &AppMetadataRaftState,
    meta: &SnapshotMeta<u64, MetadataNode>,
) -> MetadataResult<()> {
    let state_cf = required_cf(db, CF_RAFT_STATE)?;
    let snapshot_cf = required_cf(db, CF_RAFT_SNAPSHOT)?;
    let state_bytes = serde_json::to_vec(state)
        .map_err(|error| MetadataError::Internal(format!("encode installed raft state: {error}")))?;
    let meta_bytes = serde_json::to_vec(meta)
        .map_err(|error| MetadataError::Internal(format!("encode installed snapshot meta: {error}")))?;
    let mut batch = WriteBatch::default();
    batch.put_cf(state_cf, RAFT_STATE_KEY, state_bytes);
    batch.put_cf(snapshot_cf, SNAPSHOT_META_KEY, meta_bytes);
    db.write_opt(batch, &durable_write_options())
        .map_err(|error| MetadataError::Internal(format!("persist installed raft state: {error}")))
}

fn purge_logs_and_persist_state(db: &DB, end_index: u64, state: &AppMetadataRaftState) -> MetadataResult<()> {
    let log_cf = required_cf(db, CF_RAFT_LOG)?;
    let state_cf = required_cf(db, CF_RAFT_STATE)?;
    let end_key = format!("{end_index:020}");
    let mut batch = WriteBatch::default();
    for item in db.iterator_cf(log_cf, IteratorMode::Start) {
        let (key, _) = item.map_err(|error| MetadataError::Internal(format!("scan raft logs for purge: {error}")))?;
        if key.as_ref() > end_key.as_bytes() {
            break;
        }
        batch.delete_cf(log_cf, key);
    }
    batch.put_cf(
        state_cf,
        RAFT_STATE_KEY,
        serde_json::to_vec(state)
            .map_err(|error| MetadataError::Internal(format!("encode purged raft state: {error}")))?,
    );
    db.write_opt(batch, &durable_write_options())
        .map_err(|error| MetadataError::Internal(format!("persist deferred raft purge: {error}")))
}

fn persist_snapshot_meta(db: &DB, meta: &SnapshotMeta<u64, MetadataNode>) -> MetadataResult<()> {
    let cf = required_cf(db, CF_RAFT_SNAPSHOT)?;
    let bytes = serde_json::to_vec(meta)
        .map_err(|error| MetadataError::Internal(format!("encode snapshot metadata: {error}")))?;
    db.put_cf_opt(cf, SNAPSHOT_META_KEY, bytes, &durable_write_options())
        .map_err(|error| MetadataError::Internal(format!("persist snapshot metadata: {error}")))
}

fn required_cf<'a>(db: &'a DB, name: &str) -> MetadataResult<&'a ColumnFamily> {
    db.cf_handle(name)
        .ok_or_else(|| MetadataError::Internal(format!("column family {name} is missing")))
}

fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    options
}

fn publish_received_snapshot_file(incoming: &Path, final_path: &Path) -> MetadataResult<()> {
    if incoming == final_path {
        return sync_directory(
            final_path
                .parent()
                .ok_or_else(|| MetadataError::Internal("snapshot path has no parent".to_string()))?,
        );
    }
    if final_path.exists() {
        if !files_equal(incoming, final_path)? {
            return Err(MetadataError::InvalidArgument(format!(
                "snapshot id collision at {}",
                final_path.display()
            )));
        }
        fs::remove_file(incoming)
            .map_err(|error| MetadataError::Internal(format!("remove duplicate incoming snapshot: {error}")))?;
    } else {
        fs::rename(incoming, final_path).map_err(|error| {
            MetadataError::Internal(format!("publish received snapshot {}: {error}", final_path.display()))
        })?;
    }
    sync_directory(
        final_path
            .parent()
            .ok_or_else(|| MetadataError::Internal("snapshot path has no parent".to_string()))?,
    )
}

fn files_equal(left: &Path, right: &Path) -> MetadataResult<bool> {
    if fs::metadata(left).map(|meta| meta.len()).ok() != fs::metadata(right).map(|meta| meta.len()).ok() {
        return Ok(false);
    }
    let mut left = BufReader::new(
        File::open(left).map_err(|error| MetadataError::Internal(format!("open incoming snapshot: {error}")))?,
    );
    let mut right = BufReader::new(
        File::open(right).map_err(|error| MetadataError::Internal(format!("open existing snapshot: {error}")))?,
    );
    let mut left_buffer = [0u8; 64 * 1024];
    let mut right_buffer = [0u8; 64 * 1024];
    loop {
        let left_read = left
            .read(&mut left_buffer)
            .map_err(|error| MetadataError::Internal(format!("read incoming snapshot: {error}")))?;
        let right_read = right
            .read(&mut right_buffer)
            .map_err(|error| MetadataError::Internal(format!("read existing snapshot: {error}")))?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn sync_directory(path: &Path) -> MetadataResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| MetadataError::Internal(format!("sync directory {}: {error}", path.display())))
}

fn current_snapshot_path(storage: &RocksDBStorage) -> MetadataResult<Option<PathBuf>> {
    let Some(bytes) = storage.get_snapshot_meta()? else {
        return Ok(None);
    };
    let meta: SnapshotMeta<u64, MetadataNode> = serde_json::from_slice(&bytes)
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid current snapshot metadata: {error}")))?;
    let path = snapshot_file_path(storage, &meta.snapshot_id);
    if !path.is_file() {
        return Err(MetadataError::InvalidArgument(format!(
            "current snapshot file is missing at {}",
            path.display()
        )));
    }
    Ok(Some(path))
}

fn cleanup_obsolete_snapshot_files(storage: &RocksDBStorage, current: Option<&Path>) -> MetadataResult<()> {
    let directory = storage.snapshot_dir();
    for entry in fs::read_dir(&directory)
        .map_err(|error| MetadataError::Internal(format!("list snapshot directory: {error}")))?
    {
        let entry = entry.map_err(|error| MetadataError::Internal(format!("read snapshot entry: {error}")))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_complete_snapshot = name
            .strip_prefix("snapshot-")
            .and_then(|name| name.strip_suffix(".snap"))
            .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
        if !is_complete_snapshot || current == Some(path.as_path()) || snapshot_file_in_use(&path) {
            continue;
        }
        fs::remove_file(&path).map_err(|error| {
            MetadataError::Internal(format!("remove obsolete snapshot {}: {error}", path.display()))
        })?;
        observe::record_raft_storage_cleanup("obsolete_snapshot", 1);
    }
    Ok(())
}

fn local_codec_error(error: SnapshotCodecError) -> MetadataError {
    MetadataError::Internal(format!("failed to encode local snapshot: {error}"))
}

fn incoming_codec_error(error: SnapshotCodecError) -> MetadataError {
    MetadataError::InvalidArgument(format!("invalid incoming snapshot: {error}"))
}

#[allow(clippy::result_large_err)]
fn snapshot_read_error(signature: Option<SnapshotSignature<u64>>, error: &MetadataError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::<u64>::read_snapshot(signature, AnyError::new(error)),
    }
}

#[allow(clippy::result_large_err)]
fn snapshot_write_error(signature: Option<SnapshotSignature<u64>>, error: &MetadataError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(signature, AnyError::new(error)),
    }
}

#[allow(clippy::result_large_err)]
fn snapshot_join_error(signature: Option<SnapshotSignature<u64>>, error: JoinError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(signature, AnyError::new(&error)),
    }
}

impl RocksDBStorage {
    pub(crate) fn commit_applied_state(&self, raft_state: &AppMetadataRaftState) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        self.commit_authority_batch(WriteBatch::default(), raft_state)
    }

    /// Get Raft state (vote, last_purged, etc.).
    pub(super) fn get_raft_state(&self) -> MetadataResult<Option<Vec<u8>>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_RAFT_STATE)
            .ok_or_else(|| MetadataError::Internal("RaftState CF not found".to_string()))?;

        match db.get_cf(cf, b"raft_state") {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    pub(crate) fn load_raft_state(&self) -> MetadataResult<AppMetadataRaftState> {
        let _generation = self.pin_generation()?;
        match self.get_raft_state()? {
            Some(state_data) => serde_json::from_slice(&state_data)
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize Raft state: {e}"))),
            None => Ok(AppMetadataRaftState::default()),
        }
    }

    /// Persist Raft protocol state before acknowledging OpenRaft.
    pub(crate) fn persist_raft_state_durable(&self, state: &AppMetadataRaftState) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_RAFT_STATE)
            .ok_or_else(|| MetadataError::Internal("RaftState CF not found".to_string()))?;
        let state_data = serde_json::to_vec(state)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Raft state: {e}")))?;

        db.put_cf_opt(cf, b"raft_state", state_data, &durable_raft_write_options())
            .map_err(|e| MetadataError::Internal(format!("Failed to durably persist Raft state: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::Inode;
    use crate::inode::InodeAttrs;
    use crate::mount::{DataIoPolicy, MountEntry, MountKind, MountTable};
    use crate::raft::state_machine::AppRaftStateMachine;
    use crate::raft::storage::DetachedRoot;
    use crate::raft::{
        ApplySuccess, Command, DetachedRootReclaimResult, MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
        MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
    };
    use crate::state::RouteEpoch;

    use beryl_types::ids::{InodeId, MountId};
    use beryl_types::GroupName;
    use metrics::{Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::LeaderId;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    fn committed_apply_test_store() -> (TempDir, Arc<RocksDBStorage>, StateMachineStorage) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let read_view = Arc::new(
            MetadataReadView::new(Arc::new(MountTable::new()), Arc::clone(&state), Arc::clone(&storage)).unwrap(),
        );
        let store = StateMachineStorage::new(Arc::clone(&storage), state_machine, state, read_view).unwrap();
        (dir, storage, store)
    }

    fn normal_entry(index: u64, command: Command) -> Entry<MetadataRaftTypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(command),
        }
    }

    fn bootstrap_command() -> Command {
        Command::BootstrapNamespace {
            proposed_at_ms: 1,
            group_name: GroupName::parse("root").unwrap(),
        }
    }

    fn sample_raft_state() -> AppMetadataRaftState {
        AppMetadataRaftState {
            last_applied_log_id: Some(LogId::new(LeaderId::new(1, 1), 5)),
            last_purged_log_id: None,
            vote: None,
            committed: None,
            membership: StoredMembership::default(),
        }
    }

    fn test_storage_identity(name: &str, node_id: u64) -> StorageIdentity {
        StorageIdentity {
            storage_uuid: name.to_string(),
            cluster_id: "test-cluster".to_string(),
            group_name: GroupName::parse("root").unwrap(),
            node_id,
            bootstrap_client_id: "test-client".to_string(),
            bootstrap_call_id: "test-call".to_string(),
            bootstrap_proposed_at_ms: 1,
        }
    }

    async fn receive_snapshot(
        store: &mut StateMachineStorage,
        snapshot: Snapshot<MetadataRaftTypeConfig>,
    ) -> (
        SnapshotMeta<u64, MetadataNode>,
        Box<<MetadataRaftTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) {
        let Snapshot { meta, mut snapshot } = snapshot;
        let mut incoming = store.begin_receiving_snapshot().await.unwrap();
        tokio::io::copy(&mut *snapshot, &mut *incoming).await.unwrap();
        incoming.flush().await.unwrap();
        (meta, incoming)
    }

    #[tokio::test]
    async fn committed_mount_is_published_before_applied_state_is_visible() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let routing = Arc::new(MountTable::new());
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let read_view =
            Arc::new(MetadataReadView::new(Arc::clone(&routing), Arc::clone(&state), Arc::clone(&storage)).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let mut store =
            StateMachineStorage::new(storage.clone(), state_machine, Arc::clone(&state), read_view).unwrap();
        store.apply([normal_entry(1, bootstrap_command())]).await.unwrap();

        assert_eq!(
            routing
                .get_mount(MountId::new(1))
                .unwrap()
                .expect("published route")
                .mount_prefix,
            crate::mount::ROOT_MOUNT_PREFIX
        );
        assert_eq!(
            state.read().last_applied_log_id.expect("published applied state").index,
            1
        );
    }

    #[tokio::test]
    async fn routing_publication_failure_does_not_advance_in_memory_applied_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let routing = Arc::new(MountTable::new());
        routing
            .upsert(MountEntry {
                mount_id: MountId::new(99),
                mount_prefix: crate::mount::ROOT_MOUNT_PREFIX.to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 1,
                namespace_owner_group_name: GroupName::parse("root").unwrap(),
                root_inode_id: InodeId::new(99),
            })
            .unwrap();
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let read_view = Arc::new(MetadataReadView::new(routing, Arc::clone(&state), Arc::clone(&storage)).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let mut store =
            StateMachineStorage::new(storage.clone(), state_machine, Arc::clone(&state), read_view).unwrap();
        let result = store.apply([normal_entry(1, bootstrap_command())]).await;

        assert!(result.is_err());
        assert!(storage.get_mount(MountId::new(1)).unwrap().is_some());
        assert!(state.read().last_applied_log_id.is_none());
    }

    #[tokio::test]
    async fn codec_failure_does_not_advance_applied_state() {
        let (_dir, storage, mut store) = committed_apply_test_store();
        let parent_inode_id = InodeId::new(1);
        let mut inode_key = b"inode/".to_vec();
        inode_key.extend_from_slice(&parent_inode_id.to_be_bytes());
        storage
            .with_pinned_db(|db| {
                db.put_cf(required_cf(db, "inodes")?, inode_key, b"not-json")
                    .map_err(|error| MetadataError::Internal(error.to_string()))
            })
            .unwrap();
        let command = Command::CreateDirectory {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            root_inode_id: parent_inode_id,
            components: vec!["child".to_string()],
            attrs: InodeAttrs::new(),
            recursive: false,
        };

        assert!(store.apply([normal_entry(1, command)]).await.is_err());
        assert!(storage.load_raft_state().unwrap().last_applied_log_id.is_none());
    }

    #[tokio::test]
    async fn snapshot_round_trip_rebuilds_state() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        let storage_a = Arc::new(RocksDBStorage::create_for_format(dir_a.path()).unwrap());
        let storage_b = Arc::new(RocksDBStorage::create_for_format(dir_b.path()).unwrap());
        storage_a
            .bind_storage_identity(&test_storage_identity("source", 1))
            .unwrap();
        storage_b
            .bind_storage_identity(&test_storage_identity("destination", 2))
            .unwrap();

        let sm_a = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage_a)));

        storage_a.put_route_epoch(RouteEpoch::new(7)).unwrap();
        storage_a.set_next_inode_id(InodeId::new(77)).unwrap();
        let snapshot_mount = MountEntry {
            mount_id: MountId::new(17),
            mount_prefix: "/snapshot".to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 3,
            namespace_owner_group_name: GroupName::parse("root").unwrap(),
            root_inode_id: InodeId::new(17),
        };
        storage_a.put_mount(&snapshot_mount).unwrap();
        let detached_inode_id = InodeId::new(71);
        let detached_root = DetachedRoot {
            mount_id: snapshot_mount.mount_id,
            detached_at_ms: 123,
        };
        storage_a
            .put_inode(&Inode::new_dir(
                detached_inode_id,
                InodeAttrs::new(),
                snapshot_mount.mount_id,
            ))
            .unwrap();
        storage_a.put_detached_root(detached_inode_id, detached_root).unwrap();

        let file_id = InodeId::new(72);
        let mut file = Inode::new_file(
            file_id,
            InodeAttrs::new(),
            snapshot_mount.mount_id,
            beryl_types::FileLayout::new(4096),
        );
        let crate::inode::InodeKind::File(crate::inode::FileData { lease_epoch, .. }) = &mut file.kind else {
            unreachable!()
        };
        *lease_epoch = beryl_types::LeaseEpoch::new(1);
        storage_a.put_inode(&file).unwrap();
        storage_a
            .put_layout(file_id, beryl_types::FileLayout::new(1024))
            .unwrap();
        let close = Command::CommitFile {
            proposed_at_ms: 1,
            inode_id: file_id,
            client_id: beryl_types::ClientId::new(9),
            call_id: beryl_types::CallId::new(),
            publication: crate::inode::FilePublication {
                blocks: Vec::new(),
                target_size: 0,
                expected_generation: beryl_types::ContentGeneration::new(0),
                expected_file_size: 0,
                lease_epoch: beryl_types::LeaseEpoch::new(1),
                mode: crate::inode::PublishMode::ReplaceIfUnchanged,
            },
        };
        sm_a.apply(close.clone()).unwrap();
        let committed_file = storage_a.get_inode(file_id).unwrap().unwrap();

        // Persist raft state for meta.
        let raft_state = sample_raft_state();
        storage_a.persist_raft_state_durable(&raft_state).unwrap();
        let raft_state_lock = Arc::new(RwLock::new(raft_state));
        let read_view_a = Arc::new(
            MetadataReadView::new(
                Arc::new(MountTable::new()),
                Arc::clone(&raft_state_lock),
                Arc::clone(&storage_a),
            )
            .unwrap(),
        );

        let mut sm_store_a =
            StateMachineStorage::new(Arc::clone(&storage_a), Arc::clone(&sm_a), raft_state_lock, read_view_a).unwrap();
        let mut builder = sm_store_a.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let snapshot_id = snapshot.meta.snapshot_id.clone();

        // Install into a fresh store.
        let sm_b = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage_b)));
        let raft_state_b = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let routing_b = Arc::new(MountTable::new());
        let read_view_b = Arc::new(
            MetadataReadView::new(
                Arc::clone(&routing_b),
                Arc::clone(&raft_state_b),
                Arc::clone(&storage_b),
            )
            .unwrap(),
        );
        let mut sm_store_b = StateMachineStorage::new(
            Arc::clone(&storage_b),
            Arc::clone(&sm_b),
            Arc::clone(&raft_state_b),
            Arc::clone(&read_view_b),
        )
        .unwrap();
        storage_b
            .append_raft_logs_durable(&[(6, b"six".to_vec()), (7, b"seven".to_vec())])
            .unwrap();

        let (snapshot_meta, incoming) = receive_snapshot(&mut sm_store_b, snapshot).await;
        sm_store_b.install_snapshot(&snapshot_meta, incoming).await.unwrap();

        assert_eq!(storage_b.get_inode(file_id).unwrap().unwrap(), committed_file);
        sm_b.apply_with_raft_state(close, &storage_b.load_raft_state().unwrap())
            .unwrap();
        assert_eq!(storage_b.get_inode(file_id).unwrap().unwrap(), committed_file);

        // Validate data restored.
        assert_eq!(read_view_b.route_epoch(), RouteEpoch::new(7));
        assert_eq!(storage_b.get_next_inode_id().unwrap(), Some(InodeId::new(77)));
        assert_eq!(storage_b.prepare_inode_allocation().unwrap().inode_id, InodeId::new(77));
        assert_eq!(
            storage_b.get_detached_root(detached_inode_id).unwrap(),
            Some(detached_root)
        );
        assert!(storage_b.get_inode(detached_inode_id).unwrap().is_some());
        let reclaim_responses = sm_store_b
            .apply([normal_entry(
                6,
                Command::ReclaimDetachedRoots {
                    candidate_root_inode_ids: vec![detached_inode_id],
                    max_entries: MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
                    max_batch_bytes: MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
                },
            )])
            .await
            .unwrap();
        assert!(matches!(
            reclaim_responses.as_slice(),
            [Ok(ApplySuccess::DetachedRootsReclaimed(DetachedRootReclaimResult {
                completed_roots: 1,
                ..
            }))]
        ));
        assert!(storage_b.get_detached_root(detached_inode_id).unwrap().is_none());
        assert!(storage_b.get_inode(detached_inode_id).unwrap().is_none());
        assert_eq!(
            routing_b
                .get_mount(snapshot_mount.mount_id)
                .unwrap()
                .expect("snapshot routing entry")
                .mount_prefix,
            snapshot_mount.mount_prefix
        );
        assert_eq!(storage_b.get_raft_log(6).unwrap(), Some(b"six".to_vec()));
        assert_eq!(storage_b.get_raft_log(7).unwrap(), Some(b"seven".to_vec()));
        assert_eq!(
            storage_b.storage_identity().unwrap(),
            test_storage_identity("destination", 2)
        );

        // get_current_snapshot should return the just-built snapshot.
        let mut sm_store_b2 = StateMachineStorage::new(
            Arc::clone(&storage_b),
            Arc::clone(&sm_b),
            Arc::clone(&raft_state_b),
            read_view_b,
        )
        .unwrap();
        let current = sm_store_b2
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("installed snapshot is current");
        let current_path = current.snapshot.path().to_path_buf();
        let current_meta = current.meta;
        assert_eq!(current_meta.snapshot_id, snapshot_id);
        drop(current.snapshot);
        fs::remove_file(current_path).unwrap();
        assert!(sm_store_b2.get_current_snapshot().await.is_err());
    }

    #[tokio::test]
    async fn corrupt_snapshot_leaves_active_generation_unchanged() {
        let source_dir = TempDir::new().unwrap();
        let destination_dir = TempDir::new().unwrap();
        let source = Arc::new(RocksDBStorage::create_for_format(source_dir.path()).unwrap());
        let destination = Arc::new(RocksDBStorage::create_for_format(destination_dir.path()).unwrap());
        source
            .bind_storage_identity(&test_storage_identity("source", 1))
            .unwrap();
        destination
            .bind_storage_identity(&test_storage_identity("destination", 2))
            .unwrap();
        source.put_route_epoch(RouteEpoch::new(7)).unwrap();
        destination.put_route_epoch(RouteEpoch::new(99)).unwrap();

        let source_state = sample_raft_state();
        source.persist_raft_state_durable(&source_state).unwrap();
        let source_state = Arc::new(RwLock::new(source_state));
        let source_view = Arc::new(
            MetadataReadView::new(
                Arc::new(MountTable::new()),
                Arc::clone(&source_state),
                Arc::clone(&source),
            )
            .unwrap(),
        );
        let source_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&source)));
        let mut source_store =
            StateMachineStorage::new(Arc::clone(&source), source_machine, source_state, source_view).unwrap();
        let snapshot = source_store
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        let path = snapshot.snapshot.path().to_path_buf();
        let mut bytes = fs::read(&path).unwrap();
        let checksum_byte = bytes.len() - 2;
        bytes[checksum_byte] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        let destination_state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let destination_view = Arc::new(
            MetadataReadView::new(
                Arc::new(MountTable::new()),
                Arc::clone(&destination_state),
                Arc::clone(&destination),
            )
            .unwrap(),
        );
        let destination_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&destination)));
        let mut destination_store = StateMachineStorage::new(
            Arc::clone(&destination),
            destination_machine,
            destination_state,
            Arc::clone(&destination_view),
        )
        .unwrap();
        let (meta, incoming) = receive_snapshot(&mut destination_store, snapshot).await;

        assert!(destination_store.install_snapshot(&meta, incoming).await.is_err());
        assert_eq!(destination.get_route_epoch().unwrap(), RouteEpoch::new(99));
        assert_eq!(destination_view.route_epoch(), RouteEpoch::new(99));
        assert_eq!(
            fs::read_to_string(destination_dir.path().join("CURRENT")).unwrap(),
            "gen-000001\n"
        );
        let generations = fs::read_dir(destination_dir.path().join("generations"))
            .unwrap()
            .count();
        assert_eq!(generations, 1);
        destination_store
            .begin_receiving_snapshot()
            .await
            .expect("failed install releases incoming token");
    }

    #[tokio::test]
    async fn obsolete_snapshots_wait_for_open_readers_then_are_reclaimed() {
        let directory = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(directory.path()).unwrap());
        storage
            .bind_storage_identity(&test_storage_identity("snapshot-cleanup", 1))
            .unwrap();
        let raft_state = sample_raft_state();
        storage.persist_raft_state_durable(&raft_state).unwrap();
        let state = Arc::new(RwLock::new(raft_state));
        let read_view = Arc::new(
            MetadataReadView::new(Arc::new(MountTable::new()), Arc::clone(&state), Arc::clone(&storage)).unwrap(),
        );
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let mut store = StateMachineStorage::new(Arc::clone(&storage), state_machine, state, read_view).unwrap();
        let mut builder = store.get_snapshot_builder().await;

        let first = builder.build_snapshot().await.unwrap();
        let second = builder.build_snapshot().await.unwrap();
        assert_eq!(complete_snapshot_count(&storage), 2);

        drop(first);
        let third = builder.build_snapshot().await.unwrap();
        assert_eq!(complete_snapshot_count(&storage), 2);

        drop(second);
        drop(third);
        let current = current_snapshot_path(&storage).unwrap();
        let recorder = CleanupRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            cleanup_obsolete_snapshot_files(&storage, current.as_deref()).unwrap();
        });
        assert_eq!(complete_snapshot_count(&storage), 1);
        assert_eq!(recorder.cleanups.load(Ordering::Relaxed), 1);
    }

    #[derive(Default)]
    struct CleanupRecorder {
        cleanups: Arc<AtomicU64>,
    }

    impl Recorder for CleanupRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            if key.name() == crate::observe::METADATA_RAFT_STORAGE_CLEANUP_TOTAL {
                Counter::from_arc(Arc::new(CleanupCounter {
                    cleanups: Arc::clone(&self.cleanups),
                }))
            } else {
                Counter::noop()
            }
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct CleanupCounter {
        cleanups: Arc<AtomicU64>,
    }

    impl CounterFn for CleanupCounter {
        fn increment(&self, value: u64) {
            self.cleanups.fetch_add(value, Ordering::Relaxed);
        }

        fn absolute(&self, value: u64) {
            self.cleanups.store(value, Ordering::Relaxed);
        }
    }

    fn complete_snapshot_count(storage: &RocksDBStorage) -> usize {
        fs::read_dir(storage.snapshot_dir())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("snapshot-") && name.ends_with(".snap"))
            })
            .count()
    }

    impl StateMachineStorage {
        pub(crate) fn new(
            storage: Arc<RocksDBStorage>,
            state_machine: Arc<AppRaftStateMachine>,
            state: Arc<RwLock<AppMetadataRaftState>>,
            read_view: Arc<MetadataReadView>,
        ) -> MetadataResult<Self> {
            Self::new_with_tracker(
                storage,
                state_machine,
                state,
                read_view,
                Arc::new(SnapshotInstallTracker::default()),
            )
        }
    }
}
