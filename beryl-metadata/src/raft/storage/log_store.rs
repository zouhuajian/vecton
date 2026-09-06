// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB-backed Raft log store implementing openraft `RaftLogStorage`.

use crate::observe;
use crate::raft::storage::{RocksDBStorage, SnapshotInstallTracker};
use crate::raft::types::{AppMetadataRaftState, MetadataRaftTypeConfig};
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::Entry;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;

/// Raft log storage implementation using RocksDB.
pub(crate) struct AppLogStorage {
    storage: Arc<RocksDBStorage>,
    state: Arc<RwLock<AppMetadataRaftState>>,
    snapshot_install: Arc<SnapshotInstallTracker>,
}

impl AppLogStorage {
    pub(crate) fn new(
        storage: Arc<RocksDBStorage>,
        state: Arc<RwLock<AppMetadataRaftState>>,
        snapshot_install: Arc<SnapshotInstallTracker>,
    ) -> Self {
        Self {
            storage,
            state,
            snapshot_install,
        }
    }
}

impl RaftLogReader<MetadataRaftTypeConfig> for AppLogStorage {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetadataRaftTypeConfig>>, StorageError<u64>> {
        // Delegate to LogReader
        let mut reader = self.get_log_reader().await;
        reader.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<MetadataRaftTypeConfig> for AppLogStorage {
    type LogReader = AppLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<MetadataRaftTypeConfig>, StorageError<u64>> {
        let last_purged_log_id = self.state.read().last_purged_log_id;

        // The log store must never infer its tail from state-machine applied state.
        let last_log_id = match self.storage.get_last_log_index() {
            Ok(Some(idx)) => match self.storage.get_raft_log(idx) {
                Ok(Some(bytes)) => {
                    let entry: Entry<MetadataRaftTypeConfig> = decode_log_entry(idx, &bytes)?;
                    Some(*entry.get_log_id())
                }
                Ok(None) => {
                    let error = std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Raft log index {idx} exists in the index scan but has no value"),
                    );
                    return Err(StorageError::IO {
                        source: StorageIOError::<u64>::read_log_at_index(idx, AnyError::new(&error)),
                    });
                }
                Err(e) => {
                    return Err(StorageError::IO {
                        source: StorageIOError::<u64>::read_log_at_index(idx, AnyError::new(&e)),
                    })
                }
            },
            Ok(None) => last_purged_log_id,
            Err(e) => {
                return Err(StorageError::IO {
                    source: StorageIOError::<u64>::read_logs(AnyError::new(&e)),
                })
            }
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        AppLogReader {
            storage: Arc::clone(&self.storage),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let _generation = self.storage.pin_generation().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_vote(AnyError::new(&e)),
        })?;
        let mut current = self.state.write();
        let mut next = current.clone();
        next.vote = Some(*vote);
        self.storage
            .persist_raft_state_durable(&next)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_vote(AnyError::new(&e)),
            })?;
        *current = next;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let state = self.state.read();
        Ok(state.vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        let _generation = self.storage.pin_generation().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_state_machine(AnyError::new(&e)),
        })?;
        let mut current = self.state.write();
        let mut next = current.clone();
        next.committed = committed;
        self.storage
            .persist_raft_state_durable(&next)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_state_machine(AnyError::new(&e)),
            })?;
        *current = next;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.state.read().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<MetadataRaftTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MetadataRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut encoded_entries = Vec::new();
        for entry in entries {
            let log_id = entry.get_log_id();
            let log_index = log_id.index;
            let entry_data = serde_json::to_vec(&entry).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_log_entry(*log_id, AnyError::new(&e)),
            })?;
            encoded_entries.push((log_index, entry_data));
        }
        let bytes = encoded_entries.iter().map(|(_, entry)| entry.len()).sum();
        let started = Instant::now();
        if let Err(e) = self.storage.append_raft_logs_durable(&encoded_entries) {
            observe::record_raft_log_durable_write("error", bytes, started.elapsed().as_secs_f64());
            return Err(StorageError::IO {
                source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
            });
        }
        observe::record_raft_log_durable_write("ok", bytes, started.elapsed().as_secs_f64());

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.storage
            .truncate_raft_logs(log_id.index)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
            })?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        if self.snapshot_install.defer_purge(log_id) {
            return Ok(());
        }
        let _generation = self.storage.pin_generation().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
        })?;
        let mut current = self.state.write();
        let mut next = current.clone();
        next.last_purged_log_id = Some(log_id);
        self.storage
            .purge_raft_logs_and_state(log_id.index, &next)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
            })?;
        *current = next;
        Ok(())
    }
}

/// Log reader for Raft.
pub(crate) struct AppLogReader {
    storage: Arc<RocksDBStorage>,
}

impl RaftLogReader<MetadataRaftTypeConfig> for AppLogReader {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetadataRaftTypeConfig>>, StorageError<u64>> {
        use std::ops::Bound;

        // Determine start and end indices
        let start_index = match range.start_bound() {
            Bound::Included(&idx) => idx,
            Bound::Excluded(&idx) => idx + 1,
            Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            Bound::Included(&idx) => Some(idx + 1), // Make it exclusive
            Bound::Excluded(&idx) => Some(idx),
            Bound::Unbounded => None,
        };

        let raw_entries = self
            .storage
            .scan_raft_logs(start_index, end_index)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_logs(AnyError::new(&e)),
            })?;
        let mut entries = Vec::with_capacity(raw_entries.len());
        for (index, data) in raw_entries {
            entries.push(decode_log_entry(index, &data)?);
        }
        Ok(entries)
    }
}

// Keep log entry decoding in one place so error mapping stays consistent.
// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn decode_log_entry(index: u64, data: &[u8]) -> Result<Entry<MetadataRaftTypeConfig>, StorageError<u64>> {
    serde_json::from_slice(data).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::read_log_at_index(index, AnyError::new(&e)),
    })
}

use super::*;

impl RocksDBStorage {
    // ===== Raft-specific methods =====

    /// Get Raft log entry by index.
    pub fn get_raft_log(&self, log_index: u64) -> MetadataResult<Option<Vec<u8>>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let key = format!("{:020}", log_index); // Zero-padded for lexicographic ordering

        match db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Read stored Raft logs with one ordered iterator.
    pub(crate) fn scan_raft_logs(
        &self,
        start_index: u64,
        end_index_exclusive: Option<u64>,
    ) -> MetadataResult<Vec<(u64, Vec<u8>)>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let start_key = format!("{:020}", start_index);
        let iter = db.iterator_cf(
            cf,
            rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward),
        );
        let mut entries = Vec::new();
        let mut previous_index: Option<u64> = None;

        for item in iter {
            let (key, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {e}")))?;
            let key = std::str::from_utf8(&key)
                .map_err(|e| MetadataError::Internal(format!("Invalid Raft log key encoding: {e}")))?;
            let index = key
                .parse::<u64>()
                .map_err(|e| MetadataError::Internal(format!("Invalid Raft log index key {key:?}: {e}")))?;
            if end_index_exclusive.is_some_and(|end| index >= end) {
                break;
            }
            if let Some(previous) = previous_index {
                if previous.checked_add(1) != Some(index) {
                    return Err(MetadataError::Internal(format!(
                        "Raft log hole detected between indexes {previous} and {index}"
                    )));
                }
            }
            previous_index = Some(index);
            entries.push((index, value.to_vec()));
        }

        Ok(entries)
    }

    /// Append one contiguous set of Raft logs in a synchronous WAL-backed batch.
    pub(crate) fn append_raft_logs_durable(&self, entries: &[(u64, Vec<u8>)]) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        if let Some(window) = entries.windows(2).find(|window| {
            window[0]
                .0
                .checked_add(1)
                .is_none_or(|expected| expected != window[1].0)
        }) {
            return Err(MetadataError::Internal(format!(
                "Raft log append is not contiguous: {} is followed by {}",
                window[0].0, window[1].0
            )));
        }

        let cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let mut batch = WriteBatch::default();
        for (log_index, entry_data) in entries {
            let key = format!("{:020}", log_index);
            batch.put_cf(cf, key.as_bytes(), entry_data);
        }

        db.write_opt(batch, &durable_raft_write_options())
            .map_err(|e| MetadataError::Internal(format!("Failed to durably append Raft logs: {e}")))
    }

    /// Delete the complete Raft log suffix in one synchronous WAL-backed batch.
    pub(crate) fn truncate_raft_logs(&self, start_index: u64) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let start_key = format!("{:020}", start_index);
        let iter = db.iterator_cf(
            cf,
            rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward),
        );
        let mut batch = WriteBatch::default();
        for item in iter {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {e}")))?;
            batch.delete_cf(cf, key);
        }

        db.write_opt(batch, &durable_raft_write_options())
            .map_err(|e| MetadataError::Internal(format!("Failed to durably truncate Raft logs: {e}")))
    }

    /// Delete a Raft log prefix and persist its covering state in one durable batch.
    pub(crate) fn purge_raft_logs_and_state(&self, end_index: u64, state: &AppMetadataRaftState) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let log_cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let state_cf = db
            .cf_handle(CF_RAFT_STATE)
            .ok_or_else(|| MetadataError::Internal("RaftState CF not found".to_string()))?;
        let state_data = serde_json::to_vec(state)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Raft state: {e}")))?;
        let end_key = format!("{:020}", end_index);
        let mut batch = WriteBatch::default();
        for item in db.iterator_cf(log_cf, rocksdb::IteratorMode::Start) {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {e}")))?;
            if key.as_ref() > end_key.as_bytes() {
                break;
            }
            batch.delete_cf(log_cf, key);
        }
        batch.put_cf(state_cf, b"raft_state", state_data);

        db.write_opt(batch, &durable_raft_write_options())
            .map_err(|e| MetadataError::Internal(format!("Failed to durably purge Raft logs: {e}")))
    }

    /// Get the last log index from RocksDB.
    pub fn get_last_log_index(&self) -> MetadataResult<Option<u64>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;

        // Iterate from end to find the last log
        let mut iter = db.iterator_cf(cf, rocksdb::IteratorMode::End);

        if let Some(item) = iter.next() {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            // Parse the key (format: "{:020}")
            let key_str = String::from_utf8_lossy(&key);
            if let Ok(index) = key_str.trim().parse::<u64>() {
                Ok(Some(index))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::MountTable;
    use crate::raft::state_machine::AppRaftStateMachine;
    use crate::raft::storage::{StateMachineStorage, StorageIdentity};
    use crate::raft::MetadataReadView;
    use openraft::testing::{StoreBuilder, Suite};
    use openraft::LeaderId;
    use tempfile::{tempdir, TempDir};

    struct TestStoreBuilder;

    impl StoreBuilder<MetadataRaftTypeConfig, AppLogStorage, StateMachineStorage, TempDir> for TestStoreBuilder {
        async fn build(&self) -> Result<(TempDir, AppLogStorage, StateMachineStorage), StorageError<u64>> {
            let dir = tempdir().expect("create OpenRaft store test directory");
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).expect("create RocksDB test store"));
            storage
                .bind_storage_identity(&StorageIdentity {
                    storage_uuid: "openraft-suite".to_string(),
                    cluster_id: "openraft-suite".to_string(),
                    group_name: beryl_types::GroupName::parse("root").unwrap(),
                    node_id: 1,
                    bootstrap_client_id: "openraft-suite".to_string(),
                    bootstrap_call_id: "openraft-suite".to_string(),
                    bootstrap_proposed_at_ms: 1,
                })
                .expect("bind OpenRaft test storage identity");
            storage
                .put_route_epoch(crate::state::RouteEpoch::new(1))
                .expect("seed required route epoch");
            let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
            let app = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
            let snapshot_install = Arc::new(SnapshotInstallTracker::default());
            let log = AppLogStorage::new(Arc::clone(&storage), Arc::clone(&state), Arc::clone(&snapshot_install));
            let read_view = Arc::new(
                MetadataReadView::new(Arc::new(MountTable::new()), Arc::clone(&state), Arc::clone(&storage))
                    .expect("create metadata read view"),
            );
            let sm = StateMachineStorage::new_with_tracker(
                storage,
                app,
                state,
                read_view,
                snapshot_install,
                tokio_util::task::TaskTracker::new().token(),
            )
            .expect("create OpenRaft state-machine store");
            Ok((dir, log, sm))
        }
    }

    #[test]
    fn openraft_storage_contract() {
        Suite::test_all(TestStoreBuilder).expect("OpenRaft storage contract");
    }

    #[tokio::test]
    async fn purge_is_deferred_while_incoming_snapshot_is_pending() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        storage
            .append_raft_logs_durable(&[(1, b"one".to_vec()), (2, b"two".to_vec())])
            .unwrap();
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let tracker = Arc::new(SnapshotInstallTracker::default());
        let token = tracker.begin().unwrap();
        let mut log_store = AppLogStorage::new(Arc::clone(&storage), Arc::clone(&state), tracker);

        log_store
            .purge(LogId::new(openraft::LeaderId::new(1, 1), 1))
            .await
            .unwrap();

        assert!(storage.get_raft_log(1).unwrap().is_some());
        assert!(state.read().last_purged_log_id.is_none());
        assert_eq!(token.complete().unwrap().unwrap().index, 1);
    }

    #[test]
    fn raft_log_batch_rejects_a_hole_before_writing() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let entries = vec![(7, vec![7]), (9, vec![9])];

        assert!(storage.append_raft_logs_durable(&entries).is_err());
        assert_eq!(None, storage.get_raft_log(7).unwrap());
        assert_eq!(None, storage.get_raft_log(9).unwrap());
    }

    #[test]
    fn raft_log_truncate_removes_the_complete_suffix() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        storage
            .append_raft_logs_durable(&[(1, vec![1]), (2, vec![2]), (3, vec![3])])
            .unwrap();

        storage.truncate_raft_logs(2).unwrap();

        assert_eq!(Some(vec![1]), storage.get_raft_log(1).unwrap());
        assert_eq!(None, storage.get_raft_log(2).unwrap());
        assert_eq!(None, storage.get_raft_log(3).unwrap());
    }

    #[test]
    fn raft_log_purge_and_last_purged_state_commit_together() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        storage
            .append_raft_logs_durable(&[(1, vec![1]), (2, vec![2]), (3, vec![3])])
            .unwrap();
        let purged = LogId::new(LeaderId::new(2, 1), 2);
        let state = AppMetadataRaftState {
            last_purged_log_id: Some(purged),
            ..AppMetadataRaftState::default()
        };

        storage.purge_raft_logs_and_state(2, &state).unwrap();

        assert_eq!(None, storage.get_raft_log(1).unwrap());
        assert_eq!(None, storage.get_raft_log(2).unwrap());
        assert_eq!(Some(vec![3]), storage.get_raft_log(3).unwrap());
        let stored: AppMetadataRaftState = serde_json::from_slice(&storage.get_raft_state().unwrap().unwrap()).unwrap();
        assert_eq!(Some(purged), stored.last_purged_log_id);
    }
}
