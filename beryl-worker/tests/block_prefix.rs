// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Filesystem checkpoint and crash-image coverage through the local-store boundary.
use beryl_proto::worker::{BlockMetaPayloadProto, BlockStateProto};
use beryl_types::layout::BlockFormatId;
use beryl_types::{BlockId, BlockIndex, ClientId, FencingToken, GroupName, InodeId, LeaseEpoch, Tier};
use beryl_worker::store::block::{
    CheckpointBlockRequest, ChecksumKind, FullBlockFileStore, FullBlockFileStoreConfig, OpenBlockWriteRequest,
    ReclaimBlockRequest, ReclaimBlockResult,
};
use bytes::Bytes;
use prost::Message;
use std::fs::{self, OpenOptions};
use tempfile::TempDir;

fn fixture() -> (TempDir, FullBlockFileStore, OpenBlockWriteRequest) {
    let dir = tempfile::tempdir().unwrap();
    let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(dir.path().into()));
    let block_id = BlockId::new(InodeId::new(7), BlockIndex::new(2));
    let request = OpenBlockWriteRequest {
        group_name: GroupName::parse("root").unwrap(),
        block_id,
        block_size: 16,
        block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
        chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
        checksum_kind: ChecksumKind::None,
        tier: Tier::Ssd,
        fencing_token: FencingToken::new(block_id, ClientId::generate(), LeaseEpoch::new(1)),
        write_offset: 0,
        visible_len: 0,
    };
    (dir, store, request)
}

fn checkpoint(store: &FullBlockFileStore, req: &OpenBlockWriteRequest, data: &'static [u8]) {
    store.open_block_write(req.clone()).unwrap();
    store
        .write_at(
            &req.group_name,
            req.block_id,
            req.write_offset,
            Bytes::from_static(data),
        )
        .unwrap();
    let meta = store
        .checkpoint_block(CheckpointBlockRequest {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
            effective_len: req.write_offset + data.len() as u64,
            fencing_token: req.fencing_token,
        })
        .unwrap();
    assert_eq!(meta.source.durable_len, req.write_offset + data.len() as u64);
}

#[test]
fn short_streams_resume_and_new_writer_discards_only_unpublished_suffix() {
    let (_dir, store, mut req) = fixture();
    checkpoint(&store, &req, b"abcd");
    req.write_offset = 4;
    req.visible_len = 4;
    checkpoint(&store, &req, b"lost");
    assert!(
        store.open_block_write(req.clone()).is_err(),
        "same epoch cannot rewind D"
    );
    let old = req.clone();
    req.fencing_token.epoch = LeaseEpoch::new(2);
    let meta = store.open_block_write(req.clone()).unwrap();
    assert_eq!(meta.source.durable_len, 4);
    assert_eq!(
        fs::metadata(store.paths(&req.group_name, req.block_id).data_path)
            .unwrap()
            .len(),
        4
    );
    assert!(store.open_block_write(old.clone()).is_err());
    assert!(store
        .checkpoint_block(CheckpointBlockRequest {
            group_name: req.group_name.clone(),
            block_id: req.block_id,
            effective_len: 4,
            fencing_token: old.fencing_token,
        })
        .is_err());
    assert!(store
        .write_at(&req.group_name, req.block_id, 0, Bytes::from_static(b"bad"))
        .is_err());
    checkpoint(&store, &req, b"efgh");
    assert_eq!(
        store.read_at(&req.group_name, req.block_id, 0, 8).unwrap(),
        b"abcdefgh"[..]
    );
    assert!(store.read_at(&req.group_name, req.block_id, 0, 9).is_err());
}

/// Installs a complete metadata image at a named crash boundary, without production fault hooks.
fn persist_meta_image(
    store: &FullBlockFileStore,
    req: &OpenBlockWriteRequest,
    edit: impl FnOnce(&mut BlockMetaPayloadProto),
) {
    let path = store.paths(&req.group_name, req.block_id).meta_path;
    let old = fs::read(&path).unwrap();
    let mut meta = BlockMetaPayloadProto::decode(&old[20..]).unwrap();
    edit(&mut meta);
    let payload = meta.encode_to_vec();
    let mut image = old[..20].to_vec();
    image[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    image.extend_from_slice(&payload);
    fs::write(path, image).unwrap();
}

#[test]
fn recovery_uses_checkpoint_after_unsynced_io_and_interrupted_takeover() {
    for takeover in [false, true] {
        let (_dir, store, mut req) = fixture();
        checkpoint(&store, &req, b"abcd");
        req.write_offset = 4;
        checkpoint(&store, &req, b"suffix");
        if takeover {
            // Crash after E2/D4 metadata replacement and before truncation of P10.
            persist_meta_image(&store, &req, |meta| {
                meta.source.as_mut().unwrap().durable_len = 4;
                meta.visibility.as_mut().unwrap().fencing_token.as_mut().unwrap().epoch = 2;
            });
        } else {
            store
                .write_at(&req.group_name, req.block_id, 10, Bytes::from_static(b"extra"))
                .unwrap();
        }
        store.recover_blocks().unwrap();
        let meta = store.load_meta(&req.group_name, req.block_id).unwrap();
        let expected = if takeover { 4 } else { 10 };
        assert_eq!(meta.source.durable_len, expected);
        assert_eq!(
            fs::metadata(store.paths(&req.group_name, req.block_id).data_path)
                .unwrap()
                .len(),
            expected
        );
        assert_eq!(store.read_at(&req.group_name, req.block_id, 0, 4).unwrap(), b"abcd"[..]);
    }
}

#[test]
fn recovery_rejects_short_prefix_and_old_format_without_deleting_data() {
    for old_format in [false, true] {
        let (_dir, store, req) = fixture();
        checkpoint(&store, &req, b"abcd");
        let paths = store.paths(&req.group_name, req.block_id);
        if old_format {
            let mut bytes = fs::read(&paths.meta_path).unwrap();
            bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
            fs::write(&paths.meta_path, bytes).unwrap();
        } else {
            OpenOptions::new()
                .write(true)
                .open(&paths.data_path)
                .unwrap()
                .set_len(3)
                .unwrap();
        }
        assert!(store.recover_blocks().is_err());
        assert!(paths.data_path.exists());
        assert!(paths.meta_path.exists());
    }
}

#[test]
fn deletion_recovers_each_unlink_boundary_and_is_idempotent() {
    for data_removed in [false, true] {
        let (_dir, store, req) = fixture();
        checkpoint(&store, &req, b"abcd");
        persist_meta_image(&store, &req, |meta| {
            meta.visibility.as_mut().unwrap().block_state = BlockStateProto::BlockStateDeleting as i32
        });
        let paths = store.paths(&req.group_name, req.block_id);
        if data_removed {
            fs::remove_file(&paths.data_path).unwrap();
        }
        assert!(store.read_at(&req.group_name, req.block_id, 0, 1).is_err());
        store.recover_blocks().unwrap();
        assert!(!paths.data_path.exists());
        assert!(!paths.meta_path.exists());
        assert_eq!(
            store
                .reclaim_block(&ReclaimBlockRequest {
                    group_name: req.group_name,
                    block_id: req.block_id
                })
                .unwrap(),
            ReclaimBlockResult::AlreadyAbsent
        );
    }
}
