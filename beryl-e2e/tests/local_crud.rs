// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientResult, FileType, ListStatusOptions, MkdirOptions};
use beryl_e2e::data::deterministic_bytes;
use beryl_e2e::TestCluster;
use bytes::Bytes;
use std::fmt::Debug;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_client_crud_roundtrip() {
    let mut cluster = TestCluster::start().await.expect("start hermetic local cluster");
    let client = cluster.client();
    let dir = "/e2e";
    let path = "/e2e/file";
    let renamed_path = "/e2e/file.renamed";

    let created_dir = client.mkdirs(dir).await.expect("mkdirs through metadata");
    assert_eq!(created_dir.path(), dir);
    assert_eq!(created_dir.kind, FileType::Dir);

    let first = Bytes::from(deterministic_bytes(1_337));
    let suffix = Bytes::from_static(b"-beryl-append-suffix");
    let expected = [first.as_ref(), suffix.as_ref()].concat();
    let mut writer = client.create(path).await.expect("create through metadata");
    writer.write_all(first.clone()).await.expect("write through worker");
    writer.close().await.expect("close through metadata");

    let status = client.get_status(path).await.expect("status after close");
    assert_eq!(status.path(), path);
    assert_eq!(status.kind, FileType::File);
    assert_eq!(status.len, first.len() as u64);

    let read = client
        .open(path)
        .await
        .expect("open after close")
        .read_to_end()
        .await
        .expect("read first bytes");
    assert_eq!(read, first);

    let mut appender = client.append(path).await.expect("append through metadata");
    appender
        .write_all(suffix.clone())
        .await
        .expect("append write through worker");
    appender.close().await.expect("append close through metadata");

    let read = client
        .open(path)
        .await
        .expect("open after append")
        .read_to_end()
        .await
        .expect("read appended bytes");
    assert_eq!(read.as_ref(), expected.as_slice());

    let subdir = "/e2e/subdir";
    client
        .mkdirs_with_options(subdir, MkdirOptions { create_parent: false })
        .await
        .expect("create second listing entry");

    let mut statuses = client
        .list_status_with_options(dir, ListStatusOptions { page_size: Some(1) })
        .await
        .expect("start bounded directory listing");
    assert_eq!(statuses.path(), dir);
    let mut listed = Vec::new();
    while let Some(status) = statuses.next().await.expect("fetch next directory status") {
        listed.push(status);
    }
    listed.sort_by(|left, right| left.path().cmp(right.path()));
    assert_eq!(
        listed.iter().map(|status| status.path()).collect::<Vec<_>>(),
        [path, subdir]
    );
    assert_eq!(listed[0].kind, FileType::File);
    assert_eq!(listed[0].len, expected.len() as u64);
    assert_eq!(listed[1].kind, FileType::Dir);

    client.delete(subdir).await.expect("delete empty listing subdirectory");

    let before_rename = client.get_status(path).await.unwrap();
    assert_eq!(before_rename.create_time, status.create_time);
    assert!(before_rename.modify_time >= status.modify_time);
    let reader_opened_before_rename = client.open(path).await.expect("open reader before rename");
    client
        .rename(path, renamed_path)
        .await
        .expect("rename through metadata");
    assert_not_found(client.get_status(path).await, "old path after rename");

    let renamed_status = client.get_status(renamed_path).await.expect("status after rename");
    assert_eq!(renamed_status.path(), renamed_path);
    assert_eq!(renamed_status.create_time, before_rename.create_time);
    assert_eq!(renamed_status.modify_time, before_rename.modify_time);
    assert_eq!(renamed_status.len, expected.len() as u64);

    let renamed_read = client
        .open(renamed_path)
        .await
        .expect("open renamed file")
        .read_to_end()
        .await
        .expect("read renamed file");
    assert_eq!(renamed_read.as_ref(), expected.as_slice());
    let mut moved_reader_bytes = vec![0u8; expected.len()];
    reader_opened_before_rename
        .read_exact_at(0, &mut moved_reader_bytes)
        .await
        .expect("reader opened before rename remains bound to the inode");
    assert_eq!(moved_reader_bytes.as_slice(), expected.as_slice());

    client
        .delete(renamed_path)
        .await
        .expect("namespace delete renamed file");
    assert_not_found(client.get_status(renamed_path).await, "deleted path status");
    assert_not_found(client.open(renamed_path).await, "deleted path open");
    let mut probe = [0u8; 1];
    assert_not_found(
        reader_opened_before_rename.read_at(0, &mut probe).await,
        "reader for deleted inode",
    );

    let replacement = Bytes::from_static(b"replacement-file");
    let mut replacement_writer = client.create(renamed_path).await.expect("recreate deleted path");
    replacement_writer
        .write_all(replacement.clone())
        .await
        .expect("write replacement file");
    replacement_writer
        .close()
        .await
        .unwrap_or_else(|err| panic!("close replacement file: {err} ({err:?})"));
    assert_not_found(
        reader_opened_before_rename.read_at(0, &mut probe).await,
        "old reader must not bind to recreated path",
    );
    let replacement_read = client
        .open(renamed_path)
        .await
        .expect("open replacement file")
        .read_to_end()
        .await
        .expect("read replacement file");
    assert_eq!(replacement_read, replacement);
    client.delete(renamed_path).await.expect("delete replacement file");

    let mut statuses = client.list_status(dir).await.expect("list after delete");
    assert!(statuses.next().await.expect("read empty listing").is_none());

    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn visibility_sync_then_continue_write_roundtrip() {
    let mut cluster = TestCluster::start().await.expect("start hermetic local cluster");
    let client = cluster.client();
    let path = "/sync-continue";
    let first = Bytes::from(vec![b'a'; 317]);
    let second = Bytes::from(vec![b'b'; 1024]);

    let mut writer = client.create(path).await.expect("create through metadata");
    writer.write_all(first.clone()).await.expect("write first block");
    writer
        .sync()
        .await
        .expect("publish first block while keeping session open");
    let visible_prefix = client
        .open(path)
        .await
        .expect("open immediately after visibility sync")
        .read_to_end()
        .await
        .expect("read published prefix while writer remains open");
    assert_eq!(visible_prefix, first);

    writer
        .write_all(second.clone())
        .await
        .expect("write after visibility sync");
    writer.close().await.expect("close after second block");

    let actual = client
        .open(path)
        .await
        .expect("open after close")
        .read_to_end()
        .await
        .expect("read both publication revisions");
    let expected = [first.as_ref(), second.as_ref()].concat();
    assert_eq!(actual.as_ref(), expected.as_slice());

    assert_eq!(cluster.physical_block_count().unwrap(), 2);
    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_more_than_ten_blocks_roundtrip() {
    let mut cluster = TestCluster::start().await.expect("start hermetic local cluster");
    let client = cluster.client();
    let path = "/many-blocks";
    let payload = Bytes::from(deterministic_bytes(12 * 1024 + 17));
    let mut writer = client.create(path).await.expect("create file");

    for offset in (0..payload.len()).step_by(127) {
        let end = (offset + 127).min(payload.len());
        writer
            .write_all(payload.slice(offset..end))
            .await
            .expect("write small frame across more than ten blocks");
    }
    writer.close().await.expect("close file");

    let mut reader = client.open(path).await.expect("open file");
    let mut actual = Vec::with_capacity(payload.len());
    let mut buffer = [0u8; 127];
    loop {
        let read = reader.read(&mut buffer).await.expect("read bounded step");
        if read == 0 {
            break;
        }
        actual.extend_from_slice(&buffer[..read]);
    }
    assert_eq!(actual.as_slice(), payload.as_ref());
    cluster.shutdown().await.expect("local cluster shutdown");
}

fn assert_not_found<T: Debug>(result: ClientResult<T>, context: &str) {
    let err = result.expect_err(context);
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("not found") || message.contains("enoent"),
        "{context} should fail with not-found style error, got {err}"
    );
}
