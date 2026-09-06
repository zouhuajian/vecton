// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

mod support;

use beryl_client::{
    ClientConfig, ClientError, ClientErrorKind, DeleteOptions, FsClient, ListStatusOptions, MkdirOptions,
};
use beryl_common::error::rpc::{ErrorKind, InternalErrorKind, MetadataErrorKind, RefreshHint, RpcErrorDetail};
use beryl_common::header::{HEADER_PRE_HANDLER_REJECTION, PRE_HANDLER_REJECTION_RPC_CONCURRENCY};
use beryl_proto::common::{
    BlockIdProto, ClientIdProto, FencingTokenProto, FileLayoutProto, GroupStateWatermarkProto, RaftLogIdProto,
    TierProto, WorkerEndpointInfoProto,
};
use beryl_proto::metadata::{
    AbortFileWriteResponseProto, AllocateBlockResponseProto, CommitFileResponseProto, CreateDirectoryResponseProto,
    CreateFileResponseProto, FileBlockLocationProto, FileTypeProto, GetBlockLocationsResponseProto,
    GetStatusResponseProto, LocatedBlockProto, MsyncResponseProto, OpenFileResponseProto, RenewLeaseResponseProto,
    SyncWriteResponseProto, WriteHandleProto,
};
use beryl_types::BlockFormatId;
use bytes::Bytes;
use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use support::{
    MetadataCall, MetadataReply, MetadataScript, MockMetadata, MockWorker, ReadReply, ResponseAuthority, WorkerScript,
    WriteReply,
};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

const WORKER_RUN_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

#[tokio::test]
async fn list_status_rejects_zero_page_size_before_rpc() {
    let client = FsClient::new(client_config("127.0.0.1:1", 1)).expect("client");

    let error = match client
        .list_status_with_options("/alpha", ListStatusOptions { page_size: Some(0) })
        .await
    {
        Ok(_) => panic!("zero page size must fail before Metadata"),
        Err(error) => error,
    };

    assert_client_error(&error, ClientErrorKind::InvalidArgument, false, "greater than zero");
}

#[tokio::test]
async fn metadata_read_retries_reuse_one_identity_and_deadline() {
    let metadata = MockMetadata::new(MetadataScript {
        get_status: VecDeque::from([
            MetadataReply::status(pre_handler_rejection()),
            MetadataReply::error(RpcErrorDetail::retry(
                ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
                Some(1),
                "scripted server retry",
            )),
            MetadataReply::success(status_response(10)),
        ]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 3)).expect("client");

    let status = client.get_status("/alpha").await.expect("third attempt succeeds");
    assert_eq!(status.len, 10);

    let calls = metadata.calls();
    assert_methods(&calls, &["GetStatus", "GetStatus", "GetStatus"]);
    assert_same_identity_and_deadline(&calls);
    server.shutdown().await;
}

#[tokio::test]
async fn metadata_mutations_retry_only_when_the_public_operation_is_replayable() {
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([
            MetadataReply::status(Status::unavailable("CreateFile transport ambiguity")),
            MetadataReply::success(create_response(301, 8)),
        ]),
        create_directory: VecDeque::from([
            MetadataReply::status(Status::unavailable("recursive CreateDirectory transport ambiguity")),
            MetadataReply::success(CreateDirectoryResponseProto {
                create_time: 11,
                modify_time: 12,
                ..CreateDirectoryResponseProto::default()
            }),
            MetadataReply::status(Status::unavailable("non-recursive CreateDirectory transport ambiguity")),
        ]),
        open_write: VecDeque::from([MetadataReply::status(Status::unavailable(
            "OpenWrite transport ambiguity",
        ))]),
        delete: VecDeque::from([MetadataReply::status(Status::unavailable("Delete transport ambiguity"))]),
        rename: VecDeque::from([MetadataReply::status(Status::unavailable("Rename transport ambiguity"))]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 3)).expect("client");

    let _writer = client.create("/created").await.expect("CreateFile safely replays");
    client
        .mkdirs("/parent/child")
        .await
        .expect("recursive mkdirs safely replays");

    let append = client
        .append("/created")
        .await
        .expect_err("OpenWrite ambiguity fails closed");
    assert!(append.is_outcome_unknown());
    let mkdir = client
        .mkdirs_with_options("/single", MkdirOptions { create_parent: false })
        .await
        .expect_err("non-recursive mkdir ambiguity fails closed");
    assert!(mkdir.is_outcome_unknown());
    let delete = client
        .delete_with_options("/created", DeleteOptions::default())
        .await
        .expect_err("Delete ambiguity fails closed");
    assert!(delete.is_outcome_unknown());
    let rename = client
        .rename("/created", "/renamed")
        .await
        .expect_err("Rename ambiguity fails closed");
    assert!(rename.is_outcome_unknown());

    let calls = metadata.calls();
    let create_calls = calls_for(&calls, "CreateFile");
    assert_eq!(create_calls.len(), 2);
    assert_same_identity_and_deadline(&create_calls);
    let mkdir_calls = calls_for(&calls, "CreateDirectory");
    assert_eq!(mkdir_calls.len(), 3);
    assert_same_identity_and_deadline(&mkdir_calls[..2]);
    assert_ne!(call_id(&mkdir_calls[0]), call_id(&mkdir_calls[2]));
    assert_eq!(calls_for(&calls, "OpenWrite").len(), 1);
    assert_eq!(calls_for(&calls, "Delete").len(), 1);
    assert_eq!(calls_for(&calls, "Rename").len(), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn stale_metadata_refreshes_with_a_child_call_and_carries_new_authority() {
    let state_one = watermark(1);
    let state_nine = watermark(9);
    let metadata = MockMetadata::new(MetadataScript {
        get_status: VecDeque::from([
            MetadataReply::error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                RefreshHint::default(),
                "scripted stale state",
            )),
            MetadataReply::SuccessWithAuthority(
                status_response(10),
                ResponseAuthority {
                    state: vec![state_nine.clone()],
                    mount_epoch: Some(31),
                    route_epoch: Some(41),
                },
            ),
            MetadataReply::success(status_response(10)),
        ]),
        msync: VecDeque::from([MetadataReply::success(MsyncResponseProto {
            state: Some(state_one),
            ..MsyncResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 2)).expect("client");

    client.get_status("/alpha").await.expect("refresh retry succeeds");
    client
        .get_status("/alpha")
        .await
        .expect("next operation carries authority");

    let calls = metadata.calls();
    assert_methods(&calls, &["GetStatus", "Msync", "GetStatus", "GetStatus"]);
    assert_eq!(call_id(&calls[0]), call_id(&calls[2]));
    assert_ne!(call_id(&calls[0]), call_id(&calls[1]));
    assert_eq!(calls[0].header.deadline_ms, calls[1].header.deadline_ms);
    assert_eq!(calls[0].header.deadline_ms, calls[2].header.deadline_ms);
    assert!(calls[0].header.state.is_empty());
    assert_eq!(
        calls[2].header.state[0].state_id.as_ref().map(|state| state.index),
        Some(1)
    );
    assert_eq!(calls[3].header.mount_epoch, Some(31));
    assert_eq!(calls[3].header.route_epoch, Some(41));
    assert_eq!(
        calls[3].header.state[0].state_id.as_ref().map(|state| state.index),
        Some(9)
    );
    server.shutdown().await;
}

#[tokio::test]
async fn reader_replans_without_advancing_position_and_rejects_local_bounds_before_io() {
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::RefreshMetadata,
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
        ]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let location = block_location(202, 0, 0, 8, worker_server.endpoint());
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([
            MetadataReply::success(open_file_response(202, 8)),
            MetadataReply::success(open_file_response(203, 9)),
        ]),
        get_block_locations: VecDeque::from([
            MetadataReply::success(locations_response(202, 8, location.clone())),
            MetadataReply::success(locations_response(202, 8, location.clone())),
            MetadataReply::success(locations_response(202, 8, location)),
        ]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let config = ClientConfig::builder()
        .client_name("public-reader-contract")
        .metadata_endpoints([metadata_server.endpoint()])
        .max_attempts(2)
        .max_read_step_bytes(3)
        .read_to_end_limit(8)
        .build()
        .expect("reader config");
    let client = FsClient::new(config).expect("client");

    let mut reader = client.open("/alpha").await.expect("open reader");
    let mut sequential = [0u8; 4];
    assert_eq!(reader.read(&mut sequential).await.expect("replanned read"), 3);
    assert_eq!(&sequential[..3], b"abc");
    assert_eq!(reader.position(), 3);

    let metadata_calls = metadata.calls();
    let layout_calls = calls_for(&metadata_calls, "GetBlockLocations");
    assert_eq!(layout_calls.len(), 2);
    assert_same_identity_and_deadline(&layout_calls);

    let mut positioned = [0u8; 3];
    assert_eq!(reader.read_at(4, &mut positioned).await.expect("positioned read"), 3);
    assert_eq!(&positioned, b"efg");
    assert_eq!(reader.position(), 3);

    let metadata_before_eof = metadata.calls().len();
    let worker_before_eof = worker.read_calls();
    let error = reader
        .read_exact_at(8, &mut [0u8; 1])
        .await
        .expect_err("exact read beyond EOF");
    assert_client_error(&error, ClientErrorKind::UnexpectedEof, false, "opened file length");
    assert_eq!(metadata.calls().len(), metadata_before_eof);
    assert_eq!(worker.read_calls(), worker_before_eof);

    let mut oversized = client.open("/oversized").await.expect("open oversized reader");
    let metadata_before_bound = metadata.calls().len();
    let worker_before_bound = worker.read_calls();
    let error = oversized.read_to_end().await.expect_err("read_to_end bound");
    assert_client_error(&error, ClientErrorKind::InvalidArgument, false, "read_to_end maximum");
    assert_eq!(metadata.calls().len(), metadata_before_bound);
    assert_eq!(worker.read_calls(), worker_before_bound);

    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn malformed_create_and_allocate_block_successes_fail_closed_before_worker_io() {
    let mut missing_layout = create_response(301, 8);
    missing_layout.layout = None;
    let mut zero_inode = create_response(301, 8);
    zero_inode.write_handle.as_mut().unwrap().inode_id = 0;
    let mut zero_epoch = create_response(301, 8);
    zero_epoch.write_handle.as_mut().unwrap().write_lease_epoch = 0;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: [missing_layout, zero_inode, zero_epoch, create_response(302, 8)]
            .into_iter()
            .map(MetadataReply::success)
            .collect(),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(302, 0, 1, "127.0.0.1:9", 8)),
            ..AllocateBlockResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");

    for field in [
        "layout missing",
        "inode_id must be non-zero",
        "write_lease_epoch must be non-zero",
    ] {
        let error = client.create("/invalid-create").await.expect_err("malformed create");
        assert_client_error(&error, ClientErrorKind::InvalidResponse, true, field);
        assert_eq!(error.operation(), Some("CreateFile"));
    }

    let mut writer = client.create("/bad-target").await.expect("valid writer");
    let add_error = writer
        .write_all(Bytes::from_static(b"x"))
        .await
        .expect_err("mismatched AllocateBlock target");
    assert_client_error(
        &add_error,
        ClientErrorKind::InvalidResponse,
        true,
        "file_offset mismatch",
    );
    assert_eq!(add_error.operation(), Some("AllocateBlock"));
    let stale = writer
        .write_all(Bytes::from_static(b"!"))
        .await
        .expect_err("unknown AllocateBlock blocks writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unknown outcome");
    assert_eq!(calls_for(&metadata.calls(), "AllocateBlock").len(), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn ambiguous_commit_response_can_only_be_recovered_by_the_same_close() {
    for internal in [false, true] {
        let first_reply = if internal {
            MetadataReply::error(RpcErrorDetail::fail(
                ErrorKind::Internal(InternalErrorKind::Internal),
                "commit completion failed",
            ))
        } else {
            MetadataReply::success(CommitFileResponseProto {
                committed_size: 1,
                ..CommitFileResponseProto::default()
            })
        };
        let metadata = MockMetadata::new(MetadataScript {
            create_file: VecDeque::from([MetadataReply::success(create_response(303, 8))]),
            commit_file: VecDeque::from([
                first_reply,
                MetadataReply::error(RpcErrorDetail::fail(
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    "receipt no longer available",
                )),
                MetadataReply::success(CommitFileResponseProto {
                    committed_size: 0,
                    ..CommitFileResponseProto::default()
                }),
            ]),
            ..MetadataScript::default()
        });
        let server = metadata.start().await;
        let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");
        let mut writer = client.create("/commit").await.expect("writer");

        let error = writer.close().await.expect_err("unconfirmed commit");
        let (kind, message) = if internal {
            (ClientErrorKind::Internal, "completion failed")
        } else {
            (ClientErrorKind::InvalidResponse, "committed_size")
        };
        assert_client_error(&error, kind, true, message);
        let error = writer
            .close()
            .await
            .expect_err("missing evidence cannot erase prior ambiguity");
        assert!(error.is_outcome_unknown());
        writer.close().await.expect("frozen close retry succeeds");

        let metadata_calls = metadata.calls();
        let calls = calls_for(&metadata_calls, "CommitFile");
        assert_eq!(calls.len(), 3);
        assert_same_call_id(&calls);
        server.shutdown().await;
    }
}

#[tokio::test]
async fn malformed_sync_response_blocks_new_writes_until_the_same_sync_resolves() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::Success]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(304, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(304, 0, 0, worker_server.endpoint(), 8)),
            ..AllocateBlockResponseProto::default()
        })]),
        sync_write: VecDeque::from([
            MetadataReply::success(SyncWriteResponseProto {
                synced_size: 3,
                generation: None,
                ..SyncWriteResponseProto::default()
            }),
            MetadataReply::success(SyncWriteResponseProto {
                synced_size: 4,
                generation: Some(1),
                ..SyncWriteResponseProto::default()
            }),
            MetadataReply::success(SyncWriteResponseProto {
                synced_size: 3,
                generation: Some(1),
                ..SyncWriteResponseProto::default()
            }),
        ]),
        abort_file_write: VecDeque::from([MetadataReply::success(AbortFileWriteResponseProto::default())]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let client = FsClient::new(client_config(metadata_server.endpoint(), 1)).expect("client");
    let mut writer = client.create("/sync").await.expect("writer");
    writer.write_all(Bytes::from_static(b"abc")).await.expect("write");

    for expected in ["generation missing", "synced_size"] {
        let error = writer.sync().await.expect_err("malformed SyncWrite response");
        assert_client_error(&error, ClientErrorKind::InvalidResponse, true, expected);
    }
    let stale = writer
        .write_all(Bytes::from_static(b"x"))
        .await
        .expect_err("unresolved sync blocks writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unresolved SyncWrite");
    writer.sync().await.expect("frozen sync retry succeeds");
    writer.abort().await.expect("abort resolved writer");

    let metadata_calls = metadata.calls();
    let calls = calls_for(&metadata_calls, "SyncWrite");
    assert_eq!(calls.len(), 3);
    assert_same_call_id(&calls);
    assert_eq!(worker.write_calls(), 1);
    assert_eq!(worker.write_data_frames(), 1);
    assert_eq!(worker.write_completions(), 1);
    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn malformed_lease_renewal_invalidates_the_writer_for_new_side_effects() {
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(305, 8))]),
        renew_lease: VecDeque::from([MetadataReply::success(RenewLeaseResponseProto {
            expires_at_ms: 0,
            ..RenewLeaseResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");
    let mut writer = client.create("/renew").await.expect("writer");

    let error = writer.renew_lease().await.expect_err("invalid renewal response");
    assert_client_error(&error, ClientErrorKind::InvalidResponse, true, "expires_at_ms");
    let stale = writer
        .write_all(Bytes::from_static(b"x"))
        .await
        .expect_err("unknown renewal blocks writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unknown outcome");
    assert_methods(&metadata.calls(), &["CreateFile", "RenewLease"]);
    server.shutdown().await;
}

#[tokio::test]
async fn worker_failure_after_ack_invalidates_the_writer_and_prevents_commit() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::AckThenUnavailable]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(306, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(306, 0, 0, worker_server.endpoint(), 8)),
            ..AllocateBlockResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let client = FsClient::new(client_config(metadata_server.endpoint(), 1)).expect("client");
    let mut writer = client.create("/worker-failure").await.expect("writer");

    let first = match writer.write_all(Bytes::from_static(b"abc")).await {
        Ok(()) => writer.sync().await.expect_err("sync observes Worker failure"),
        Err(error) => error,
    };
    assert!(first.is_outcome_unknown());
    assert_eq!(first.operation(), Some("WriteBlock"));
    let stale = writer
        .write_all(Bytes::from_static(b"x"))
        .await
        .expect_err("uncertain Worker write blocks later writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unknown outcome");
    assert!(calls_for(&metadata.calls(), "CommitFile").is_empty());
    assert_eq!(worker.write_calls(), 1);
    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn allocation_replay_and_worker_capacity_retries_keep_the_same_block() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([
            WriteReply::CapacityRejected,
            WriteReply::CapacityRejected,
            WriteReply::CapacityRejected,
            WriteReply::Success,
        ]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let allocated = AllocateBlockResponseProto {
        block: Some(write_target(307, 0, 0, worker_server.endpoint(), 8)),
        ..AllocateBlockResponseProto::default()
    };
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(307, 8))]),
        allocate_block: VecDeque::from([
            MetadataReply::status(Status::unavailable("allocation response lost")),
            MetadataReply::success(allocated.clone()),
            MetadataReply::success(allocated),
        ]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let client = FsClient::new(client_config(metadata_server.endpoint(), 3)).expect("client");
    let mut writer = client.create("/capacity").await.expect("writer");

    let error = writer
        .write_all(Bytes::from_static(b"x"))
        .await
        .expect_err("capacity attempts exhausted");
    assert_client_error(&error, ClientErrorKind::ResourceExhausted, false, "capacity exhausted");
    writer
        .write_all(Bytes::new())
        .await
        .expect("definite pre-side-effect rejection leaves writer open");
    assert_eq!(worker.write_calls(), 3);
    assert_eq!(worker.write_data_frames(), 0);
    writer
        .write_all(Bytes::from_static(b"12345678"))
        .await
        .expect("capacity recovered");
    assert_eq!(writer.cursor(), 8);
    assert_eq!(worker.write_calls(), 4);
    assert_eq!(worker.write_completions(), 1);
    let calls = metadata.calls();
    let allocations = calls_for(&calls, "AllocateBlock");
    assert_eq!(allocations.len(), 3);
    assert_same_identity_and_deadline(&allocations[..2]);
    for request in metadata.allocations() {
        assert_eq!(request.write_handle, Some(write_handle(307)));
        assert_eq!(request.previous_block_id, None);
    }
    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

fn client_config(metadata_endpoint: &str, max_attempts: usize) -> ClientConfig {
    ClientConfig::builder()
        .client_name("public-client-contract")
        .metadata_endpoints([metadata_endpoint])
        .operation_timeout(Duration::from_secs(2))
        .max_attempts(max_attempts)
        .build()
        .expect("client config")
}

fn status_response(size: u64) -> GetStatusResponseProto {
    GetStatusResponseProto {
        len: size,
        create_time: 11,
        modify_time: 12,
        kind: FileTypeProto::FileTypeFile as i32,
        ..GetStatusResponseProto::default()
    }
}

fn create_response(inode_id: u64, block_size: u32) -> CreateFileResponseProto {
    CreateFileResponseProto {
        layout: Some(file_layout(block_size)),
        write_handle: Some(write_handle(inode_id)),
        expires_at_ms: unix_now_ms() + 60_000,
        generation: 0,
        ..CreateFileResponseProto::default()
    }
}

fn open_file_response(inode_id: u64, file_size: u64) -> OpenFileResponseProto {
    OpenFileResponseProto {
        inode_id,
        file_size,
        generation: Some(3),
        ..OpenFileResponseProto::default()
    }
}

fn locations_response(
    inode_id: u64,
    file_size: u64,
    location: FileBlockLocationProto,
) -> GetBlockLocationsResponseProto {
    GetBlockLocationsResponseProto {
        inode_id,
        file_size,
        locations: vec![location],
        generation: Some(3),
        ..GetBlockLocationsResponseProto::default()
    }
}

fn block_location(
    inode_id: u64,
    block_index: u32,
    file_offset: u64,
    len: u64,
    worker_endpoint: &str,
) -> FileBlockLocationProto {
    let format = BlockFormatId::CURRENT_FOR_NEW_FILE;
    FileBlockLocationProto {
        block_id: Some(BlockIdProto { inode_id, block_index }),
        file_offset,
        len,
        workers: vec![worker(worker_endpoint)],

        block_format_id: format.as_raw(),
        block_size: 64 * 1024 * 1024,
        chunk_size: format.spec().expect("block format").storage_chunk_size,
        effective_len: len,
    }
}

fn write_target(
    inode_id: u64,
    block_index: u32,
    file_offset: u64,
    worker_endpoint: &str,
    block_size: u64,
) -> LocatedBlockProto {
    let block_id = BlockIdProto { inode_id, block_index };
    let format = BlockFormatId::CURRENT_FOR_NEW_FILE;
    LocatedBlockProto {
        write_offset: 0,
        block_id: Some(block_id),
        file_offset,
        block_format_id: format.as_raw(),
        block_size,
        chunk_size: format.spec().expect("block format").storage_chunk_size,

        worker_endpoints: vec![worker(worker_endpoint)],
        fencing_token: Some(FencingTokenProto {
            block_id: Some(block_id),
            owner: Some(ClientIdProto { high: 0, low: 7 }),
            epoch: 1,
        }),
        tier: TierProto::TierHdd as i32,
    }
}

fn worker(endpoint: &str) -> WorkerEndpointInfoProto {
    WorkerEndpointInfoProto {
        worker_id: 1,
        endpoint: endpoint.to_string(),
        worker_run_id: WORKER_RUN_ID.to_string(),
    }
}

fn file_layout(block_size: u32) -> FileLayoutProto {
    FileLayoutProto {
        block_size,
        block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
    }
}

fn write_handle(inode_id: u64) -> WriteHandleProto {
    WriteHandleProto {
        inode_id,
        write_lease_epoch: 1,
    }
}

fn watermark(index: u64) -> GroupStateWatermarkProto {
    GroupStateWatermarkProto {
        state_id: Some(RaftLogIdProto {
            term: 1,
            leader_node_id: 1,
            index,
        }),
        group_name: "root".to_string(),
    }
}

fn pre_handler_rejection() -> Status {
    let mut metadata = MetadataMap::new();
    metadata.insert(
        HEADER_PRE_HANDLER_REJECTION,
        MetadataValue::from_static(PRE_HANDLER_REJECTION_RPC_CONCURRENCY),
    );
    Status::with_metadata(
        Code::ResourceExhausted,
        "scripted Metadata capacity rejection",
        metadata,
    )
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_millis() as u64
}

fn call_id(call: &MetadataCall) -> &str {
    &call.header.client.as_ref().expect("request client").call_id
}

fn calls_for(calls: &[MetadataCall], method: &str) -> Vec<MetadataCall> {
    calls.iter().filter(|call| call.method == method).cloned().collect()
}

fn assert_methods(calls: &[MetadataCall], expected: &[&str]) {
    assert_eq!(calls.iter().map(|call| call.method).collect::<Vec<_>>(), expected);
}

fn assert_same_call_id(calls: &[MetadataCall]) {
    assert!(!calls.is_empty());
    assert!(calls.iter().all(|call| call_id(call) == call_id(&calls[0])));
}

fn assert_same_identity_and_deadline(calls: &[MetadataCall]) {
    assert!(!calls.is_empty());
    let first = &calls[0];
    assert!(calls
        .iter()
        .all(|call| { call_id(call) == call_id(first) && call.header.deadline_ms == first.header.deadline_ms }));
}

fn assert_client_error(error: &ClientError, kind: ClientErrorKind, unknown: bool, message: &str) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.is_outcome_unknown(), unknown);
    assert!(error.message().contains(message), "unexpected error: {error:?}");
}
