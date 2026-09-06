// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientError, ClientErrorKind, FileStatus};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction};
use beryl_common::header::RequestHeader;
use beryl_e2e::data::deterministic_bytes;
use beryl_e2e::{TestCluster, TestResult};
use beryl_proto::common::{ByteRangeProto, RequestHeaderProto, ResponseHeaderProto};
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::get_block_locations_request_proto::Target;
use beryl_proto::metadata::{
    AbortFileWriteRequestProto, AllocateBlockRequestProto, CommitFileRequestProto, CommittedBlockProto,
    CreateFileRequestProto, GetBlockLocationsRequestProto, LocatedBlockProto, OpenWriteModeProto,
    OpenWriteRequestProto, SyncWriteRequestProto, WriteHandleProto,
};
use beryl_proto::worker::worker_data_service_client::WorkerDataServiceClient;
use beryl_proto::worker::write_block_request_proto::Payload;
use beryl_proto::worker::{DataRequestHeaderProto, WriteBlockCommandProto, WriteBlockRequestProto};
use beryl_types::ClientId;
use bytes::Bytes;
use std::path::Path;
use tokio_stream::iter;
use tonic::Request;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_visible_file_survives_metadata_restart() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    let path = "/restart/committed";
    let payload = Bytes::from(deterministic_bytes(1_537));
    client.mkdirs("/restart").await.expect("create restart dir");
    let mut writer = client.create(path).await.expect("create file");
    writer.write_all(payload.clone()).await.expect("write file");
    writer.close().await.expect("close file");
    cluster
        .converge_block_reports()
        .await
        .expect("pre-restart report convergence");

    let before = client
        .open(path)
        .await
        .expect("open before restart")
        .read_to_end()
        .await;
    assert_eq!(before.expect("read before restart"), payload);

    cluster.restart_metadata().await.expect("restart metadata");

    let after = client.open(path).await.expect("open after restart").read_to_end().await;
    assert_eq!(after.expect("read after restart"), payload);
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_empty_create_requires_new_authority_for_noop_close() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    client.mkdirs("/restart").await.expect("create restart dir");

    let owner_client_id = 701;
    let foreign_client_id = 702;
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("connect metadata");
    let create = metadata
        .create_file(Request::new(CreateFileRequestProto {
            header: Some(metadata_header(owner_client_id)),
            path: "/restart/create-before-close".to_string(),
        }))
        .await
        .expect("create active writer")
        .into_inner();
    assert_metadata_ok(create.header);
    let close_request = CommitFileRequestProto {
        header: Some(metadata_header(owner_client_id)),
        write_handle: create.write_handle,
        committed_blocks: Vec::new(),
        final_size: 0,
        expected_generation: create.generation,
        write_mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
        expected_file_size: 0,
    };
    let mut aborted = client
        .create("/restart/create-before-abort")
        .await
        .expect("create second active writer");

    cluster.restart_metadata().await.expect("restart metadata");

    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("reconnect metadata");
    for client_id in [foreign_client_id, owner_client_id] {
        let mut request = close_request.clone();
        request.header = Some(metadata_header(client_id));
        let response = metadata.commit_file(Request::new(request)).await.unwrap().into_inner();
        let error = response
            .header
            .unwrap()
            .error
            .expect("CreateFile replay is not Commit evidence");
        assert_eq!(
            rpc_error_from_proto(&error).kind,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)
        );
    }
    let abort = metadata
        .abort_file_write(Request::new(AbortFileWriteRequestProto {
            header: Some(metadata_header(owner_client_id)),
            write_handle: create.write_handle,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_metadata_ok(abort.header);
    aborted
        .abort()
        .await
        .expect("abort reconstructs the initial durable owner");
    let mut reopened = client
        .append("/restart/create-before-close")
        .await
        .expect("new OpenWrite call establishes a new session after restart");
    reopened
        .close()
        .await
        .expect("new authority can complete a no-op close");
    let mut reopened_after_abort = client
        .append("/restart/create-before-abort")
        .await
        .expect("aborted CreateFile no longer reserves write authority");
    reopened_after_abort
        .abort()
        .await
        .expect("abort reopened second session");
    assert_no_committed_bytes(&cluster, "/restart/create-before-close")
        .await
        .expect("no committed bytes");
    assert_no_committed_bytes(&cluster, "/restart/create-before-abort")
        .await
        .expect("aborted file has no committed bytes");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_worker_ready_before_metadata_close_rejects_stale_writer_and_hides_data() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    client.mkdirs("/restart").await.expect("create restart dir");

    let mut writer = client
        .create("/restart/worker-ready-before-close")
        .await
        .expect("create active writer");
    writer
        .write_all(Bytes::from(deterministic_bytes(1024)))
        .await
        .expect("write Worker Ready block without metadata close");

    cluster.restart_metadata().await.expect("restart metadata");

    let err = writer.renew_lease().await.expect_err("stale writer must fail closed");
    assert_stale_writer_error(&err);
    assert_no_committed_bytes(&cluster, "/restart/worker-ready-before-close")
        .await
        .expect("no committed bytes");
    assert_no_metadata_locations(&cluster, "/restart/worker-ready-before-close", 1024)
        .await
        .expect("no metadata locations");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_worker_ready_before_metadata_commit_hides_unpublished_block() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let active = raw_create_worker_ready_block(&cluster, "/restart/worker-ready-no-metadata", b"worker-ready")
        .await
        .expect("write Worker Ready block without CommitFile");
    assert_eq!(cluster.ready_block_count().expect("ready blocks before restart"), 1);

    cluster.restart_metadata().await.expect("restart metadata");

    assert_stale_commit_file(&cluster, active)
        .await
        .expect("stale CommitFile must fail");
    assert_eq!(cluster.ready_block_count().expect("ready blocks after restart"), 1);
    assert_no_committed_bytes(&cluster, "/restart/worker-ready-no-metadata")
        .await
        .expect("worker-only block not visible");
    assert_no_metadata_locations(&cluster, "/restart/worker-ready-no-metadata", 11)
        .await
        .expect("worker-only block has no metadata locations");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_visible_data_remains_readable_while_active_write_fails_closed() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    client.mkdirs("/restart").await.expect("create restart dir");

    let visible_path = "/restart/existing-visible";
    let active_path = "/restart/active-hidden";
    let visible = Bytes::from_static(b"already-visible");
    let hidden = Bytes::from_static(b"hidden-after-restart");
    let mut visible_writer = client.create(visible_path).await.expect("create visible file");
    visible_writer
        .write_all(visible.clone())
        .await
        .expect("write visible file");
    visible_writer.close().await.expect("close visible file");
    cluster
        .converge_block_reports()
        .await
        .expect("visible report convergence");

    let mut active_writer = client.create(active_path).await.expect("create active file");
    active_writer
        .write_all(hidden)
        .await
        .expect("write active file without close");

    cluster.restart_metadata().await.expect("restart metadata");

    let visible_after = client
        .open(visible_path)
        .await
        .expect("open visible after restart")
        .read_to_end()
        .await
        .expect("read visible after restart");
    assert_eq!(visible_after, visible);
    let err = active_writer.close().await.expect_err("active writer must fail closed");
    assert_stale_writer_error(&err);
    assert_no_committed_bytes(&cluster, active_path)
        .await
        .expect("active path has no committed bytes");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_replay_survives_restart_and_session_operations_converge() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    cluster.client().mkdirs("/restart").await.expect("create restart dir");
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("connect metadata");
    let path = "/restart/replay";
    let create_request = CreateFileRequestProto {
        header: Some(metadata_header(801)),
        path: path.to_string(),
    };
    let create = metadata
        .create_file(Request::new(create_request.clone()))
        .await
        .expect("CreateFile")
        .into_inner();
    assert_metadata_ok(create.header);

    cluster
        .restart_metadata()
        .await
        .expect("restart metadata after CreateFile");
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("reconnect metadata");

    let mut wrong_deadline = create_request.clone();
    wrong_deadline
        .header
        .as_mut()
        .expect("CreateFile request header")
        .deadline_ms += 1;
    let mismatch = metadata
        .create_file(Request::new(wrong_deadline))
        .await
        .expect("mismatched CreateFile replay response")
        .into_inner();
    let mismatch_error = mismatch
        .header
        .expect("mismatched replay response header")
        .error
        .expect("mismatched deadline must fail");
    assert_eq!(
        rpc_error_from_proto(&mismatch_error).kind,
        ErrorKind::Protocol(ProtocolErrorKind::InvalidArgument)
    );
    let replay_create = metadata
        .create_file(Request::new(create_request))
        .await
        .expect("replayed CreateFile")
        .into_inner();
    assert_metadata_ok(replay_create.header);
    assert_eq!(replay_create.layout, create.layout);
    assert_eq!(replay_create.write_handle, create.write_handle);
    assert_eq!(replay_create.expires_at_ms, create.expires_at_ms);
    assert_eq!(replay_create.generation, create.generation);

    let write_handle = create.write_handle.expect("write handle");

    let header = metadata_header(801);
    let first = allocate_block(&mut metadata, write_handle, None, header.clone()).await;
    let replay = allocate_block(&mut metadata, write_handle, None, header.clone()).await;
    assert_eq!(replay, first);
    let first_id = first.block_id.expect("first block id");
    let second = allocate_block(&mut metadata, write_handle, Some(first_id), metadata_header(801)).await;
    assert_eq!(second.block_id.unwrap().block_index, first_id.block_index + 1);
    let historical = allocate_block(&mut metadata, write_handle, None, header).await;
    assert_eq!(historical, first);

    let abort_request = AbortFileWriteRequestProto {
        header: Some(metadata_header(801)),
        write_handle: Some(write_handle),
    };
    let first_abort = metadata
        .abort_file_write(Request::new(abort_request.clone()))
        .await
        .expect("first AbortFileWrite")
        .into_inner();
    let replay_abort = metadata
        .abort_file_write(Request::new(abort_request))
        .await
        .expect("replayed AbortFileWrite")
        .into_inner();
    assert_metadata_ok(first_abort.header);
    assert_metadata_ok(replay_abort.header);
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_index_continues_after_restart_and_more_than_ten_allocations() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    cluster.start_additional_worker().await.expect("start second worker");
    assert_eq!(cluster.current_worker_run_ids().len(), 2);
    cluster
        .start_metadata_process(Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start metadata child process");
    cluster.client().mkdirs("/restart").await.expect("create restart dir");
    let path = "/restart/many-blocks";
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("connect metadata");
    let create = metadata
        .create_file(Request::new(CreateFileRequestProto {
            header: Some(metadata_header(900)),
            path: path.to_string(),
        }))
        .await
        .expect("CreateFile")
        .into_inner();
    assert_metadata_ok(create.header);
    let old_handle = create.write_handle.expect("write handle");
    let mut previous_block_id = None;
    for index in 0..12 {
        let block = allocate_block(&mut metadata, old_handle, previous_block_id, metadata_header(900)).await;
        let block_id = block.block_id.expect("block id");
        assert_eq!(block_id.inode_id, old_handle.inode_id);
        assert_eq!(block_id.block_index, index as u32);
        previous_block_id = Some(block_id);
    }

    cluster
        .kill_metadata_process_and_restart()
        .await
        .expect("SIGKILL metadata child and restart in-process metadata");
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("reconnect metadata");
    let reopened = metadata
        .open_write(Request::new(OpenWriteRequestProto {
            header: Some(metadata_header(900)),
            path: path.to_string(),
            mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
        }))
        .await
        .expect("reopen write")
        .into_inner();
    assert_metadata_ok(reopened.header);
    let new_handle = reopened.write_handle.expect("new write handle");
    assert_eq!(new_handle.inode_id, old_handle.inode_id);
    let payload = deterministic_bytes(1024);
    let target = allocate_block(&mut metadata, new_handle, None, metadata_header(900)).await;
    let block_id = target.block_id.expect("block id after restart");
    assert_eq!(block_id.inode_id, new_handle.inode_id);
    assert_eq!(block_id.block_index, 12);
    let selected_run_id = &target
        .worker_endpoints
        .first()
        .expect("write target has a worker")
        .worker_run_id;
    assert!(
        cluster
            .current_worker_run_ids()
            .iter()
            .any(|run_id| run_id.to_string() == *selected_run_id),
        "placement must use one of the two currently registered worker runs"
    );

    write_worker_target(&target, &payload)
        .await
        .expect("write restarted target to Ready on selected worker");
    let commit = metadata
        .commit_file(Request::new(CommitFileRequestProto {
            header: Some(metadata_header(900)),
            write_handle: Some(new_handle),
            committed_blocks: vec![CommittedBlockProto {
                block_id: Some(block_id),

                len: payload.len() as u64,
            }],
            final_size: payload.len() as u64,
            expected_generation: reopened.generation,
            write_mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
            expected_file_size: reopened.base_size,
        }))
        .await
        .expect("publish restarted target")
        .into_inner();
    assert_metadata_ok(commit.header);
    cluster
        .converge_block_reports()
        .await
        .expect("converge both worker reports after publish");
    let read = cluster
        .client()
        .open(path)
        .await
        .expect("open restarted write")
        .read_to_end()
        .await
        .expect("read restarted write");
    assert_eq!(read.as_ref(), payload.as_slice());
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_commit_response_is_resolved_after_metadata_restart() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let path = "/restart/durable-publish";
    let active = raw_create_worker_ready_block(&cluster, path, b"durable-publish")
        .await
        .expect("prepare Worker Ready block");
    let request = CommitFileRequestProto {
        header: Some(metadata_header(401)),
        write_handle: Some(active.write_handle),
        committed_blocks: vec![active.committed_block],
        final_size: b"durable-publish".len() as u64,
        expected_generation: active.expected_generation,
        write_mode: active.write_mode,
        expected_file_size: active.expected_file_size,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let proxy = DropCommitResponse(
        tonic::transport::Endpoint::from_shared(cluster.metadata_endpoint())
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let proxy_task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(proxy)
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            }),
    );
    let mut metadata = FileSystemServiceProtoClient::connect(endpoint).await.unwrap();
    let error = metadata
        .commit_file(Request::new(request.clone()))
        .await
        .expect_err("proxy discards the actual commit response");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    drop(metadata);
    stop.send(()).unwrap();
    proxy_task.await.unwrap().unwrap();

    cluster.restart_metadata().await.expect("restart metadata");

    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("reconnect metadata");
    let replay = metadata
        .commit_file(Request::new(request))
        .await
        .expect("resolve completed CommitFile")
        .into_inner();
    assert_metadata_ok(replay.header);
    assert_eq!(replay.committed_size, b"durable-publish".len() as u64);
    cluster.shutdown().await.expect("shutdown cluster");
}

/// Forward the real request, then replace its response with a transport failure.
/// This keeps fault injection outside Metadata and proves replay across a restart.
#[derive(Clone)]
struct DropCommitResponse(tonic::transport::Channel);

impl tonic::server::NamedService for DropCommitResponse {
    const NAME: &'static str = "metadata.FileSystemServiceProto";
}

impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for DropCommitResponse {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        let mut upstream = self.0.clone();
        Box::pin(async move {
            std::future::poll_fn(|cx| upstream.poll_ready(cx)).await.unwrap();
            // Await the response body too: gRPC headers alone do not prove that
            // the upstream handler has finished its application operation.
            let response = upstream.call(request).await.unwrap();
            let mut body = response.into_body();
            use tonic::codegen::Body;
            while std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
                .await
                .is_some()
            {}
            Ok(tonic::Status::unavailable("injected CommitFile response loss").into_http())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_takeover_preserves_published_prefix_after_abort_or_metadata_restart() {
    for restart in [false, true] {
        let mut cluster = TestCluster::start().await.unwrap();
        let client = cluster.client().clone();
        let path = "/tail-takeover";
        let prefix = Bytes::from(vec![b'a'; 317]);
        let mut writer = client.create(path).await.unwrap();
        writer.write_all(prefix.clone()).await.unwrap();
        writer.close().await.unwrap();
        let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
            .await
            .unwrap();
        let opened = metadata
            .open_write(OpenWriteRequestProto {
                header: Some(metadata_header(901)),
                path: path.into(),
                mode: OpenWriteModeProto::OpenWriteModeAppend as i32,
            })
            .await
            .unwrap()
            .into_inner();
        assert_metadata_ok(opened.header);
        let mut tail = opened.tail_block.expect("OpenWrite reuses partial tail");
        assert_eq!(tail.write_offset, 317);
        write_worker_target(&tail, b"unpublished").await.unwrap();
        tail.write_offset += 11;
        if restart {
            cluster.restart_metadata().await.unwrap();
        } else {
            let aborted = metadata
                .abort_file_write(AbortFileWriteRequestProto {
                    header: Some(metadata_header(901)),
                    write_handle: opened.write_handle,
                })
                .await
                .unwrap()
                .into_inner();
            assert_metadata_ok(aborted.header);
        }
        assert!(
            write_worker_target(&tail, b"late").await.is_err(),
            "persisted token alone cannot authorize a new RPC"
        );
        let suffix = Bytes::from(vec![b'b'; 800]);
        let mut appender = client.append(path).await.unwrap();
        appender.write_all(suffix.clone()).await.unwrap();
        appender.close().await.unwrap();
        let actual = client.open(path).await.unwrap().read_to_end().await.unwrap();
        assert_eq!(actual.as_ref(), [prefix.as_ref(), suffix.as_ref()].concat());
        let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
            .await
            .unwrap();
        let layout = metadata
            .get_block_locations(GetBlockLocationsRequestProto {
                header: Some(metadata_header(902)),
                target: Some(Target::Path(path.into())),
                range: Some(ByteRangeProto { offset: 0, len: 1117 }),
            })
            .await
            .unwrap()
            .into_inner();
        assert_metadata_ok(layout.header);
        assert_eq!(layout.locations.len(), 2);
        assert_eq!(
            layout.locations[0].block_id, tail.block_id,
            "tail identity survives session boundaries"
        );
        assert_eq!(layout.locations[0].len, 1024);
        assert_eq!(
            cluster.physical_block_count().unwrap(),
            2,
            "no replacement or orphan tail block is allocated"
        );
        cluster.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_sync_replay_keeps_the_append_phase_and_invalidates_fresh_reads() {
    let mut cluster = TestCluster::start().await.unwrap();
    let client = cluster.client().clone();
    let path = "/overwrite-sync";
    let mut original = client.create(path).await.unwrap();
    original.write_all(Bytes::from_static(b"original")).await.unwrap();
    original.close().await.unwrap();
    let reader = client.open(path).await.unwrap();
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .unwrap();
    let opened = metadata
        .open_write(OpenWriteRequestProto {
            header: Some(metadata_header(903)),
            path: path.into(),
            mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_metadata_ok(opened.header);
    assert_eq!(opened.base_size, 8);
    assert!(opened.tail_block.is_none());
    let handle = opened.write_handle.unwrap();
    let mut target = allocate_block(&mut metadata, handle, None, metadata_header(903)).await;
    write_worker_target(&target, b"new").await.unwrap();
    let frozen = SyncWriteRequestProto {
        header: Some(metadata_header(903)),
        write_handle: Some(handle),
        committed_blocks: vec![CommittedBlockProto {
            block_id: target.block_id,
            len: 3,
        }],
        target_size: 3,
        expected_generation: opened.generation,
        expected_file_size: 8,
        write_mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
    };
    let published = metadata.sync_write(frozen.clone()).await.unwrap().into_inner();
    assert_metadata_ok(published.header);
    // Repeat the original frozen request as a client resolving an unknown response would.
    let replay = metadata.sync_write(frozen).await.unwrap().into_inner();
    assert_metadata_ok(replay.header);
    assert_eq!(replay.generation, published.generation);
    assert!(
        reader.read_at(0, &mut [0u8; 1]).await.is_err(),
        "new layout invalidates the old generation"
    );
    target.write_offset = 3;
    write_worker_target(&target, b"tail").await.unwrap();
    let committed = metadata
        .commit_file(CommitFileRequestProto {
            header: Some(metadata_header(903)),
            write_handle: Some(handle),
            committed_blocks: vec![CommittedBlockProto {
                block_id: target.block_id,
                len: 7,
            }],
            final_size: 7,
            expected_generation: published.generation.unwrap(),
            expected_file_size: 3,
            write_mode: OpenWriteModeProto::OpenWriteModeAppend as i32,
        })
        .await
        .unwrap()
        .into_inner();
    assert_metadata_ok(committed.header);
    assert_eq!(
        client.open(path).await.unwrap().read_to_end().await.unwrap(),
        b"newtail"[..]
    );
    cluster.shutdown().await.unwrap();
}

struct RawWorkerReadyWrite {
    write_handle: WriteHandleProto,
    committed_block: CommittedBlockProto,
    expected_generation: u64,
    expected_file_size: u64,
    write_mode: i32,
}

async fn raw_create_worker_ready_block(
    cluster: &TestCluster,
    path: &str,
    payload: &[u8],
) -> TestResult<RawWorkerReadyWrite> {
    let client = cluster.client();
    client.mkdirs("/restart").await.expect("create restart dir");

    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let create = metadata
        .create_file(Request::new(CreateFileRequestProto {
            header: Some(metadata_header(401)),
            path: path.to_string(),
        }))
        .await?
        .into_inner();
    assert_metadata_ok(create.header);
    let expected_generation = create.generation;
    let expected_file_size = 0;
    let write_mode = OpenWriteModeProto::OpenWriteModeWrite as i32;
    let write_handle = create.write_handle.expect("write handle");

    let target = allocate_block(&mut metadata, write_handle, None, metadata_header(401)).await;
    write_worker_target(&target, payload).await?;
    let committed_block = CommittedBlockProto {
        block_id: target.block_id,

        len: payload.len() as u64,
    };

    Ok(RawWorkerReadyWrite {
        write_handle,
        committed_block,
        expected_generation,
        expected_file_size,
        write_mode,
    })
}

async fn assert_stale_commit_file(cluster: &TestCluster, active: RawWorkerReadyWrite) -> TestResult<()> {
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let final_size = active.committed_block.len;
    let stale_commit = metadata
        .commit_file(Request::new(CommitFileRequestProto {
            header: Some(metadata_header(401)),
            write_handle: Some(active.write_handle),
            committed_blocks: vec![active.committed_block],
            final_size,
            expected_generation: active.expected_generation,
            write_mode: active.write_mode,
            expected_file_size: active.expected_file_size,
        }))
        .await?
        .into_inner();
    let err = stale_commit
        .header
        .expect("commit response header")
        .error
        .expect("stale commit error");
    let rpc_error = rpc_error_from_proto(&err);
    assert_eq!(rpc_error.kind, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));
    assert!(matches!(rpc_error.recovery, RecoveryAction::ReopenWriteSession { .. }));
    Ok(())
}

async fn allocate_block(
    metadata: &mut FileSystemServiceProtoClient<tonic::transport::Channel>,
    write_handle: WriteHandleProto,
    previous_block_id: Option<beryl_proto::common::BlockIdProto>,
    header: RequestHeaderProto,
) -> LocatedBlockProto {
    let response = metadata
        .allocate_block(Request::new(AllocateBlockRequestProto {
            header: Some(header),
            write_handle: Some(write_handle),
            previous_block_id,
        }))
        .await
        .expect("AllocateBlock transport")
        .into_inner();
    assert_metadata_ok(response.header);
    response.block.expect("allocated block")
}

async fn write_worker_target(target: &LocatedBlockProto, payload: &[u8]) -> TestResult<()> {
    let worker = target
        .worker_endpoints
        .first()
        .expect("metadata write target has worker")
        .clone();
    let endpoint = if worker.endpoint.starts_with("http://") || worker.endpoint.starts_with("https://") {
        worker.endpoint.clone()
    } else {
        format!("http://{}", worker.endpoint)
    };
    let mut worker_client = WorkerDataServiceClient::connect(endpoint).await?;
    let mut responses = worker_client
        .write_block(Request::new(iter(vec![
            WriteBlockRequestProto {
                payload: Some(Payload::Command(Box::new(WriteBlockCommandProto {
                    header: Some(data_header(501)),
                    group_name: "root".to_string(),
                    block_id: target.block_id,
                    worker_run_id: worker.worker_run_id,
                    block_format_id: target.block_format_id,
                    block_size: target.block_size,
                    chunk_size: target.chunk_size,
                    fencing_token: target.fencing_token,
                    write_offset: target.write_offset,
                    tier: target.tier,
                }))),
            },
            WriteBlockRequestProto {
                payload: Some(Payload::Data(Bytes::copy_from_slice(payload))),
            },
        ])))
        .await?
        .into_inner();
    assert!(responses.message().await?.is_some(), "worker must acknowledge staging");
    assert!(responses.message().await?.is_none(), "worker must complete after Ready");
    Ok(())
}

async fn assert_no_committed_bytes(cluster: &TestCluster, path: &str) -> TestResult<()> {
    match cluster.client().get_status(path).await {
        Ok(FileStatus { len, .. }) => {
            assert_eq!(len, 0, "{path} must not publish incomplete bytes");
        }
        Err(err) => assert_not_found(&err),
    }
    Ok(())
}

async fn assert_no_metadata_locations(cluster: &TestCluster, path: &str, len: u32) -> TestResult<()> {
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let response = metadata
        .get_block_locations(Request::new(GetBlockLocationsRequestProto {
            header: Some(metadata_header(601)),
            target: Some(Target::Path(path.to_string())),
            range: Some(ByteRangeProto { offset: 0, len }),
        }))
        .await?
        .into_inner();
    assert_metadata_ok(response.header);
    assert_eq!(response.file_size, 0);
    assert!(response.locations.is_empty(), "{path} returned locations");
    Ok(())
}

fn metadata_header(client_id: u128) -> RequestHeaderProto {
    let mut header: RequestHeaderProto = (&RequestHeader::new(ClientId::new(client_id))).into();
    header.group_name = "root".to_string();
    header
}

fn data_header(client_id: u128) -> DataRequestHeaderProto {
    let header = RequestHeader::new(ClientId::new(client_id));
    DataRequestHeaderProto {
        client: Some((&header.client).into()),
        trace_context: None,
    }
}

#[track_caller]
fn assert_metadata_ok(header: Option<ResponseHeaderProto>) {
    let error = header.expect("metadata response header").error;
    assert!(error.is_none(), "metadata response carried business error: {error:?}");
}

fn assert_stale_writer_error(err: &ClientError) {
    assert!(
        matches!(
            err.kind(),
            ClientErrorKind::SessionInvalid
                | ClientErrorKind::SessionExpired
                | ClientErrorKind::Fenced
                | ClientErrorKind::StaleHandle
        ),
        "expected stale writer error, got {err:?}"
    );
}

fn assert_not_found(err: &ClientError) {
    assert_eq!(err.kind(), ClientErrorKind::NotFound, "unexpected error: {err:?}");
}
