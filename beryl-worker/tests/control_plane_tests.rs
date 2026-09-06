// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_common::error::rpc::RpcErrorDetail;
use beryl_proto::common::ResponseHeaderProto;
use beryl_proto::convert::rpc_error_to_proto;
use beryl_proto::metadata::block_report_request_proto::Batch;
use beryl_proto::metadata::delta_block_report_entry_proto::Block;
use beryl_proto::metadata::metadata_worker_service_proto_server::{
    MetadataWorkerServiceProto, MetadataWorkerServiceProtoServer,
};
use beryl_proto::metadata::{
    BlockCleanupCommandProto, BlockReportKindProto, BlockReportRequestProto, BlockReportResponseProto,
    HeartbeatRequestProto, HeartbeatResponseProto, RegisterWorkerRequestProto, RegisterWorkerResponseProto,
    ReportedBlockStateProto,
};
use beryl_proto::worker::worker_data_service_server::WorkerDataService;
use beryl_proto::worker::ReadBlockRequestProto;
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
use beryl_types::layout::BlockFormatId;
use beryl_types::{GroupName, Tier, WorkerRunId};
use beryl_worker::config::{StoreDirConfig, WorkerConfig, WorkerRegistrationConfig};
use beryl_worker::control::{
    BlockCleanupOptions, BlockCleanupRuntime, BlockReportError, BlockReportOptions, HeartbeatSnapshot,
    MetadataBlockReportLoop, MetadataHeartbeatLoop, Registration, RegistrationDescriptor, RegistrationSet,
};
use beryl_worker::net::protocol::WorkerNetProtocol;
use beryl_worker::net::server::grpc::WorkerDataServiceImpl;
use beryl_worker::store::block::{
    CheckpointBlockRequest, ChecksumKind, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
    OpenBlockWriteRequest, ReclaimBlockRequest,
};
use beryl_worker::store::dirs::StoreDirs;
use beryl_worker::{ReclaimBlockResult, WorkerCore};
use bytes::Bytes;
use futures::StreamExt;
use std::collections::{BTreeMap, VecDeque};
use std::io::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::oneshot::Sender;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

const BLOCK_SIZE: u64 = 4096;

fn chunk_size() -> u32 {
    BlockFormatId::DURABLE_PREFIX.spec().unwrap().storage_chunk_size
}

fn block_id() -> BlockId {
    BlockId::new(InodeId::new(7), BlockIndex::new(3))
}

fn group_name() -> GroupName {
    GroupName::parse("root").expect("test group name is valid")
}

#[derive(Clone)]
enum MockRegisterReply {
    Echo,
}

#[derive(Clone)]
enum MockHeartbeatReply {
    Ok {
        worker_id: u64,
        worker_run_id: WorkerRunId,
    },
    OkWithCleanup {
        worker_id: u64,
        worker_run_id: WorkerRunId,
        cleanup_commands: Vec<BlockCleanupCommandProto>,
    },
}

#[derive(Clone)]
enum MockBlockReportReply {
    Ok,
    Response {
        report_kind: i32,
        baseline_seq: u64,
        next_batch_seq: u64,
        baseline_published: bool,
    },
    AppliedThenStatus(Status),
    Status(Status),
}

#[derive(Default)]
struct MockFullReportState {
    worker_run_id: Option<String>,
    baseline_seq: Option<u64>,
    next_batch_seq: u64,
    baseline_published: bool,
}

#[derive(Default)]
struct MockMetadataState {
    replies: Mutex<VecDeque<MockRegisterReply>>,
    heartbeat_replies: Mutex<VecDeque<MockHeartbeatReply>>,
    block_report_replies: Mutex<VecDeque<MockBlockReportReply>>,
    requests: Mutex<Vec<RegisterWorkerRequestProto>>,
    heartbeat_requests: Mutex<Vec<HeartbeatRequestProto>>,
    block_report_requests: Mutex<Vec<BlockReportRequestProto>>,
    full_report: Mutex<MockFullReportState>,
}

#[derive(Clone)]
struct MockMetadataWorkerService {
    state: Arc<MockMetadataState>,
}

#[tonic::async_trait]
impl MetadataWorkerServiceProto for MockMetadataWorkerService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequestProto>,
    ) -> Result<Response<RegisterWorkerResponseProto>, Status> {
        let request = request.into_inner();
        self.state.requests.lock().unwrap().push(request.clone());
        let reply = self
            .state
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("mock register reply");

        match reply {
            MockRegisterReply::Echo => Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(response_header_from_request(&request, None)),
                worker_id: request.worker_id,
                accepted_worker_run_id: request.worker_run_id,
            })),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let request = request.into_inner();
        self.state.heartbeat_requests.lock().unwrap().push(request.clone());
        let reply = self
            .state
            .heartbeat_replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(MockHeartbeatReply::Ok {
                worker_id: request.worker_id,
                worker_run_id: WorkerRunId::parse(&request.worker_run_id).unwrap_or_else(|_| test_worker_run_id()),
            });

        match reply {
            MockHeartbeatReply::Ok {
                worker_id,
                worker_run_id,
            } => Ok(Response::new(HeartbeatResponseProto {
                header: Some(response_header_from_heartbeat_request(&request, None)),
                worker_id,
                accepted_worker_run_id: worker_run_id.to_string(),
                liveness_timeout_ms: 5_000,
                cleanup_commands: Vec::new(),
            })),
            MockHeartbeatReply::OkWithCleanup {
                worker_id,
                worker_run_id,
                cleanup_commands,
            } => Ok(Response::new(HeartbeatResponseProto {
                header: Some(response_header_from_heartbeat_request(&request, None)),
                worker_id,
                accepted_worker_run_id: worker_run_id.to_string(),
                liveness_timeout_ms: 5_000,
                cleanup_commands,
            })),
        }
    }

    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let request = request.into_inner();
        self.state.block_report_requests.lock().unwrap().push(request.clone());
        let reply = self
            .state
            .block_report_replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(MockBlockReportReply::Ok);

        match reply {
            MockBlockReportReply::Ok => {
                let (report_kind, next_batch_seq, baseline_published) = apply_block_report(&self.state, &request);
                Ok(Response::new(BlockReportResponseProto {
                    header: Some(response_header_from_block_report_request(&request, None)),
                    report_kind: report_kind as i32,
                    baseline_seq: request.baseline_seq,
                    next_batch_seq,
                    baseline_published,
                }))
            }
            MockBlockReportReply::Response {
                report_kind,
                baseline_seq,
                next_batch_seq,
                baseline_published,
            } => Ok(Response::new(BlockReportResponseProto {
                header: Some(response_header_from_block_report_request(&request, None)),
                report_kind,
                baseline_seq,
                next_batch_seq,
                baseline_published,
            })),
            MockBlockReportReply::AppliedThenStatus(status) => {
                apply_block_report(&self.state, &request);
                Err(status)
            }
            MockBlockReportReply::Status(status) => Err(status),
        }
    }
}

fn apply_block_report(
    state: &MockMetadataState,
    request: &BlockReportRequestProto,
) -> (BlockReportKindProto, u64, bool) {
    match request.batch.as_ref().expect("mock block report kind") {
        Batch::FullReport(full) => {
            let mut accepted = state.full_report.lock().unwrap();
            if accepted.worker_run_id.as_deref() != Some(request.worker_run_id.as_str())
                || accepted.baseline_seq != Some(request.baseline_seq)
            {
                assert_eq!(full.batch_seq, 0, "a new mock Full baseline must start at batch zero");
                *accepted = MockFullReportState {
                    worker_run_id: Some(request.worker_run_id.clone()),
                    baseline_seq: Some(request.baseline_seq),
                    next_batch_seq: 0,
                    baseline_published: false,
                };
            }
            if !accepted.baseline_published {
                if full.batch_seq == accepted.next_batch_seq {
                    accepted.next_batch_seq += 1;
                    accepted.baseline_published = full.final_batch;
                } else {
                    assert!(
                        full.batch_seq < accepted.next_batch_seq,
                        "mock Full report received a batch gap"
                    );
                }
            }
            (
                BlockReportKindProto::BlockReportKindFull,
                accepted.next_batch_seq,
                accepted.baseline_published,
            )
        }
        Batch::DeltaReport(delta) => (BlockReportKindProto::BlockReportKindDelta, delta.batch_seq + 1, true),
    }
}

fn response_header_from_request(
    request: &RegisterWorkerRequestProto,
    error: Option<RpcErrorDetail>,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.header.as_ref().and_then(|header| header.client.clone()),
        error: error.as_ref().map(rpc_error_to_proto),
        state: Vec::new(),
        group_name: request
            .header
            .as_ref()
            .map(|header| header.group_name.clone())
            .unwrap_or_default(),
        mount_epoch: None,
        route_epoch: None,
    }
}

fn response_header_from_heartbeat_request(
    request: &HeartbeatRequestProto,
    error: Option<RpcErrorDetail>,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.header.as_ref().and_then(|header| header.client.clone()),
        error: error.as_ref().map(rpc_error_to_proto),
        state: Vec::new(),
        group_name: request
            .header
            .as_ref()
            .map(|header| header.group_name.clone())
            .unwrap_or_default(),
        mount_epoch: None,
        route_epoch: None,
    }
}

fn response_header_from_block_report_request(
    request: &BlockReportRequestProto,
    error: Option<RpcErrorDetail>,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.header.as_ref().and_then(|header| header.client.clone()),
        error: error.as_ref().map(rpc_error_to_proto),
        state: Vec::new(),
        group_name: request
            .header
            .as_ref()
            .map(|header| header.group_name.clone())
            .unwrap_or_default(),
        mount_epoch: None,
        route_epoch: None,
    }
}

async fn start_mock_metadata(replies: Vec<MockRegisterReply>) -> (String, Arc<MockMetadataState>, Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock metadata");
    let addr = listener.local_addr().expect("mock metadata local addr");
    let state = Arc::new(MockMetadataState {
        replies: Mutex::new(VecDeque::from(replies)),
        heartbeat_replies: Mutex::new(VecDeque::new()),
        block_report_replies: Mutex::new(VecDeque::new()),
        requests: Mutex::new(Vec::new()),
        heartbeat_requests: Mutex::new(Vec::new()),
        block_report_requests: Mutex::new(Vec::new()),
        full_report: Mutex::new(MockFullReportState::default()),
    });
    let service = MockMetadataWorkerService {
        state: Arc::clone(&state),
    };
    let incoming = futures::stream::try_unfold(listener, |listener| async move {
        let (stream, _) = listener.accept().await?;
        Ok::<_, Error>(Some((stream, listener)))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .add_service(MetadataWorkerServiceProtoServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("mock metadata server");
    });

    (format!("http://{addr}"), state, shutdown_tx)
}

async fn start_mock_metadata_with_block_reports(
    replies: Vec<MockBlockReportReply>,
) -> (String, Arc<MockMetadataState>, Sender<()>) {
    let (endpoint, state, shutdown) = start_mock_metadata(Vec::new()).await;
    *state.block_report_replies.lock().unwrap() = VecDeque::from(replies);
    (endpoint, state, shutdown)
}

fn test_registration_config(endpoint: String) -> WorkerRegistrationConfig {
    WorkerRegistrationConfig {
        group_name: group_name(),
        endpoints: vec![endpoint],
        request_timeout_ms: 1_000,
        retry_initial_backoff_ms: 1,
        retry_max_backoff_ms: 1,
    }
}

#[test]
fn worker_metadata_config_requires_one_leader_endpoint() {
    let mut config = WorkerRegistrationConfig::default();
    config.endpoints.clear();
    assert!(config.validate().is_err());

    config.endpoints = vec![
        "http://127.0.0.1:18080".to_string(),
        "http://127.0.0.1:18081".to_string(),
    ];
    assert!(config.validate().is_err());
}

fn test_worker_run_id() -> WorkerRunId {
    "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
}

fn test_registration_descriptor(worker_run_id: WorkerRunId) -> RegistrationDescriptor {
    RegistrationDescriptor {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        endpoint_host: "127.0.0.1".to_string(),
        endpoint_port: 9090,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        worker_net_protocol: WorkerNetProtocol::Grpc,
    }
}

fn payload() -> Bytes {
    Bytes::from((0..BLOCK_SIZE).map(|idx| (idx % 251) as u8).collect::<Vec<_>>())
}

fn report_store(temp: &TempDir) -> Arc<StoreDirs> {
    Arc::new(
        StoreDirs::open(
            BTreeMap::from([(
                "hdd0".to_string(),
                StoreDirConfig {
                    path: temp.path().join("hdd0"),
                    tier: Tier::Hdd,
                    capacity_bytes: 64 * 1024 * 1024,
                },
            )]),
            0,
            30_000,
        )
        .expect("open report store"),
    )
}

fn test_worker_core(store: Arc<StoreDirs>) -> Arc<WorkerCore> {
    Arc::new(WorkerCore::with_local_store(1024, 1024, store))
}

fn publish_ready_block_for(
    store: &(impl LocalBlockStore + ?Sized),
    group_name: GroupName,
    block_id: BlockId,
    data: Bytes,
    lease_epoch: u64,
) {
    let token = beryl_types::FencingToken::new(
        block_id,
        beryl_types::ClientId::new(9),
        beryl_types::LeaseEpoch::new(lease_epoch),
    );
    store
        .open_block_write(OpenBlockWriteRequest {
            fencing_token: token,
            write_offset: 0,
            visible_len: 0,
            group_name: group_name.clone(),
            block_id,
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::DURABLE_PREFIX,
            chunk_size: chunk_size(),
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        })
        .expect("create staging block");
    store
        .write_at(&group_name, block_id, 0, data.clone())
        .expect("write block");
    store
        .checkpoint_block(CheckpointBlockRequest {
            group_name,
            block_id,
            effective_len: data.len() as u64,
            fencing_token: token,
        })
        .expect("publish ready block");
}

async fn wait_for_block_report_requests(mock: &MockMetadataState, expected: usize, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            if mock.block_report_requests.lock().unwrap().len() >= expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected} block report requests"));
}

fn assert_same_report_retry(first: &BlockReportRequestProto, retry: &BlockReportRequestProto) {
    let mut first = first.clone();
    let mut retry = retry.clone();
    first.header.as_mut().expect("request header").deadline_ms = 0;
    retry.header.as_mut().expect("request header").deadline_ms = 0;
    assert_eq!(first, retry, "retry may refresh only the RPC deadline");
}

async fn wait_for_block_report_absent(mock: &MockMetadataState, expected_block_id: BlockId, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            let found = mock.block_report_requests.lock().unwrap().iter().any(|request| {
                let Some(Batch::DeltaReport(delta)) = request.batch.as_ref() else {
                    return false;
                };
                delta.entries.iter().any(|entry| {
                    matches!(
                        entry.block.as_ref(),
                        Some(Block::Absent(block_id))
                            if BlockId::try_from(*block_id) == Ok(expected_block_id)
                    )
                })
            });
            if found {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for absent Delta entry for {expected_block_id}"));
}

#[cfg(unix)]
struct WorkerProcess {
    child: Child,
}

#[cfg(unix)]
impl WorkerProcess {
    fn start(config_path: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_beryl-worker"))
            .arg("start")
            .arg("--config")
            .arg(config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start Worker process");
        Self { child }
    }

    fn send_signal(&self, signal: i32) {
        let pid = self.child.id().expect("Worker process id");
        assert_eq!(unsafe { libc::kill(pid as i32, signal) }, 0);
    }

    async fn wait_successfully(mut self) {
        let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .expect("Worker process shutdown must be bounded")
            .expect("wait for Worker process");
        assert!(status.success(), "Worker process must exit successfully: {status}");
    }

    async fn signal_and_wait(self, signal: i32) {
        self.send_signal(signal);
        self.wait_successfully().await;
    }
}

#[cfg(unix)]
async fn wait_for_worker_http_status(address: SocketAddr, path: &str, status: &[u8]) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(mut stream) = TcpStream::connect(address).await {
                let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
                stream.write_all(request.as_bytes()).await.ok();
                let mut response = Vec::new();
                stream.read_to_end(&mut response).await.ok();
                if response.starts_with(status) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Worker HTTP status must become available");
}

#[cfg(unix)]
async fn request_http_keep_alive(stream: &mut TcpStream, path: &str) -> Vec<u8> {
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write HTTP request");
    let mut response = Vec::new();
    loop {
        let mut chunk = [0u8; 1024];
        let count = stream.read(&mut chunk).await.expect("read HTTP response");
        assert_ne!(count, 0, "HTTP connection closed before a complete response");
        response.extend_from_slice(&chunk[..count]);
        let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let body_start = header_end + 4;
        let headers = std::str::from_utf8(&response[..header_end]).expect("HTTP response headers are UTF-8");
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("valid content length"))
                })
            })
            .expect("HTTP response has content length");
        if response.len() >= body_start + content_length {
            response.truncate(body_start + content_length);
            return response;
        }
    }
}

#[cfg(unix)]
fn worker_process_config(endpoint: &str) -> (TempDir, PathBuf, SocketAddr, WorkerConfig) {
    let temp = TempDir::new().expect("worker process tempdir");
    let rpc_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve Worker RPC port");
    let rpc_addr = rpc_listener.local_addr().unwrap();
    let http_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve Worker HTTP port");
    let http_addr = http_listener.local_addr().unwrap();
    let identity_path = temp.path().join("worker.identity");
    let store_path = temp.path().join("hdd0");
    let config_path = temp.path().join("worker.yaml");
    let config_yaml = format!(
        r#"beryl.cluster.id: "process-shutdown"
beryl.worker.identity-file: {identity_path:?}
beryl.worker.host: "127.0.0.1"
beryl.worker.bind-host: "127.0.0.1"
beryl.worker.rpc.port: {rpc_port}
beryl.worker.rpc.max-concurrent-read-requests: 8
beryl.worker.rpc.max-concurrent-write-requests: 8
beryl.worker.http.port: {http_port}
beryl.worker.metadata.addresses: [{endpoint:?}]
beryl.worker.metadata.request-timeout: 1s
beryl.worker.metadata.retry.initial-backoff: 10ms
beryl.worker.metadata.retry.max-backoff: 100ms
beryl.worker.heartbeat.interval: 20ms
beryl.worker.block.report.delta-flush-interval: 20ms
beryl.worker.block.report.batch-size: 100
beryl.worker.block.cleanup.queue-capacity: 16
beryl.worker.block.cleanup.concurrency: 2
beryl.worker.block.cleanup.retry.initial-backoff: 10ms
beryl.worker.block.cleanup.retry.max-backoff: 100ms
beryl.worker.stream.frame-size: 1KiB
beryl.worker.stream.max-frame-size: 4KiB
beryl.worker.storage.dirs:
  hdd0:
    path: {store_path:?}
    tier: hdd
    capacity: 64MiB
beryl.worker.storage.reserved-space: 1MiB
beryl.worker.storage.check-interval: 1s
beryl.worker.shutdown.timeout: 200ms
beryl.logging.format: compact
beryl.logging.output: stderr
beryl.logging.level: warn
"#,
        identity_path = identity_path.to_string_lossy(),
        store_path = store_path.to_string_lossy(),
        rpc_port = rpc_addr.port(),
        http_port = http_addr.port(),
    );
    std::fs::write(&config_path, config_yaml).expect("write Worker process config");
    drop(rpc_listener);
    drop(http_listener);
    let config = WorkerConfig::load(&config_path).expect("load Worker process config");
    (temp, config_path, http_addr, config)
}

#[cfg(unix)]
async fn wait_for_full_report_count(mock: &MockMetadataState, block_id: BlockId, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count = mock
                .block_report_requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| {
                    let Some(Batch::FullReport(full)) = request.batch.as_ref() else {
                        return false;
                    };
                    full.blocks.iter().any(|block| {
                        block
                            .block_id
                            .is_some_and(|reported| BlockId::try_from(reported) == Ok(block_id))
                    })
                })
                .count();
            if count >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Worker must report recovered Ready block");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_signals_exit_cleanly_and_restart_reports_current_blocks() {
    let (endpoint, mock, metadata_shutdown) =
        start_mock_metadata(vec![MockRegisterReply::Echo, MockRegisterReply::Echo]).await;
    let (_temp, config_path, http_addr, config) = worker_process_config(&endpoint);
    beryl_worker::control::prepare_worker_start(&config).expect("format Worker storage");
    let store = StoreDirs::open(
        config.store.dirs.clone(),
        config.store.reserve_space_bytes,
        config.store.check_interval_ms,
    )
    .expect("open Worker process store");
    let ready_block = block_id();
    publish_ready_block_for(&store, group_name(), ready_block, payload(), 101);
    drop(store);

    let first = WorkerProcess::start(&config_path);
    wait_for_worker_http_status(http_addr, "/ready", b"HTTP/1.1 200").await;
    wait_for_full_report_count(&mock, ready_block, 1).await;
    let mut held_connection = TcpStream::connect(http_addr)
        .await
        .expect("open accepted HTTP connection");
    held_connection
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let mut readiness_connection = TcpStream::connect(http_addr)
        .await
        .expect("open readiness connection before shutdown");
    let ready_response = request_http_keep_alive(&mut readiness_connection, "/ready").await;
    assert!(ready_response.starts_with(b"HTTP/1.1 200"));
    first.send_signal(libc::SIGTERM);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let response = request_http_keep_alive(&mut readiness_connection, "/ready").await;
            if response.starts_with(b"HTTP/1.1 503") {
                return;
            }
            assert!(response.starts_with(b"HTTP/1.1 200"));
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("readiness must close before accepted HTTP work drains");
    first.wait_successfully().await;
    drop(held_connection);
    drop(readiness_connection);

    let second = WorkerProcess::start(&config_path);
    wait_for_worker_http_status(http_addr, "/ready", b"HTTP/1.1 200").await;
    wait_for_full_report_count(&mock, ready_block, 2).await;
    second.signal_and_wait(libc::SIGINT).await;

    let registrations = mock.requests.lock().unwrap();
    assert_eq!(registrations.len(), 2);
    assert_ne!(registrations[0].worker_run_id, registrations[1].worker_run_id);
    metadata_shutdown.send(()).ok();
}

#[tokio::test]
async fn heartbeat_cleanup_command_reports_deleting_then_delta_absent() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(Vec::new()).await;
    *mock.heartbeat_replies.lock().unwrap() = VecDeque::from([MockHeartbeatReply::OkWithCleanup {
        worker_id: 42,
        worker_run_id,
        cleanup_commands: vec![
            BlockCleanupCommandProto {
                block_id: Some(block_id().into()),
            },
            BlockCleanupCommandProto {
                block_id: Some(block_id().into()),
            },
        ],
    }]);
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));

    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let core = test_worker_core(Arc::clone(&store));
    let cleanup_runtime = BlockCleanupRuntime::start(
        Arc::clone(&core),
        Arc::clone(&state),
        BlockCleanupOptions {
            max_pending: 4,
            max_concurrent: 1,
            retry_initial_backoff: Duration::from_millis(10),
            retry_max_backoff: Duration::from_millis(10),
        },
    )
    .expect("cleanup executor");
    let cleanup = cleanup_runtime.executor();
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint.clone()),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        cleanup,
    )
    .expect("heartbeat loop");
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        Arc::clone(&core),
    )
    .expect("block reporter");

    let service = WorkerDataServiceImpl::new(
        Arc::clone(&core),
        Arc::clone(&state),
        64,
        32,
        &test_registration_config("http://127.0.0.1:1".into()),
    )
    .unwrap();
    let read = service
        .read_block(Request::new(ReadBlockRequestProto {
            header: None,
            group_name: group_name().to_string(),
            block_id: Some(block_id().into()),
            worker_run_id: worker_run_id.to_string(),
            byte_range: Some(ByteRange { offset: 0, len: 1 }.into()),

            block_format_id: BlockFormatId::DURABLE_PREFIX.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: chunk_size(),
            effective_len: BLOCK_SIZE,
            frame_size: 1024,
        }))
        .await
        .expect("start pinned read")
        .into_inner();
    reporter.send_full_once().await.expect("publish Ready baseline");
    heartbeat
        .send_once(HeartbeatSnapshot::default())
        .await
        .expect("accept cleanup heartbeat");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let round = reporter.send_delta_once().await.expect("send Deleting delta");
            if round.accepted_peers > 0
                && latest_delta_has_present_state(&mock, ReportedBlockStateProto::ReportedBlockStateDeleting)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must become Deleting while the read pin is active");

    let chunks = read
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("finish pinned read");
    assert_eq!(chunks.len(), 1);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if store.report().expect("store report").dirs[0].block_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must delete the local block after the reader exits");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let round = reporter.send_delta_once().await.expect("send absent Delta entry");
            if round.accepted_peers > 0 && latest_delta_has_absent(&mock, block_id()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must publish an absent entry after physical deletion completes");
    shutdown.send(()).ok();
}

fn latest_delta_has_present_state(mock: &MockMetadataState, expected_state: ReportedBlockStateProto) -> bool {
    let requests = mock.block_report_requests.lock().unwrap();
    let Some(request) = requests.last() else {
        return false;
    };
    let Some(Batch::DeltaReport(delta)) = request.batch.as_ref() else {
        return false;
    };
    delta.entries.iter().any(|entry| {
        matches!(
            entry.block.as_ref(),
            Some(Block::Present(block)) if block.state() == expected_state
        )
    })
}

fn latest_delta_has_absent(mock: &MockMetadataState, expected_block_id: BlockId) -> bool {
    let requests = mock.block_report_requests.lock().unwrap();
    let Some(request) = requests.last() else {
        return false;
    };
    let Some(Batch::DeltaReport(delta)) = request.batch.as_ref() else {
        return false;
    };
    delta.entries.iter().any(|entry| {
        matches!(
            entry.block.as_ref(),
            Some(Block::Absent(block_id))
                if BlockId::try_from(*block_id) == Ok(expected_block_id)
        )
    })
}

#[tokio::test]
async fn block_report_loop_sends_coalesced_present_and_absent_entries_on_store_changes() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    let first = BlockId::new(InodeId::new(7), BlockIndex::new(0));
    let second = BlockId::new(InodeId::new(7), BlockIndex::new(1));
    let core = test_worker_core(Arc::clone(&store));
    let reporter = MetadataBlockReportLoop::with_options_and_delta_flush_interval(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        Arc::clone(&core),
        BlockReportOptions::default(),
        Duration::from_millis(20),
    )
    .expect("block reporter");
    let reporter_handle = reporter.spawn();
    wait_for_block_report_requests(&mock, 1, Duration::from_millis(500)).await;

    publish_ready_block_for(store.as_ref(), group_name(), first, payload(), 101);
    publish_ready_block_for(store.as_ref(), group_name(), second, payload(), 102);
    wait_for_block_report_requests(&mock, 2, Duration::from_millis(500)).await;

    {
        let requests = mock.block_report_requests.lock().unwrap();
        assert!(matches!(requests[0].batch.as_ref(), Some(Batch::FullReport(_))));
        let Some(Batch::DeltaReport(delta)) = requests[1].batch.as_ref() else {
            panic!("expected event-driven delta report");
        };
        assert_eq!(delta.entries.len(), 2);
        assert!(delta
            .entries
            .iter()
            .all(|entry| matches!(entry.block.as_ref(), Some(Block::Present(_)))));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        mock.block_report_requests.lock().unwrap().len(),
        2,
        "an accepted baseline must not trigger periodic Full reports across later flush ticks"
    );

    assert_eq!(
        core.reclaim_block(ReclaimBlockRequest {
            group_name: group_name(),
            block_id: first,
        })
        .await
        .expect("reclaim Ready block"),
        ReclaimBlockResult::Deleted {
            effective_len: BLOCK_SIZE
        }
    );
    wait_for_block_report_absent(&mock, first, Duration::from_millis(500)).await;

    reporter_handle.abort();
    shutdown.send(()).ok();
}

#[tokio::test]
async fn result_unknown_retries_immutable_batches_and_preserves_newer_changes() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(vec![
        MockBlockReportReply::Ok,
        MockBlockReportReply::AppliedThenStatus(Status::unavailable("full acknowledgement lost")),
        MockBlockReportReply::Ok,
        MockBlockReportReply::Ok,
        MockBlockReportReply::Status(Status::unavailable("delta unavailable")),
        MockBlockReportReply::Ok,
        MockBlockReportReply::Ok,
    ])
    .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    let first = BlockId::new(InodeId::new(7), BlockIndex::new(0));
    let second = BlockId::new(InodeId::new(7), BlockIndex::new(1));
    let third = BlockId::new(InodeId::new(7), BlockIndex::new(2));
    let fourth = BlockId::new(InodeId::new(7), BlockIndex::new(3));
    publish_ready_block_for(store.as_ref(), group_name(), first, payload(), 101);
    publish_ready_block_for(store.as_ref(), group_name(), second, payload(), 102);
    publish_ready_block_for(store.as_ref(), group_name(), third, payload(), 103);
    let reporter = MetadataBlockReportLoop::with_options(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        test_worker_core(Arc::clone(&store)),
        BlockReportOptions {
            full_max_blocks_per_batch: 1,
            delta_max_entries_per_batch: 1,
        },
    )
    .expect("block reporter");

    reporter
        .send_full_once()
        .await
        .expect_err("second Full batch result is unknown");
    publish_ready_block_for(store.as_ref(), group_name(), fourth, payload(), 104);
    assert_eq!(reporter.send_full_once().await.unwrap().accepted_peers, 1);
    reporter
        .send_delta_once()
        .await
        .expect_err("first Delta result is unknown");
    assert!(matches!(
        store.reclaim_block(&ReclaimBlockRequest {
            group_name: group_name(),
            block_id: fourth,
        }),
        Ok(ReclaimBlockResult::Deleted { .. })
    ));
    assert_eq!(reporter.send_delta_once().await.unwrap().accepted_peers, 1);
    assert_eq!(reporter.send_delta_once().await.unwrap().accepted_peers, 1);

    {
        let requests = mock.block_report_requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        assert_same_report_retry(&requests[0], &requests[2]);
        let Some(Batch::FullReport(full)) = requests[0].batch.as_ref() else {
            panic!("expected Full request");
        };
        assert_eq!(full.batch_seq, 0);
        assert_eq!(full.blocks.len(), 1);
        assert_eq!(full.blocks[0].block_id.map(BlockId::try_from), Some(Ok(first)));
        let Some(Batch::FullReport(full)) = requests[1].batch.as_ref() else {
            panic!("expected second Full batch");
        };
        assert_eq!(full.batch_seq, 1);
        assert_eq!(full.blocks[0].block_id.map(BlockId::try_from), Some(Ok(second)));
        let Some(Batch::FullReport(full)) = requests[3].batch.as_ref() else {
            panic!("expected recovery to continue from Metadata's acknowledged cursor");
        };
        assert_eq!(full.batch_seq, 2);
        assert_eq!(full.blocks[0].block_id.map(BlockId::try_from), Some(Ok(third)));
        assert_same_report_retry(&requests[4], &requests[5]);
        let Some(Batch::DeltaReport(delta)) = requests[4].batch.as_ref() else {
            panic!("expected Delta request");
        };
        assert!(delta.entries.iter().any(|entry| {
            matches!(
                entry.block.as_ref(),
                Some(Block::Present(block))
                    if block.block_id.map(BlockId::try_from) == Some(Ok(fourth))
            )
        }));
        let Some(Batch::DeltaReport(delta)) = requests[6].batch.as_ref() else {
            panic!("expected retained newer Delta request");
        };
        assert!(matches!(
            delta.entries[0].block.as_ref(),
            Some(Block::Absent(block_id))
                if BlockId::try_from(*block_id) == Ok(fourth)
        ));
    }

    shutdown.send(()).ok();
}

#[tokio::test]
async fn block_report_rejects_responses_that_do_not_confirm_the_request() {
    let full_kind = BlockReportKindProto::BlockReportKindFull as i32;
    let delta_kind = BlockReportKindProto::BlockReportKindDelta as i32;
    let response = |report_kind, baseline_seq, next_batch_seq, baseline_published| MockBlockReportReply::Response {
        report_kind,
        baseline_seq,
        next_batch_seq,
        baseline_published,
    };
    let invalid_full_responses = [
        (response(i32::MAX, 1, 1, false), "unknown report_kind"),
        (response(delta_kind, 1, 1, true), "requested report kind"),
        (response(full_kind, 2, 1, false), "baseline_seq"),
        (response(full_kind, 1, 0, false), "expected next_batch_seq>=1"),
        (response(full_kind, 1, 3, false), "invalid next_batch_seq 3"),
    ];
    let mut replies = invalid_full_responses
        .iter()
        .map(|(reply, _)| reply.clone())
        .collect::<Vec<_>>();
    replies.extend([
        MockBlockReportReply::Ok,
        MockBlockReportReply::Ok,
        response(delta_kind, 1, 1, false),
    ]);
    let worker_run_id = test_worker_run_id();
    let (endpoint, _mock, shutdown) = start_mock_metadata_with_block_reports(replies).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    for (block_index, lease_epoch) in [(0, 101), (1, 102)] {
        publish_ready_block_for(
            store.as_ref(),
            group_name(),
            BlockId::new(InodeId::new(7), BlockIndex::new(block_index)),
            payload(),
            lease_epoch,
        );
    }
    let reporter = MetadataBlockReportLoop::with_options(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        test_worker_core(Arc::clone(&store)),
        BlockReportOptions {
            full_max_blocks_per_batch: 1,
            delta_max_entries_per_batch: 1,
        },
    )
    .expect("block reporter");

    for (_, expected_message) in invalid_full_responses {
        let error = reporter
            .send_full_once()
            .await
            .expect_err("invalid Full acknowledgement must fail closed");
        assert!(
            matches!(&error, BlockReportError::Fatal(_)),
            "invalid Full acknowledgement must be fatal: {error}"
        );
        assert!(
            error.to_string().contains(expected_message),
            "unexpected Full acknowledgement error: {error}"
        );
    }

    assert_eq!(reporter.send_full_once().await.unwrap().accepted_peers, 1);
    publish_ready_block_for(
        store.as_ref(),
        group_name(),
        BlockId::new(InodeId::new(7), BlockIndex::new(2)),
        payload(),
        103,
    );
    let error = reporter
        .send_delta_once()
        .await
        .expect_err("Delta acknowledgement without a published baseline must fail closed");
    assert!(matches!(&error, BlockReportError::Fatal(_)));
    assert!(error.to_string().contains("published Delta baseline"));

    shutdown.send(()).ok();
}

#[tokio::test]
async fn startup_deleting_recovery_precedes_first_full_block_report() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));

    let temp = TempDir::new().expect("tempdir");
    let data_root = temp.path().join("hdd0");
    std::fs::create_dir_all(&data_root).unwrap();
    let raw_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(data_root));
    publish_ready_block_for(&raw_store, group_name(), block_id(), payload(), 101);
    let paths = raw_store.paths(&group_name(), block_id());
    // Recreate the durable Deleting checkpoint before either unlink is completed.
    let mut bytes = std::fs::read(&paths.meta_path).unwrap();
    let mut meta = <beryl_proto::worker::BlockMetaPayloadProto as prost::Message>::decode(&bytes[20..]).unwrap();
    meta.visibility.as_mut().unwrap().block_state = beryl_proto::worker::BlockStateProto::BlockStateDeleting as i32;
    let payload = prost::Message::encode_to_vec(&meta);
    bytes.truncate(20);
    bytes[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&payload);
    std::fs::write(&paths.meta_path, bytes).unwrap();
    drop(raw_store);

    let store = report_store(&temp);
    assert_eq!(store.report().expect("store report").dirs[0].block_count, 0);
    assert!(!paths.data_path.exists());
    assert!(!paths.meta_path.exists());

    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        test_worker_core(Arc::clone(&store)),
    )
    .expect("block reporter");
    let round = reporter.send_full_once().await.expect("first full report");

    assert_eq!(round.accepted_peers, 1);
    let requests = mock.block_report_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let Batch::FullReport(full) = requests[0].batch.as_ref().expect("full report") else {
        panic!("expected full block report");
    };
    assert!(full.blocks.is_empty(), "recovered block must not reappear as Ready");
    shutdown.send(()).ok();
}

#[tokio::test]
async fn block_report_waits_for_registration_and_heartbeat_readiness() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        test_worker_core(Arc::clone(&store)),
    )
    .expect("block reporter");

    let without_registration = reporter.send_full_once().await.expect("skip unregistered");
    assert_eq!(without_registration.attempted_peers, 0);
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    let without_heartbeat = reporter.send_full_once().await.expect("skip not ready");

    assert_eq!(without_heartbeat.attempted_peers, 0);
    assert!(mock.block_report_requests.lock().unwrap().is_empty());
    shutdown.send(()).ok();
}
