// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_common::error::rpc::{ErrorKind, RecoveryAction, WorkerErrorKind};
use beryl_common::header::{RequestHeader, HEADER_WORKER_DATA_ERROR_DETAIL, WORKER_DATA_ERROR_DETAIL_V1};
use beryl_e2e::{data::deterministic_bytes, TestCluster, TestResult};
use beryl_proto::common::{ByteRangeProto, ErrorDetailProto, RequestHeaderProto};
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::get_block_locations_request_proto;
use beryl_proto::metadata::{FileBlockLocationProto, GetBlockLocationsRequestProto};
use beryl_proto::worker::worker_data_service_client::WorkerDataServiceClient;
use beryl_proto::worker::{DataRequestHeaderProto, DataResponseHeaderProto, ReadBlockRequestProto};
use beryl_types::{ClientId, WorkerRunId};
use bytes::Bytes;
use prost::Message;
use tonic::Request;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_locations_before_full_report_convergence_are_unavailable_then_recover() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    let path = "/worker-restart/pre-convergence";
    let payload = write_closed_file(&mut cluster, path, 1_537)
        .await
        .expect("write committed file");

    cluster
        .restart_worker_until_heartbeat()
        .await
        .expect("restart worker without full report");
    assert_block_location_unavailable(&cluster, path, payload.len() as u32)
        .await
        .expect("pre-convergence location error");

    cluster.converge_block_reports().await.expect("full report convergence");
    let after = client
        .open(path)
        .await
        .expect("open after convergence")
        .read_to_end()
        .await;
    assert_eq!(after.expect("read after convergence"), payload);
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_old_worker_run_is_rejected_after_restart() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let path = "/worker-restart/stale-run";
    let payload = write_closed_file(&mut cluster, path, 1_537)
        .await
        .expect("write committed file");
    let before_locations = metadata_locations(&cluster, path, payload.len() as u32)
        .await
        .expect("pre-restart metadata locations");
    let old_run = first_location_run_id(&before_locations);
    let old_location = before_locations.first().expect("pre-restart location").clone();
    let old_worker = old_location.workers.first().expect("pre-restart worker").clone();

    cluster.restart_worker().await.expect("restart worker");
    let new_run = cluster.current_worker_run_id().expect("new worker run id");
    assert!(
        !old_run.matches(new_run),
        "worker restart must create a new WorkerRunId"
    );
    let after_locations = metadata_locations(&cluster, path, payload.len() as u32)
        .await
        .expect("post-restart metadata locations");
    assert_locations_use_only_run(&after_locations, new_run);
    assert_locations_do_not_use_run(&after_locations, old_run);
    assert_stale_worker_run_rejected(&old_worker.endpoint, old_run, &old_location)
        .await
        .expect("old worker run rejected");

    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_block_file_is_readable_after_worker_restart_full_report_convergence() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    let path = "/worker-restart/multi-block";
    let payload = write_closed_file(&mut cluster, path, 5_123)
        .await
        .expect("write multi-block file");
    let before_locations = metadata_locations(&cluster, path, payload.len() as u32)
        .await
        .expect("pre-restart metadata locations");
    assert!(
        before_locations.len() >= 2,
        "test payload must produce multiple block locations"
    );
    let old_run = first_location_run_id(&before_locations);
    assert_eq!(cluster.current_worker_run_id(), Some(old_run));

    cluster.restart_worker().await.expect("restart worker");

    let new_run = cluster.current_worker_run_id().expect("new worker run id");
    assert!(
        !old_run.matches(new_run),
        "worker restart must create a new WorkerRunId"
    );
    let after_locations = metadata_locations(&cluster, path, payload.len() as u32)
        .await
        .expect("post-restart metadata locations");
    assert_eq!(after_locations.len(), before_locations.len());
    assert_locations_use_only_run(&after_locations, new_run);
    assert_locations_do_not_use_run(&after_locations, old_run);
    let after = client.open(path).await.expect("open after restart").read_to_end().await;
    assert_eq!(after.expect("read multi-block after restart"), payload);
    cluster.shutdown().await.expect("shutdown cluster");
}

async fn write_closed_file(cluster: &mut TestCluster, path: &str, payload_len: usize) -> TestResult<Bytes> {
    cluster
        .client()
        .mkdirs("/worker-restart")
        .await
        .expect("create worker restart dir");
    let payload = Bytes::from(deterministic_bytes(payload_len));
    let mut writer = cluster.client().create(path).await?;
    writer.write_all(payload.clone()).await?;
    writer.close().await?;
    cluster.converge_block_reports().await?;
    Ok(payload)
}

async fn metadata_locations(cluster: &TestCluster, path: &str, len: u32) -> TestResult<Vec<FileBlockLocationProto>> {
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let response = metadata
        .get_block_locations(Request::new(GetBlockLocationsRequestProto {
            header: Some(metadata_header(701)),
            target: Some(get_block_locations_request_proto::Target::Path(path.to_string())),
            range: Some(ByteRangeProto { offset: 0, len }),
        }))
        .await?
        .into_inner();
    assert!(
        response.header.expect("metadata response header").error.is_none(),
        "metadata locations response must not carry business error"
    );
    assert!(!response.locations.is_empty(), "{path} must have metadata locations");
    Ok(response.locations)
}

async fn assert_block_location_unavailable(cluster: &TestCluster, path: &str, len: u32) -> TestResult<()> {
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let response = metadata
        .get_block_locations(Request::new(GetBlockLocationsRequestProto {
            header: Some(metadata_header(702)),
            target: Some(get_block_locations_request_proto::Target::Path(path.to_string())),
            range: Some(ByteRangeProto { offset: 0, len }),
        }))
        .await?
        .into_inner();
    let error = response
        .header
        .expect("metadata response header")
        .error
        .expect("pre-convergence metadata error");
    assert_refresh_metadata(&error, ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable));
    assert!(response.locations.is_empty());
    Ok(())
}

async fn assert_stale_worker_run_rejected(
    endpoint: &str,
    stale_run_id: WorkerRunId,
    location: &FileBlockLocationProto,
) -> TestResult<()> {
    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let mut worker = WorkerDataServiceClient::connect(endpoint).await?;
    let status = match worker
        .read_block(Request::new(ReadBlockRequestProto {
            header: Some(data_header(801)),
            group_name: "root".to_string(),
            block_id: location.block_id,
            byte_range: Some(ByteRangeProto {
                offset: 0,
                len: location.len as u32,
            }),

            frame_size: 1024,
            worker_run_id: stale_run_id.to_string(),
            block_format_id: location.block_format_id,
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
        }))
        .await
    {
        Ok(_) => panic!("stale Worker run unexpectedly read a block"),
        Err(status) => status,
    };
    assert_eq!(
        status
            .metadata()
            .get(HEADER_WORKER_DATA_ERROR_DETAIL)
            .and_then(|value| value.to_str().ok()),
        Some(WORKER_DATA_ERROR_DETAIL_V1)
    );
    let error = DataResponseHeaderProto::decode(status.details())?
        .error
        .expect("stale worker run error");
    assert_refresh_metadata(&error, ErrorKind::Worker(WorkerErrorKind::RunMismatch));
    Ok(())
}

fn assert_refresh_metadata(error: &ErrorDetailProto, expected_kind: ErrorKind) {
    let rpc_error = rpc_error_from_proto(error);
    assert_eq!(rpc_error.kind, expected_kind);
    assert!(matches!(rpc_error.recovery, RecoveryAction::RefreshMetadata { .. }));
}

fn first_location_run_id(locations: &[FileBlockLocationProto]) -> WorkerRunId {
    let workers = locations.first().expect("at least one location").workers.as_slice();
    let worker = workers.first().expect("location has worker");
    WorkerRunId::parse(&worker.worker_run_id).expect("valid worker run id")
}

fn assert_locations_use_only_run(locations: &[FileBlockLocationProto], expected: WorkerRunId) {
    for location in locations {
        assert!(!location.workers.is_empty(), "location must have a worker");
        for worker in &location.workers {
            let actual = WorkerRunId::parse(&worker.worker_run_id).expect("valid worker run id");
            assert!(
                actual.matches(expected),
                "metadata location used worker_run_id {actual}, expected {expected}"
            );
        }
    }
}

fn assert_locations_do_not_use_run(locations: &[FileBlockLocationProto], stale: WorkerRunId) {
    for location in locations {
        for worker in &location.workers {
            let actual = WorkerRunId::parse(&worker.worker_run_id).expect("valid worker run id");
            assert!(
                !actual.matches(stale),
                "metadata location reused stale worker_run_id {stale}"
            );
        }
    }
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
