// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::sync::Arc;
use std::time::Duration;
use std::{path::Path, process::Stdio};

use beryl_metadata::runtime::{MetadataAuthority, Readiness};
use beryl_metadata::service::MetadataFileSystemServiceImpl;
use beryl_metadata::worker::MetadataWorkerServiceImpl;
use beryl_proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use beryl_proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use beryl_proto::worker::worker_data_service_server::WorkerDataServiceServer;
use beryl_worker::control::RegistrationSet;
use beryl_worker::net::server::grpc::WorkerDataServiceImpl;
use beryl_worker::WorkerCore;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::TestResult;

/// Process stop budget used by lifecycle acceptance tests.
///
/// This intentionally exceeds the configured 200 ms RPC/background drain so
/// Metadata still has time to explicitly close and await its Raft authority.
const PROCESS_STOP_BUDGET: Duration = Duration::from_secs(5);

pub struct MetadataServiceInstance {
    handle: ServerHandle,
    readiness: Option<Readiness>,
    authority: Option<MetadataAuthority>,
}

impl MetadataServiceInstance {
    pub fn start(
        listener: TcpListener,
        filesystem: MetadataFileSystemServiceImpl,
        worker: MetadataWorkerServiceImpl,
        readiness: Readiness,
        authority: MetadataAuthority,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(filesystem))
                .add_service(MetadataWorkerServiceProtoServer::new(worker))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await?;
            Ok(())
        });
        Self {
            handle: ServerHandle::new(shutdown_tx, task),
            readiness: Some(readiness),
            authority: Some(authority),
        }
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        drop(self.readiness.take());
        self.handle.shutdown().await?;
        if let Some(authority) = self.authority.take() {
            authority.shutdown().await?;
        }
        Ok(())
    }

    pub fn abort(&mut self) {
        self.handle.abort();
        self.readiness.take();
        self.authority.take();
    }
}

pub struct MetadataProcessInstance {
    child: Child,
}

impl MetadataProcessInstance {
    pub fn start(executable: &Path, config_path: &Path) -> TestResult<Self> {
        let mut command = Command::new(executable);
        command
            .arg(config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        Ok(Self {
            child: command.spawn()?,
        })
    }

    pub async fn kill(mut self) -> TestResult<()> {
        self.child.kill().await?;
        let status = self.child.wait().await?;
        if status.success() {
            return Err("metadata process exited successfully instead of being killed".into());
        }
        Ok(())
    }

    /// Delivers one Unix termination signal and requires a bounded clean exit.
    #[cfg(unix)]
    pub async fn signal_and_wait(mut self, signal: i32) -> TestResult<()> {
        let pid = self.child.id().ok_or("metadata process has no operating-system pid")?;
        let result = unsafe { libc::kill(pid as i32, signal) };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let status = match timeout(PROCESS_STOP_BUDGET, self.child.wait()).await {
            Ok(status) => status?,
            Err(_) => {
                let _ = self.child.start_kill();
                let _ = self.child.wait().await;
                return Err("metadata process graceful shutdown timed out".into());
            }
        };
        if !status.success() {
            return Err(format!("metadata process exited unsuccessfully after signal: {status}").into());
        }
        Ok(())
    }

    pub fn abort(&mut self) {
        let _ = self.child.start_kill();
    }
}

pub struct WorkerServiceInstance {
    handle: ServerHandle,
}

impl WorkerServiceInstance {
    pub fn start(
        listener: TcpListener,
        core: Arc<WorkerCore>,
        registration_state: Arc<RegistrationSet>,
        metadata: beryl_worker::config::WorkerRegistrationConfig,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let service = WorkerDataServiceImpl::new(core, registration_state, 64, 32, &metadata)
                .expect("data service configuration");
            Server::builder()
                .add_service(WorkerDataServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await?;
            Ok(())
        });
        Self {
            handle: ServerHandle::new(shutdown_tx, task),
        }
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        self.handle.shutdown().await
    }

    pub fn abort(&mut self) {
        self.handle.abort();
    }
}

struct ServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<TestResult<()>>,
}

impl ServerHandle {
    fn new(shutdown: oneshot::Sender<()>, task: JoinHandle<TestResult<()>>) -> Self {
        Self {
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn shutdown(&mut self) -> TestResult<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match timeout(PROCESS_STOP_BUDGET, &mut self.task).await {
            Ok(result) => result?,
            Err(_) => {
                self.task.abort();
                Err("server shutdown timed out".into())
            }
        }
    }

    fn abort(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}
