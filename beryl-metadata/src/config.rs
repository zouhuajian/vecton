// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata process configuration.

use crate::raft::{
    MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES, MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
    MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
};
use crate::readiness::RootReadinessConfig;
use beryl_common::config::{format_host_port, load_from_yaml_file, validate_public_host, FlatConfig};
use beryl_common::error::{CommonError, CommonErrorKind};
use beryl_common::grpc_server::MAX_GRPC_CONCURRENT_REQUESTS;
use beryl_common::observe::config::{LogConfig, ResourceConfig};
use beryl_common::observe::ObservabilityConfig;
use beryl_types::{FileLayout, GroupName, MAX_FILE_BLOCKS};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

const CLUSTER_ID: &str = "beryl.cluster.id";
const HOST: &str = "beryl.metadata.host";
const BIND_HOST: &str = "beryl.metadata.bind-host";
const RPC_PORT: &str = "beryl.metadata.rpc.port";
const RPC_MAX_CONCURRENT_REQUESTS: &str = "beryl.metadata.rpc.max-concurrent-requests";
const RPC_MAX_CONCURRENT_REQUESTS_PER_CONNECTION: &str = "beryl.metadata.rpc.max-concurrent-requests-per-connection";
const RPC_RESERVED_CONTROL_REQUESTS: &str = "beryl.metadata.rpc.reserved-control-requests";
const WRITE_SESSION_MAX_ACTIVE: &str = "beryl.metadata.write-session.max-active";
const WRITE_SESSION_MAX_ACTIVE_PER_CLIENT: &str = "beryl.metadata.write-session.max-active-per-client";
const WRITE_TARGET_MAX_OUTSTANDING: &str = "beryl.metadata.write-target.max-outstanding";
const WRITE_TARGET_MAX_OUTSTANDING_PER_SESSION: &str = "beryl.metadata.write-target.max-outstanding-per-session";
const FILE_BLOCK_SIZE_DEFAULT: &str = "beryl.file.block-size.default";
const HTTP_PORT: &str = "beryl.metadata.http.port";
const STORAGE_DIR: &str = "beryl.metadata.storage.dir";
const LIST_DEFAULT_PAGE_SIZE: &str = "beryl.metadata.namespace.list.default-page-size";
const LIST_MAX_PAGE_SIZE: &str = "beryl.metadata.namespace.list.max-page-size";
const BLOCK_CLEANUP_ENABLED: &str = "beryl.metadata.block.cleanup.enabled";
const BLOCK_CLEANUP_INTERVAL: &str = "beryl.metadata.block.cleanup.interval";
const BLOCK_CLEANUP_GRACE_PERIOD: &str = "beryl.metadata.block.cleanup.grace-period";
const BLOCK_CLEANUP_SCAN_LIMIT: &str = "beryl.metadata.block.cleanup.scan-limit";
const BLOCK_CLEANUP_QUEUE_CAPACITY: &str = "beryl.metadata.block.cleanup.queue-capacity";
const BLOCK_CLEANUP_BATCH_SIZE: &str = "beryl.metadata.block.cleanup.batch-size";
const BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF: &str = "beryl.metadata.block.cleanup.retry.initial-backoff";
const BLOCK_CLEANUP_RETRY_MAX_BACKOFF: &str = "beryl.metadata.block.cleanup.retry.max-backoff";
const NAMESPACE_DELETE_INTERVAL: &str = "beryl.metadata.namespace.delete.interval";
const NAMESPACE_DELETE_MAX_ROOTS: &str = "beryl.metadata.namespace.delete.batch.max-roots";
const NAMESPACE_DELETE_MAX_ENTRIES: &str = "beryl.metadata.namespace.delete.batch.max-entries";
const NAMESPACE_DELETE_MAX_SIZE: &str = "beryl.metadata.namespace.delete.batch.max-size";
const NAMESPACE_DELETE_RETRY_INITIAL_BACKOFF: &str = "beryl.metadata.namespace.delete.retry.initial-backoff";
const NAMESPACE_DELETE_RETRY_MAX_BACKOFF: &str = "beryl.metadata.namespace.delete.retry.max-backoff";
const STARTUP_INITIAL_BACKOFF: &str = "beryl.metadata.startup.retry.initial-backoff";
const STARTUP_MAX_BACKOFF: &str = "beryl.metadata.startup.retry.max-backoff";
const STARTUP_WARN_AFTER: &str = "beryl.metadata.startup.warn-after";
const STARTUP_TIMEOUT: &str = "beryl.metadata.startup.timeout";
const STARTUP_FAIL_FAST: &str = "beryl.metadata.startup.fail-fast";
const WRITE_LEASE_TIMEOUT: &str = "beryl.metadata.write-lease.timeout";
const SHUTDOWN_TIMEOUT: &str = "beryl.metadata.shutdown.timeout";
const WORKER_TIMEOUT: &str = "beryl.metadata.worker.liveness.timeout";
const WORKER_SCAN_INTERVAL: &str = "beryl.metadata.worker.liveness.scan-interval";

const DEFAULT_LIST_STATUS_PAGE_SIZE: u32 = 1_000;
pub(crate) const MAX_LIST_STATUS_PAGE_SIZE: u32 = 10_000;

/// Configuration consumed by one Metadata process.
#[derive(Clone, Debug)]
pub struct MetadataConfig {
    /// Cluster identity persisted in local storage markers.
    pub cluster_id: String,
    /// Host published to clients and workers.
    pub host: String,
    /// Local interface shared by Metadata listeners.
    pub bind_host: IpAddr,
    /// RPC port published with `host` and bound with `bind_host`.
    pub rpc_port: u16,
    /// Immediate inbound RPC concurrency policy.
    pub rpc_concurrency: MetadataRpcConcurrencyConfig,
    /// Leader-local write-session capacity limits.
    pub write_session_limits: MetadataWriteSessionLimitsConfig,
    /// Leader-local pending plus issued write-target capacity limits.
    pub write_target_limits: MetadataWriteTargetLimitsConfig,
    /// Server-owned defaults materialized into every newly created file layout.
    pub file_layout_defaults: FileLayoutDefaults,
    /// Process-owned HTTP port for metrics, health, and future APIs.
    pub http_port: u16,
    /// Local directory for authoritative Metadata state.
    pub storage_dir: PathBuf,
    /// Internal single-node Raft configuration.
    pub raft: RaftConfig,
    /// Internal authority identity for the supported root group.
    pub authority: MetadataAuthorityConfig,
    /// Bounded public directory-listing policy.
    pub namespace_list: NamespaceListConfig,
    /// Physical Worker block cleanup policy.
    pub block_cleanup: BlockCleanupConfig,
    /// Post-detach namespace deletion policy.
    pub namespace_delete: NamespaceDeleteConfig,
    /// Worker liveness and soft-state cleanup policy.
    pub worker_liveness: WorkerLivenessConfig,
    /// Metadata startup readiness policy.
    pub startup: StartupConfig,
    /// Leader-local write-session expiry policy.
    pub write_lease_timeout_ms: u64,
    /// Graceful RPC/background drain interval before remaining work is cancelled.
    ///
    /// Raft shutdown is always awaited afterward, so the deployment stop
    /// budget must also include authority-close time.
    pub shutdown_timeout_ms: u64,
    /// Shared logging and metrics recorder configuration.
    pub observability: ObservabilityConfig,
}

/// Metadata RPC concurrency bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataRpcConcurrencyConfig {
    /// Maximum active Metadata RPCs across all client and Worker connections.
    pub max_concurrent_requests: usize,
    /// Maximum active Metadata RPCs from one HTTP/2 connection.
    pub max_concurrent_requests_per_connection: usize,
    /// Capacity protected from filesystem RPCs for Worker and health traffic.
    pub reserved_control_requests: usize,
}

/// Bounds opening and active write sessions owned by one Metadata leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataWriteSessionLimitsConfig {
    /// Maximum opening plus active sessions across all clients.
    pub max_active: usize,
    /// Maximum opening plus active sessions attributed to one client ID.
    pub max_active_per_client: usize,
}

/// Bounds pending and issued write targets retained by one Metadata leader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataWriteTargetLimitsConfig {
    /// Maximum targets retained across every active write session.
    pub max_outstanding: usize,
    /// Maximum targets retained by one active write session.
    pub max_outstanding_per_session: usize,
}

/// Defaults materialized into the immutable layout of newly created files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileLayoutDefaults {
    /// Logical capacity of each newly allocated block.
    pub block_size: u32,
}

impl FileLayoutDefaults {
    /// Validate and construct the server-owned defaults for new files.
    pub fn try_new(block_size: u32) -> Result<Self, CommonError> {
        let config = Self { block_size };
        config.layout()?;
        Ok(config)
    }

    /// Materialize the fully validated layout persisted by Metadata.
    pub fn layout(self) -> Result<FileLayout, CommonError> {
        let layout = FileLayout::new(self.block_size);
        layout.validate().map_err(|error| {
            CommonError::new(
                CommonErrorKind::InvalidArgument,
                format!("invalid default file layout: {error}"),
            )
        })?;
        Ok(layout)
    }
}

impl MetadataConfig {
    /// Bind socket for the Metadata RPC service.
    pub fn rpc_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.rpc_port)
    }

    /// Address published to processes that connect to Metadata RPC.
    pub fn rpc_address(&self) -> String {
        format_host_port(&self.host, self.rpc_port)
    }

    /// Bind socket for the process-owned HTTP service.
    pub fn http_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }
}

/// Startup/readiness configuration.
#[derive(Clone, Debug)]
pub struct StartupConfig {
    pub root_readiness: RootReadinessConfig,
}

/// Server-owned page-size policy for one public `ListStatus` response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NamespaceListConfig {
    default_page_size: u32,
    max_page_size: u32,
}

/// Bounds detection and delivery of exact block cleanup commands.
#[derive(Clone, Debug)]
pub struct BlockCleanupConfig {
    pub scan_interval_ms: u64,
    pub reclaim_grace_ms: u64,
    pub max_replicas_per_scan: usize,
    pub max_candidates: usize,
    pub enabled: bool,
    pub max_commands_per_heartbeat: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

/// Bounds leader proposals that finish recursively deleted namespace trees.
#[derive(Clone, Debug)]
pub struct NamespaceDeleteConfig {
    pub scan_interval_ms: u64,
    pub max_candidates: u32,
    pub max_entries: u32,
    pub max_batch_bytes: u32,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
}

/// Worker liveness policy owned by Metadata.
#[derive(Clone, Debug)]
pub struct WorkerLivenessConfig {
    /// Timeout returned by the heartbeat protocol and used by all liveness checks.
    pub heartbeat_timeout_ms: u32,
    pub scan_interval_ms: u64,
}

/// Internal Raft configuration for the current single-node product boundary.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    pub node_id: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self { node_id: 1 }
    }
}

/// Internal identity for the one supported metadata authority group.
#[derive(Clone, Debug)]
pub struct MetadataAuthorityConfig {
    pub group_name: GroupName,
}

impl Default for MetadataAuthorityConfig {
    fn default() -> Self {
        Self {
            group_name: GroupName::parse("root").expect("the supported metadata group is valid"),
        }
    }
}

impl Default for WorkerLivenessConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_ms: 60_000,
            scan_interval_ms: 30_000,
        }
    }
}

impl Default for BlockCleanupConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 30_000,
            reclaim_grace_ms: 300_000,
            max_replicas_per_scan: 10_000,
            max_candidates: 10_000,
            enabled: true,
            max_commands_per_heartbeat: 32,
            retry_initial_backoff_ms: 1_000,
            retry_max_backoff_ms: 60_000,
        }
    }
}

impl Default for NamespaceListConfig {
    fn default() -> Self {
        Self {
            default_page_size: DEFAULT_LIST_STATUS_PAGE_SIZE,
            max_page_size: MAX_LIST_STATUS_PAGE_SIZE,
        }
    }
}

impl Default for MetadataRpcConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 64,
            max_concurrent_requests_per_connection: 16,
            reserved_control_requests: 8,
        }
    }
}

impl Default for MetadataWriteSessionLimitsConfig {
    fn default() -> Self {
        Self {
            max_active: 1_024,
            max_active_per_client: 64,
        }
    }
}

impl Default for MetadataWriteTargetLimitsConfig {
    fn default() -> Self {
        Self {
            max_outstanding: 65_536,
            max_outstanding_per_session: MAX_FILE_BLOCKS,
        }
    }
}

impl Default for FileLayoutDefaults {
    fn default() -> Self {
        Self {
            block_size: 64 * 1024 * 1024,
        }
    }
}

impl NamespaceListConfig {
    pub fn try_new(default_page_size: u32, max_page_size: u32) -> Result<Self, CommonError> {
        if default_page_size == 0 {
            return Err(invalid_config(LIST_DEFAULT_PAGE_SIZE, "must be greater than zero"));
        }
        if max_page_size == 0 {
            return Err(invalid_config(LIST_MAX_PAGE_SIZE, "must be greater than zero"));
        }
        if default_page_size > max_page_size {
            return Err(invalid_config(
                LIST_DEFAULT_PAGE_SIZE,
                "must be less than or equal to the configured maximum",
            ));
        }
        if max_page_size > MAX_LIST_STATUS_PAGE_SIZE {
            return Err(invalid_config(
                LIST_MAX_PAGE_SIZE,
                "exceeds the compiled page-size ceiling",
            ));
        }
        Ok(Self {
            default_page_size,
            max_page_size,
        })
    }

    pub fn default_page_size(self) -> u32 {
        self.default_page_size
    }

    pub fn max_page_size(self) -> u32 {
        self.max_page_size
    }
}

impl Default for NamespaceDeleteConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 1_000,
            max_candidates: MAX_RECLAIM_DETACHED_ROOT_CANDIDATES,
            max_entries: MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
            max_batch_bytes: MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
            retry_initial_backoff_ms: 1_000,
            retry_max_backoff_ms: 60_000,
        }
    }
}

impl Default for MetadataConfig {
    fn default() -> Self {
        let bind_host = "0.0.0.0".parse().expect("default bind host is valid");
        Self {
            cluster_id: "local-beryl".to_string(),
            host: "127.0.0.1".to_string(),
            bind_host,
            rpc_port: 18080,
            rpc_concurrency: MetadataRpcConcurrencyConfig::default(),
            write_session_limits: MetadataWriteSessionLimitsConfig::default(),
            write_target_limits: MetadataWriteTargetLimitsConfig::default(),
            file_layout_defaults: FileLayoutDefaults::default(),
            http_port: 18081,
            storage_dir: PathBuf::from("data/metadata"),
            raft: RaftConfig::default(),
            authority: MetadataAuthorityConfig::default(),
            namespace_list: NamespaceListConfig::default(),
            block_cleanup: BlockCleanupConfig::default(),
            namespace_delete: NamespaceDeleteConfig::default(),
            worker_liveness: WorkerLivenessConfig::default(),
            startup: StartupConfig {
                root_readiness: RootReadinessConfig::default(),
            },
            write_lease_timeout_ms: 60_000,
            shutdown_timeout_ms: 30_000,
            observability: ObservabilityConfig {
                log: LogConfig {
                    format: "compact".to_string(),
                    output: "stderr".to_string(),
                    level: "info".to_string(),
                },
                resource: ResourceConfig::default(),
            },
        }
    }
}

impl MetadataConfig {
    /// Load metadata configuration from a YAML file.
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, CommonError> {
        Self::from_flat(load_from_yaml_file(config_path)?)
    }

    /// Build the typed Metadata configuration from shared YAML mechanics.
    pub fn from_flat(flat: FlatConfig) -> Result<Self, CommonError> {
        let flat = &flat;
        let defaults = Self::default();

        let cluster_id = flat.string_or(CLUSTER_ID, &defaults.cluster_id)?;
        if cluster_id.trim().is_empty() {
            return Err(invalid_config(CLUSTER_ID, "must not be empty"));
        }
        let host = flat.string_or(HOST, &defaults.host)?;
        validate_public_host(HOST, &host)?;
        let bind_host = flat
            .string_or(BIND_HOST, &defaults.bind_host.to_string())?
            .parse::<IpAddr>()
            .map_err(|_| invalid_config(BIND_HOST, "must be an IP address"))?;
        let rpc_port = flat.port_or(RPC_PORT, defaults.rpc_port)?;
        let http_port = flat.port_or(HTTP_PORT, defaults.http_port)?;
        if rpc_port == http_port {
            return Err(invalid_config(HTTP_PORT, "must differ from the RPC port"));
        }
        let rpc_concurrency_defaults = MetadataRpcConcurrencyConfig::default();
        let rpc_concurrency = MetadataRpcConcurrencyConfig {
            max_concurrent_requests: flat.positive_usize_or(
                RPC_MAX_CONCURRENT_REQUESTS,
                rpc_concurrency_defaults.max_concurrent_requests,
            )?,
            max_concurrent_requests_per_connection: flat.positive_usize_or(
                RPC_MAX_CONCURRENT_REQUESTS_PER_CONNECTION,
                rpc_concurrency_defaults.max_concurrent_requests_per_connection,
            )?,
            reserved_control_requests: flat.positive_usize_or(
                RPC_RESERVED_CONTROL_REQUESTS,
                rpc_concurrency_defaults.reserved_control_requests,
            )?,
        };
        validate_rpc_concurrency(&rpc_concurrency)?;
        let write_session_defaults = MetadataWriteSessionLimitsConfig::default();
        let write_session_limits = MetadataWriteSessionLimitsConfig {
            max_active: flat.positive_usize_or(WRITE_SESSION_MAX_ACTIVE, write_session_defaults.max_active)?,
            max_active_per_client: flat.positive_usize_or(
                WRITE_SESSION_MAX_ACTIVE_PER_CLIENT,
                write_session_defaults.max_active_per_client,
            )?,
        };
        validate_write_session_limits(&write_session_limits)?;
        let write_target_defaults = MetadataWriteTargetLimitsConfig::default();
        let write_target_limits = MetadataWriteTargetLimitsConfig {
            max_outstanding: flat
                .positive_usize_or(WRITE_TARGET_MAX_OUTSTANDING, write_target_defaults.max_outstanding)?,
            max_outstanding_per_session: flat.positive_usize_or(
                WRITE_TARGET_MAX_OUTSTANDING_PER_SESSION,
                write_target_defaults.max_outstanding_per_session,
            )?,
        };
        validate_write_target_limits(&write_target_limits)?;
        let file_layout_defaults = FileLayoutDefaults::default();
        let file_layout_defaults =
            FileLayoutDefaults::try_new(flat.bytes_u32_or(FILE_BLOCK_SIZE_DEFAULT, file_layout_defaults.block_size)?)?;
        let storage_dir = PathBuf::from(flat.string_or(STORAGE_DIR, defaults.storage_dir.to_str().unwrap())?);
        let observability = ObservabilityConfig::from_flat(flat)?;

        let namespace_list = NamespaceListConfig::try_new(
            flat.positive_u32_or(LIST_DEFAULT_PAGE_SIZE, DEFAULT_LIST_STATUS_PAGE_SIZE)?,
            flat.positive_u32_or(LIST_MAX_PAGE_SIZE, MAX_LIST_STATUS_PAGE_SIZE)?,
        )?;

        let cleanup_defaults = BlockCleanupConfig::default();
        let block_cleanup = BlockCleanupConfig {
            scan_interval_ms: flat.duration_ms_or(BLOCK_CLEANUP_INTERVAL, cleanup_defaults.scan_interval_ms)?,
            reclaim_grace_ms: flat.duration_ms_or(BLOCK_CLEANUP_GRACE_PERIOD, cleanup_defaults.reclaim_grace_ms)?,
            max_replicas_per_scan: flat
                .positive_usize_or(BLOCK_CLEANUP_SCAN_LIMIT, cleanup_defaults.max_replicas_per_scan)?,
            max_candidates: flat.positive_usize_or(BLOCK_CLEANUP_QUEUE_CAPACITY, cleanup_defaults.max_candidates)?,
            enabled: flat.bool_or(BLOCK_CLEANUP_ENABLED, cleanup_defaults.enabled)?,
            max_commands_per_heartbeat: flat
                .positive_usize_or(BLOCK_CLEANUP_BATCH_SIZE, cleanup_defaults.max_commands_per_heartbeat)?,
            retry_initial_backoff_ms: flat.duration_ms_or(
                BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF,
                cleanup_defaults.retry_initial_backoff_ms,
            )?,
            retry_max_backoff_ms: flat
                .duration_ms_or(BLOCK_CLEANUP_RETRY_MAX_BACKOFF, cleanup_defaults.retry_max_backoff_ms)?,
        };
        ensure_backoff_order(
            BLOCK_CLEANUP_RETRY_INITIAL_BACKOFF,
            block_cleanup.retry_initial_backoff_ms,
            BLOCK_CLEANUP_RETRY_MAX_BACKOFF,
            block_cleanup.retry_max_backoff_ms,
        )?;
        let delete_defaults = NamespaceDeleteConfig::default();
        let namespace_delete = NamespaceDeleteConfig {
            scan_interval_ms: flat.duration_ms_or(NAMESPACE_DELETE_INTERVAL, delete_defaults.scan_interval_ms)?,
            max_candidates: flat.positive_u32_or(NAMESPACE_DELETE_MAX_ROOTS, delete_defaults.max_candidates)?,
            max_entries: flat.positive_u32_or(NAMESPACE_DELETE_MAX_ENTRIES, delete_defaults.max_entries)?,
            max_batch_bytes: flat.bytes_u32_or(NAMESPACE_DELETE_MAX_SIZE, delete_defaults.max_batch_bytes)?,
            retry_initial_backoff_ms: flat.duration_ms_or(
                NAMESPACE_DELETE_RETRY_INITIAL_BACKOFF,
                delete_defaults.retry_initial_backoff_ms,
            )?,
            retry_max_backoff_ms: flat
                .duration_ms_or(NAMESPACE_DELETE_RETRY_MAX_BACKOFF, delete_defaults.retry_max_backoff_ms)?,
        };
        validate_namespace_delete(&namespace_delete)?;

        let worker_defaults = WorkerLivenessConfig::default();
        let heartbeat_timeout_ms =
            flat.duration_ms_or(WORKER_TIMEOUT, u64::from(worker_defaults.heartbeat_timeout_ms))?;
        let worker_liveness = WorkerLivenessConfig {
            heartbeat_timeout_ms: u32::try_from(heartbeat_timeout_ms)
                .map_err(|_| invalid_config(WORKER_TIMEOUT, "exceeds the heartbeat protocol maximum"))?,
            scan_interval_ms: flat.duration_ms_or(WORKER_SCAN_INTERVAL, worker_defaults.scan_interval_ms)?,
        };
        let readiness_defaults = RootReadinessConfig::default();
        let startup = StartupConfig {
            root_readiness: RootReadinessConfig {
                initial_backoff_ms: flat
                    .duration_ms_or(STARTUP_INITIAL_BACKOFF, readiness_defaults.initial_backoff_ms)?,
                max_backoff_ms: flat.duration_ms_or(STARTUP_MAX_BACKOFF, readiness_defaults.max_backoff_ms)?,
                warn_after_ms: flat.duration_ms_or(STARTUP_WARN_AFTER, readiness_defaults.warn_after_ms)?,
                timeout_ms: flat.duration_ms_or(STARTUP_TIMEOUT, readiness_defaults.timeout_ms)?,
                fail_fast: flat.bool_or(STARTUP_FAIL_FAST, readiness_defaults.fail_fast)?,
            },
        };
        ensure_backoff_order(
            STARTUP_INITIAL_BACKOFF,
            startup.root_readiness.initial_backoff_ms,
            STARTUP_MAX_BACKOFF,
            startup.root_readiness.max_backoff_ms,
        )?;
        if startup.root_readiness.warn_after_ms > startup.root_readiness.timeout_ms {
            return Err(invalid_config(
                STARTUP_WARN_AFTER,
                "must not exceed the startup timeout",
            ));
        }
        let write_lease_timeout_ms = flat.duration_ms_or(WRITE_LEASE_TIMEOUT, defaults.write_lease_timeout_ms)?;
        let shutdown_timeout_ms = flat.duration_ms_or(SHUTDOWN_TIMEOUT, defaults.shutdown_timeout_ms)?;

        Ok(Self {
            cluster_id,
            host,
            bind_host,
            rpc_port,
            rpc_concurrency,
            write_session_limits,
            write_target_limits,
            file_layout_defaults,
            http_port,
            storage_dir,
            raft: RaftConfig::default(),
            authority: MetadataAuthorityConfig::default(),
            namespace_list,
            block_cleanup,
            namespace_delete,
            worker_liveness,
            startup,
            write_lease_timeout_ms,
            shutdown_timeout_ms,
            observability,
        })
    }
}

/// Preserves the capacity relationships required by the raw gRPC concurrency layer.
fn validate_rpc_concurrency(config: &MetadataRpcConcurrencyConfig) -> Result<(), CommonError> {
    if config.max_concurrent_requests > MAX_GRPC_CONCURRENT_REQUESTS {
        return Err(invalid_config(
            RPC_MAX_CONCURRENT_REQUESTS,
            "exceeds the runtime semaphore maximum",
        ));
    }
    if config.max_concurrent_requests_per_connection > config.max_concurrent_requests {
        return Err(invalid_config(
            RPC_MAX_CONCURRENT_REQUESTS_PER_CONNECTION,
            "must not exceed the server-wide maximum",
        ));
    }
    if config.reserved_control_requests >= config.max_concurrent_requests {
        return Err(invalid_config(
            RPC_RESERVED_CONTROL_REQUESTS,
            "must be smaller than the server-wide maximum",
        ));
    }
    Ok(())
}

/// Preserves the relationship between global and per-client write-session limits.
fn validate_write_session_limits(config: &MetadataWriteSessionLimitsConfig) -> Result<(), CommonError> {
    if config.max_active_per_client > config.max_active {
        return Err(invalid_config(
            WRITE_SESSION_MAX_ACTIVE_PER_CLIENT,
            "must not exceed the global write-session maximum",
        ));
    }
    Ok(())
}

/// Preserves the relationship between global, per-session, and file block limits.
fn validate_write_target_limits(config: &MetadataWriteTargetLimitsConfig) -> Result<(), CommonError> {
    if config.max_outstanding_per_session > config.max_outstanding {
        return Err(invalid_config(
            WRITE_TARGET_MAX_OUTSTANDING_PER_SESSION,
            "must not exceed the global write-target maximum",
        ));
    }
    if config.max_outstanding_per_session > MAX_FILE_BLOCKS {
        return Err(invalid_config(
            WRITE_TARGET_MAX_OUTSTANDING_PER_SESSION,
            "must not exceed the compiled file block maximum",
        ));
    }
    Ok(())
}

fn validate_namespace_delete(config: &NamespaceDeleteConfig) -> Result<(), CommonError> {
    if config.max_candidates > MAX_RECLAIM_DETACHED_ROOT_CANDIDATES {
        return Err(invalid_config(
            NAMESPACE_DELETE_MAX_ROOTS,
            "exceeds the replicated protocol maximum",
        ));
    }
    if config.max_entries > MAX_RECLAIM_DETACHED_ROOT_ENTRIES {
        return Err(invalid_config(
            NAMESPACE_DELETE_MAX_ENTRIES,
            "exceeds the replicated protocol maximum",
        ));
    }
    if !(MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES..=MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES)
        .contains(&config.max_batch_bytes)
    {
        return Err(invalid_config(
            NAMESPACE_DELETE_MAX_SIZE,
            "is outside the replicated protocol size range",
        ));
    }
    ensure_backoff_order(
        NAMESPACE_DELETE_RETRY_INITIAL_BACKOFF,
        config.retry_initial_backoff_ms,
        NAMESPACE_DELETE_RETRY_MAX_BACKOFF,
        config.retry_max_backoff_ms,
    )
}

fn ensure_backoff_order(
    _initial_key: &'static str,
    initial: u64,
    max_key: &'static str,
    max: u64,
) -> Result<(), CommonError> {
    if max < initial {
        return Err(invalid_config(max_key, "must not be smaller than the initial backoff"));
    }
    Ok(())
}

fn invalid_config(key: &'static str, detail: &'static str) -> CommonError {
    CommonError::new(CommonErrorKind::InvalidArgument, format!("{key} {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_flat() -> FlatConfig {
        let mut flat = FlatConfig::new();
        flat.set("beryl.logging.format", "compact");
        flat.set("beryl.logging.output", "stderr");
        flat.set("beryl.logging.level", "info");
        flat
    }

    #[test]
    fn active_safety_bounds_are_enforced() {
        let mut flat = base_flat();
        flat.set(LIST_MAX_PAGE_SIZE, 20_000i64);
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(NAMESPACE_DELETE_MAX_SIZE, "2MiB");
        assert!(MetadataConfig::from_flat(flat).is_err());

        for host in [" metadata-01", "http://metadata-01", "metadata-01:18080"] {
            let mut flat = base_flat();
            flat.set(HOST, host);
            assert!(MetadataConfig::from_flat(flat).is_err());
        }

        let mut flat = base_flat();
        flat.set(RPC_MAX_CONCURRENT_REQUESTS, 8i64);
        flat.set(RPC_MAX_CONCURRENT_REQUESTS_PER_CONNECTION, 9i64);
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(RPC_MAX_CONCURRENT_REQUESTS, 8i64);
        flat.set(RPC_RESERVED_CONTROL_REQUESTS, 8i64);
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(RPC_RESERVED_CONTROL_REQUESTS, 0i64);
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(
            RPC_MAX_CONCURRENT_REQUESTS,
            i64::try_from(MAX_GRPC_CONCURRENT_REQUESTS).unwrap(),
        );
        assert!(MetadataConfig::from_flat(flat).is_ok());

        let mut flat = base_flat();
        flat.set(
            RPC_MAX_CONCURRENT_REQUESTS,
            i64::try_from(MAX_GRPC_CONCURRENT_REQUESTS + 1).unwrap(),
        );
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(WRITE_SESSION_MAX_ACTIVE, 8i64);
        flat.set(WRITE_SESSION_MAX_ACTIVE_PER_CLIENT, 9i64);
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(WRITE_TARGET_MAX_OUTSTANDING, 8i64);
        flat.set(WRITE_TARGET_MAX_OUTSTANDING_PER_SESSION, 9i64);
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(
            WRITE_TARGET_MAX_OUTSTANDING_PER_SESSION,
            i64::try_from(MAX_FILE_BLOCKS + 1).unwrap(),
        );
        assert!(MetadataConfig::from_flat(flat).is_err());

        let mut flat = base_flat();
        flat.set(FILE_BLOCK_SIZE_DEFAULT, "0");
        assert!(MetadataConfig::from_flat(flat).is_err());
    }
}
