// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Bounded execution of metadata-authorized block cleanup commands.

use crate::control::{Registration, RegistrationSet};
use crate::error::WorkerError;
use crate::{observe, ReclaimBlockRequest, ReclaimBlockResult, WorkerCore};
use beryl_types::{BlockId, GroupName, WorkerId, WorkerRunId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// One exact block identity received from an authenticated heartbeat response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockCleanupCommand {
    /// Logical identity of the block selected by metadata.
    pub block_id: BlockId,
}

/// Bounds process-local cleanup work and retry pressure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockCleanupOptions {
    /// Maximum number of distinct queued and active cleanup commands.
    pub max_pending: usize,
    /// Maximum number of local reclamation attempts that may run concurrently.
    pub max_concurrent: usize,
    /// Delay before retrying the first transient local failure.
    pub retry_initial_backoff: Duration,
    /// Upper bound for exponential retry backoff.
    pub retry_max_backoff: Duration,
}

impl Default for BlockCleanupOptions {
    fn default() -> Self {
        Self {
            max_pending: 1_024,
            max_concurrent: 4,
            retry_initial_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CleanupReplicaKey {
    group_name: GroupName,
    worker_id: WorkerId,
    worker_run_id: WorkerRunId,
    block_id: BlockId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CleanupPhase {
    Queued,
    Reclaiming,
}

struct BlockCleanupInner {
    core: Arc<WorkerCore>,
    registrations: Arc<RegistrationSet>,
    options: BlockCleanupOptions,
    pending: Mutex<HashMap<CleanupReplicaKey, CleanupPhase>>,
    concurrency: Arc<Semaphore>,
}

/// Process-run-local executor for exact, idempotent block cleanup commands.
///
/// Accepted work is bounded across queued and active tasks. Exact duplicates
/// are coalesced and transient local failures retry with capped backoff.
/// Admission and execution remain bound to the authorizing Worker run.
#[derive(Clone)]
pub struct BlockCleanupExecutor {
    inner: Arc<BlockCleanupInner>,
    sender: Sender<CleanupReplicaKey>,
}

/// Owned task lifecycle for the Worker block cleanup executor.
///
/// The executor is cloned into heartbeat handling, while this value remains
/// with the process shutdown owner so queued and retrying reclamation work is
/// cancelled and awaited before local storage is dropped.
pub struct BlockCleanupRuntime {
    executor: BlockCleanupExecutor,
    shutdown: CancellationToken,
    force: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl BlockCleanupRuntime {
    /// Starts cleanup with an explicit process-owned task handle.
    pub fn start(
        core: Arc<WorkerCore>,
        registrations: Arc<RegistrationSet>,
        options: BlockCleanupOptions,
    ) -> Result<Self, WorkerError> {
        let (executor, receiver) = BlockCleanupExecutor::build(core, registrations, options)?;
        let shutdown = CancellationToken::new();
        let force = CancellationToken::new();
        let task = tokio::spawn(run_executor(
            Arc::clone(&executor.inner),
            receiver,
            shutdown.clone(),
            force.clone(),
        ));
        Ok(Self {
            executor,
            shutdown,
            force,
            task: Some(task),
        })
    }

    /// Returns the command sink shared with authenticated heartbeat handling.
    pub fn executor(&self) -> BlockCleanupExecutor {
        self.executor.clone()
    }

    /// Drains cleanup until `deadline`, then aborts and awaits the executor.
    ///
    /// Returns `true` when the process owner had to force cancellation.
    pub async fn shutdown_until(mut self, deadline: Instant) -> Result<bool, JoinError> {
        self.shutdown.cancel();
        let Some(mut task) = self.task.take() else {
            return Ok(false);
        };
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(result) => {
                result?;
                Ok(false)
            }
            Err(_) => {
                self.force.cancel();
                task.await?;
                Ok(true)
            }
        }
    }
}

impl Drop for BlockCleanupRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.force.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl BlockCleanupExecutor {
    fn build(
        core: Arc<WorkerCore>,
        registrations: Arc<RegistrationSet>,
        options: BlockCleanupOptions,
    ) -> Result<(Self, Receiver<CleanupReplicaKey>), WorkerError> {
        validate_options(&options)?;
        let (sender, receiver) = mpsc::channel(options.max_pending);
        let inner = Arc::new(BlockCleanupInner {
            core,
            registrations,
            concurrency: Arc::new(Semaphore::new(options.max_concurrent)),
            options,
            pending: Mutex::new(HashMap::new()),
        });
        Ok((Self { inner, sender }, receiver))
    }

    /// Enqueues authenticated commands bound to one accepted worker run.
    ///
    /// Exact duplicates are coalesced. Queue saturation drops only the local
    /// work item; metadata will redispatch the still-reported Ready replica
    /// after its retry backoff.
    pub fn enqueue(&self, registration: &Registration, commands: impl IntoIterator<Item = BlockCleanupCommand>) {
        for command in commands {
            let key = CleanupReplicaKey {
                group_name: registration.group_name.clone(),
                worker_id: registration.worker_id,
                worker_run_id: registration.worker_run_id,
                block_id: command.block_id,
            };

            let mut pending = self.inner.pending.lock().expect("cleanup state poisoned");
            if pending.contains_key(&key) {
                observe::record_cleanup_enqueue("duplicate");
                continue;
            }
            if pending.len() >= self.inner.options.max_pending {
                observe::record_cleanup_enqueue("full");
                warn!(
                    group_name = %key.group_name,
                    worker_id = key.worker_id.as_raw(),
                    worker_run_id = %key.worker_run_id,
                    block_id = %key.block_id,
                    "Worker cleanup queue is full; metadata must retry the command"
                );
                continue;
            }
            pending.insert(key.clone(), CleanupPhase::Queued);
            observe::set_cleanup_queue_depth(pending.len());
            drop(pending);

            if self.sender.try_send(key.clone()).is_err() {
                let mut pending = self.inner.pending.lock().expect("cleanup state poisoned");
                pending.remove(&key);
                observe::set_cleanup_queue_depth(pending.len());
                observe::record_cleanup_enqueue("unavailable");
            } else {
                observe::record_cleanup_enqueue("accepted");
            }
        }
    }
}

/// Drains the bounded command channel and owns all process-local cleanup tasks.
///
/// Graceful shutdown stops accepting new tasks and drains active tasks until forced;
/// any undeleted replica remains discoverable through a later block report.
async fn run_executor(
    inner: Arc<BlockCleanupInner>,
    mut receiver: Receiver<CleanupReplicaKey>,
    shutdown: CancellationToken,
    force: CancellationToken,
) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = force.cancelled() => {
                finish_executor(&inner, &mut tasks, true).await;
                return;
            }
            _ = shutdown.cancelled() => {
                receiver.close();
                finish_executor_until_forced(&inner, &mut tasks, force).await;
                return;
            }
            command = receiver.recv() => {
                let Some(key) = command else {
                    finish_executor_until_forced(&inner, &mut tasks, force).await;
                    return;
                };
                tasks.spawn(run_cleanup_task(Arc::clone(&inner), key));
            }
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    warn!(%error, "Worker cleanup task terminated unexpectedly");
                }
            }
        }
    }
}

/// Waits for active cleanup tasks while retaining a force-cancel path for the process deadline.
async fn finish_executor_until_forced(inner: &BlockCleanupInner, tasks: &mut JoinSet<()>, force: CancellationToken) {
    while !tasks.is_empty() {
        tokio::select! {
            biased;
            _ = force.cancelled() => {
                finish_executor(inner, tasks, true).await;
                return;
            }
            _ = tasks.join_next() => {}
        }
    }
    finish_executor(inner, tasks, false).await;
}

/// Reaps every cleanup task and clears queue metrics before the executor returns.
async fn finish_executor(inner: &BlockCleanupInner, tasks: &mut JoinSet<()>, abort: bool) {
    if abort {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    inner.pending.lock().expect("cleanup state poisoned").clear();
    observe::set_cleanup_queue_depth(0);
    observe::set_cleanup_reclaiming(0);
}

/// Reclaims one exact replica while its worker registration remains current.
///
/// Registration is rechecked after waiting for the concurrency permit so work
/// queued for an obsolete Worker run cannot reach local storage. Local failures
/// retry with capped backoff while that run remains current; Metadata authorizes
/// a never-reused block identity.
async fn run_cleanup_task(inner: Arc<BlockCleanupInner>, key: CleanupReplicaKey) {
    let mut attempts = 0u32;
    loop {
        if !registration_matches(&inner.registrations, &key) {
            finish_task(&inner, &key);
            observe::record_cleanup_result("stale_run");
            return;
        }
        let Ok(concurrency) = Arc::clone(&inner.concurrency).acquire_owned().await else {
            finish_task(&inner, &key);
            return;
        };
        if !registration_matches(&inner.registrations, &key) {
            drop(concurrency);
            finish_task(&inner, &key);
            observe::record_cleanup_result("stale_run");
            return;
        }
        mark_reclaiming(&inner, &key);

        let request = ReclaimBlockRequest {
            group_name: key.group_name.clone(),
            block_id: key.block_id,
        };
        match inner.core.reclaim_block(request).await {
            Ok(ReclaimBlockResult::Deleted { .. }) => {
                finish_task(&inner, &key);
                observe::record_cleanup_result("deleted");
                info!(
                    group_name = %key.group_name,
                    worker_id = key.worker_id.as_raw(),
                    worker_run_id = %key.worker_run_id,
                    block_id = %key.block_id,
                    "Worker reclaimed metadata-authorized block"
                );
                return;
            }
            Ok(ReclaimBlockResult::AlreadyAbsent) => {
                finish_task(&inner, &key);
                observe::record_cleanup_result("already_absent");
                return;
            }
            Err(error) => {
                attempts = attempts.saturating_add(1);
                observe::record_cleanup_result("retry");
                warn!(
                    group_name = %key.group_name,
                    worker_id = key.worker_id.as_raw(),
                    worker_run_id = %key.worker_run_id,
                    block_id = %key.block_id,
                    attempts,
                    %error,
                    "Worker block cleanup failed; retrying locally"
                );
            }
        }

        drop(concurrency);
        tokio::time::sleep(retry_backoff(&inner.options, attempts)).await;
    }
}

/// Returns whether a cleanup key still belongs to the active registration.
fn registration_matches(registrations: &RegistrationSet, key: &CleanupReplicaKey) -> bool {
    registrations.registration(&key.group_name).is_some_and(|registration| {
        registration.worker_id == key.worker_id && registration.worker_run_id.matches(key.worker_run_id)
    })
}

fn mark_reclaiming(inner: &BlockCleanupInner, key: &CleanupReplicaKey) {
    let mut pending = inner.pending.lock().expect("cleanup state poisoned");
    if let Some(phase) = pending.get_mut(key) {
        *phase = CleanupPhase::Reclaiming;
    }
    let reclaiming = pending
        .values()
        .filter(|phase| matches!(phase, CleanupPhase::Reclaiming))
        .count();
    observe::set_cleanup_reclaiming(reclaiming);
}

fn finish_task(inner: &BlockCleanupInner, key: &CleanupReplicaKey) {
    let mut pending = inner.pending.lock().expect("cleanup state poisoned");
    pending.remove(key);
    let reclaiming = pending
        .values()
        .filter(|phase| matches!(phase, CleanupPhase::Reclaiming))
        .count();
    observe::set_cleanup_queue_depth(pending.len());
    observe::set_cleanup_reclaiming(reclaiming);
}

/// Computes capped exponential backoff without overflowing long-running tasks.
fn retry_backoff(options: &BlockCleanupOptions, attempts: u32) -> Duration {
    let multiplier = 1_u128 << attempts.saturating_sub(1).min(63);
    let delay = options.retry_initial_backoff.as_millis().saturating_mul(multiplier);
    Duration::from_millis(delay.min(options.retry_max_backoff.as_millis()) as u64)
}

/// Validates bounds that are required for bounded, live cleanup execution.
fn validate_options(options: &BlockCleanupOptions) -> Result<(), WorkerError> {
    if options.max_pending == 0 {
        return Err(WorkerError::InvalidArgument(
            "cleanup max_pending must be greater than zero".to_string(),
        ));
    }
    if options.max_concurrent == 0 {
        return Err(WorkerError::InvalidArgument(
            "cleanup max_concurrent must be greater than zero".to_string(),
        ));
    }
    if options.retry_initial_backoff.is_zero() {
        return Err(WorkerError::InvalidArgument(
            "cleanup retry_initial_backoff must be greater than zero".to_string(),
        ));
    }
    if options.retry_initial_backoff > options.retry_max_backoff {
        return Err(WorkerError::InvalidArgument(
            "cleanup retry_initial_backoff must not exceed retry_max_backoff".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::block::{
        BlockMetaPayload, CheckpointBlockRequest, LocalBlockStore, OpenBlockWriteRequest, ReclaimBlockState,
        StoreResult,
    };
    use beryl_types::{BlockIndex, InodeId};
    use bytes::Bytes;
    use std::ops::Deref;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    enum ReclaimBehavior {
        Succeed,
        Fail,
    }

    struct ControlledStore {
        behavior: Mutex<ReclaimBehavior>,
        delay: Duration,
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        reclaimed: Mutex<Vec<BlockId>>,
    }

    impl ControlledStore {
        fn new(behavior: ReclaimBehavior, delay: Duration) -> Self {
            Self {
                behavior: Mutex::new(behavior),
                delay,
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                reclaimed: Mutex::new(Vec::new()),
            }
        }

        fn set_behavior(&self, behavior: ReclaimBehavior) {
            *self.behavior.lock().expect("controlled store poisoned") = behavior;
        }
    }

    impl LocalBlockStore for ControlledStore {
        fn open_block_write(&self, _req: OpenBlockWriteRequest) -> StoreResult<BlockMetaPayload> {
            panic!("unused test operation")
        }

        fn write_at(&self, _group_name: &GroupName, _block_id: BlockId, _offset: u64, _data: Bytes) -> StoreResult<()> {
            panic!("unused test operation")
        }

        fn checkpoint_block(&self, _req: CheckpointBlockRequest) -> StoreResult<BlockMetaPayload> {
            panic!("unused test operation")
        }

        fn read_at(&self, _group_name: &GroupName, _block_id: BlockId, _offset: u64, _len: u64) -> StoreResult<Bytes> {
            panic!("unused test operation")
        }

        fn load_meta(&self, _group_name: &GroupName, _block_id: BlockId) -> StoreResult<BlockMetaPayload> {
            panic!("unused test operation")
        }

        fn inspect_reclaim_block(&self, _req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
            Ok(ReclaimBlockState::Ready)
        }

        fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            let behavior = *self.behavior.lock().expect("controlled store poisoned");
            self.active.fetch_sub(1, Ordering::SeqCst);
            match behavior {
                ReclaimBehavior::Succeed => {
                    self.reclaimed
                        .lock()
                        .expect("controlled store poisoned")
                        .push(req.block_id);
                    Ok(ReclaimBlockResult::Deleted { effective_len: 1 })
                }
                ReclaimBehavior::Fail => Err(WorkerError::DiskError("injected cleanup failure".to_string())),
            }
        }

        fn discard_unsynced_suffix(&self, _group_name: &GroupName, _block_id: BlockId) -> StoreResult<()> {
            panic!("unused test operation")
        }
    }

    #[tokio::test]
    async fn queue_full_drops_work_until_metadata_redispatches() {
        let run_id = WorkerRunId::new();
        let registrations = registered(run_id);
        let store = Arc::new(ControlledStore::new(ReclaimBehavior::Fail, Duration::ZERO));
        let executor = executor(
            Arc::clone(&store),
            Arc::clone(&registrations),
            BlockCleanupOptions {
                max_pending: 1,
                max_concurrent: 1,
                retry_initial_backoff: Duration::from_millis(20),
                retry_max_backoff: Duration::from_millis(20),
            },
        );
        let first = test_block_id(1);
        let second = test_block_id(2);

        executor.enqueue(&registration(run_id), [command(first)]);
        wait_for(|| store.calls.load(Ordering::SeqCst) > 0).await;
        executor.enqueue(&registration(run_id), [command(second)]);
        assert_eq!(executor.inner.pending.lock().expect("cleanup state poisoned").len(), 1);

        store.set_behavior(ReclaimBehavior::Succeed);
        wait_for(|| {
            executor
                .inner
                .pending
                .lock()
                .expect("cleanup state poisoned")
                .is_empty()
        })
        .await;
        assert_eq!(*store.reclaimed.lock().expect("controlled store poisoned"), vec![first]);

        executor.enqueue(&registration(run_id), [command(second)]);
        wait_for(|| {
            executor
                .inner
                .pending
                .lock()
                .expect("cleanup state poisoned")
                .is_empty()
        })
        .await;
        assert_eq!(
            *store.reclaimed.lock().expect("controlled store poisoned"),
            vec![first, second]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn max_concurrent_bounds_active_reclaims() {
        let run_id = WorkerRunId::new();
        let registrations = registered(run_id);
        let store = Arc::new(ControlledStore::new(
            ReclaimBehavior::Succeed,
            Duration::from_millis(50),
        ));
        let executor = executor(
            Arc::clone(&store),
            registrations,
            BlockCleanupOptions {
                max_pending: 4,
                max_concurrent: 2,
                retry_initial_backoff: Duration::from_millis(10),
                retry_max_backoff: Duration::from_millis(10),
            },
        );

        executor.enqueue(
            &registration(run_id),
            (1..=4).map(|index| command(test_block_id(index))),
        );
        wait_for(|| {
            executor
                .inner
                .pending
                .lock()
                .expect("cleanup state poisoned")
                .is_empty()
        })
        .await;

        assert_eq!(store.calls.load(Ordering::SeqCst), 4);
        assert_eq!(store.max_active.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn permanent_failure_retries_with_backoff_and_recovers() {
        let run_id = WorkerRunId::new();
        let registrations = registered(run_id);
        let store = Arc::new(ControlledStore::new(ReclaimBehavior::Fail, Duration::ZERO));
        let executor = executor(
            Arc::clone(&store),
            registrations,
            BlockCleanupOptions {
                max_pending: 2,
                max_concurrent: 1,
                retry_initial_backoff: Duration::from_millis(20),
                retry_max_backoff: Duration::from_millis(20),
            },
        );
        let block_id = test_block_id(1);

        executor.enqueue(&registration(run_id), [command(block_id)]);
        tokio::time::sleep(Duration::from_millis(75)).await;
        let failed_calls = store.calls.load(Ordering::SeqCst);
        assert!(
            (2..=5).contains(&failed_calls),
            "unexpected retry count: {failed_calls}"
        );
        assert_eq!(executor.inner.pending.lock().expect("cleanup state poisoned").len(), 1);

        store.set_behavior(ReclaimBehavior::Succeed);
        wait_for(|| {
            executor
                .inner
                .pending
                .lock()
                .expect("cleanup state poisoned")
                .is_empty()
        })
        .await;
        assert_eq!(
            *store.reclaimed.lock().expect("controlled store poisoned"),
            vec![block_id]
        );
    }

    #[tokio::test]
    async fn owned_runtime_cancels_retrying_cleanup_work() {
        let run_id = WorkerRunId::new();
        let registrations = registered(run_id);
        let store = Arc::new(ControlledStore::new(ReclaimBehavior::Fail, Duration::ZERO));
        let core = Arc::new(WorkerCore::with_local_store(1_024, 1_024, store.clone()));
        let runtime = BlockCleanupRuntime::start(
            core,
            registrations,
            BlockCleanupOptions {
                max_pending: 2,
                max_concurrent: 1,
                retry_initial_backoff: Duration::from_secs(30),
                retry_max_backoff: Duration::from_secs(30),
            },
        )
        .unwrap();
        runtime
            .executor()
            .enqueue(&registration(run_id), [command(test_block_id(1))]);
        wait_for(|| store.calls.load(Ordering::SeqCst) == 1).await;

        let forced = tokio::time::timeout(Duration::from_secs(1), runtime.shutdown_until(Instant::now()))
            .await
            .expect("cleanup shutdown must cancel retry backoff")
            .unwrap();
        assert!(forced);
    }

    #[tokio::test]
    async fn shutdown_deadline_forces_and_awaits_retrying_cleanup_work() {
        let run_id = WorkerRunId::new();
        let registrations = registered(run_id);
        let store = Arc::new(ControlledStore::new(ReclaimBehavior::Fail, Duration::ZERO));
        let core = Arc::new(WorkerCore::with_local_store(1_024, 1_024, store.clone()));
        let runtime = BlockCleanupRuntime::start(
            core,
            registrations,
            BlockCleanupOptions {
                max_pending: 2,
                max_concurrent: 1,
                retry_initial_backoff: Duration::from_secs(30),
                retry_max_backoff: Duration::from_secs(30),
            },
        )
        .unwrap();
        let executor = runtime.executor();
        executor.enqueue(&registration(run_id), [command(test_block_id(1))]);
        wait_for(|| store.calls.load(Ordering::SeqCst) == 1).await;

        let forced = runtime
            .shutdown_until(Instant::now() + Duration::from_millis(20))
            .await
            .unwrap();

        assert!(forced);
        assert!(executor
            .inner
            .pending
            .lock()
            .expect("cleanup state poisoned")
            .is_empty());
    }

    struct TestExecutor {
        executor: BlockCleanupExecutor,
        _runtime: BlockCleanupRuntime,
    }

    impl Deref for TestExecutor {
        type Target = BlockCleanupExecutor;

        fn deref(&self) -> &Self::Target {
            &self.executor
        }
    }

    fn executor(
        store: Arc<ControlledStore>,
        registrations: Arc<RegistrationSet>,
        options: BlockCleanupOptions,
    ) -> TestExecutor {
        let core = Arc::new(WorkerCore::with_local_store(1_024, 1_024, store));
        let runtime = BlockCleanupRuntime::start(core, registrations, options).expect("start cleanup executor");
        TestExecutor {
            executor: runtime.executor(),
            _runtime: runtime,
        }
    }

    fn registered(run_id: WorkerRunId) -> Arc<RegistrationSet> {
        let registrations = Arc::new(RegistrationSet::new());
        registrations.record_registered(registration(run_id));
        registrations
    }

    fn registration(run_id: WorkerRunId) -> Registration {
        Registration {
            group_name: GroupName::parse("root").expect("group name"),
            worker_id: WorkerId::new(42),
            worker_run_id: run_id,
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        }
    }

    fn command(block_id: BlockId) -> BlockCleanupCommand {
        BlockCleanupCommand { block_id }
    }

    fn test_block_id(index: u32) -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(index))
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition must become true");
    }
}
