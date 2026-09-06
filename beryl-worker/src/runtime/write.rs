// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-local ownership for active block writes.

use super::block::BlockPin;
use crate::runtime::DataRpcPermit;
use beryl_types::ids::BlockId;
use beryl_types::{FencingToken, GroupName};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Exact worker-local identity of write state owned by one write RPC.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BlockWriteKey {
    pub(crate) group_name: GroupName,
    pub(crate) block_id: BlockId,
}

struct BlockWriteEntry {
    io: Mutex<BlockWriteIoState>,
    token: FencingToken,
    retired: CancellationToken,
}

impl BlockWriteEntry {
    fn new(rpc_permit: DataRpcPermit, token: FencingToken, block_pin: BlockPin) -> Self {
        Self {
            io: Mutex::new(BlockWriteIoState {
                inflight: 0,
                cleanup_running: false,
                resources: Some((block_pin, rpc_permit)),
            }),
            retired: CancellationToken::new(),
            token,
        }
    }

    fn begin_io(self: &Arc<Self>) -> Option<BlockWriteIoGuard> {
        let mut io = self.io.lock();
        if self.retired.is_cancelled() {
            return None;
        }
        io.inflight += 1;
        Some(BlockWriteIoGuard {
            entry: Arc::clone(self),
        })
    }

    fn retire(&self) {
        let _io = self.io.lock();
        self.retired.cancel();
    }

    fn retire_and_claim_cleanup(&self, drain: bool) -> bool {
        let mut io = self.io.lock();
        if drain {
            self.retired.cancel();
        }
        if !self.retired.is_cancelled() || io.inflight != 0 || io.cleanup_running {
            return false;
        }
        io.cleanup_running = true;
        true
    }

    fn release_cleanup(&self) {
        let mut io = self.io.lock();
        debug_assert!(io.cleanup_running);
        io.cleanup_running = false;
    }
}

struct BlockWriteIoState {
    inflight: usize,
    cleanup_running: bool,
    // Cleanup releases admission even if transport backpressure retains the old RPC.
    resources: Option<(BlockPin, DataRpcPermit)>,
}

struct BlockWriteRegistryState {
    writes: HashMap<BlockWriteKey, Arc<BlockWriteEntry>>,
    cleanup_order: VecDeque<BlockWriteKey>,
}

/// Prevents concurrent RPCs from owning the same block write and retains
/// cancelled writes until process-owned cleanup releases their local files.
pub(crate) struct BlockWriteRegistry {
    inner: Mutex<BlockWriteRegistryState>,
    changed: Notify,
}

/// RPC-owned registration. Dropping it before completion schedules cleanup
/// without performing filesystem IO from a cancellation path.
pub(crate) struct BlockWriteRegistration {
    registry: Arc<BlockWriteRegistry>,
    key: BlockWriteKey,
    entry: Arc<BlockWriteEntry>,
    completed: bool,
}

impl BlockWriteRegistration {
    /// Wakes idle streams when takeover, reclamation, or shutdown stops their IO.
    pub(crate) async fn retired(&self) {
        self.entry.retired.cancelled().await;
    }

    /// Acquires an IO lease that keeps cleanup behind a detached blocking task.
    pub(crate) fn begin_io(&self) -> Option<BlockWriteIoGuard> {
        self.entry.begin_io()
    }

    /// Removes exactly this RPC's registry entry after terminal local work.
    pub(crate) fn complete(mut self) {
        self.registry.complete_registration(&self.key, &self.entry);
        self.completed = true;
    }
}

impl Drop for BlockWriteRegistration {
    fn drop(&mut self) {
        if !self.completed {
            self.entry.retire();
        }
    }
}

/// Lease moved into one blocking store operation. Cleanup cannot select the
/// owning write until the operation has actually exited, even if its async
/// caller was cancelled and dropped its `JoinHandle`.
pub(crate) struct BlockWriteIoGuard {
    entry: Arc<BlockWriteEntry>,
}

impl Drop for BlockWriteIoGuard {
    fn drop(&mut self) {
        let mut io = self.entry.io.lock();
        io.inflight = io.inflight.checked_sub(1).expect("block write IO guard is balanced");
    }
}

/// Exclusive cleanup claim for one retiring write. Dropping the claim after an
/// error or unwind makes the exact registry entry eligible for a later retry.
pub(crate) struct RetiringBlockWrite {
    pub(crate) key: BlockWriteKey,
    registry: Arc<BlockWriteRegistry>,
    entry: Arc<BlockWriteEntry>,
    claimed: bool,
}

impl RetiringBlockWrite {
    /// Removes the exact registry entry after its terminal store operation.
    pub(crate) fn complete(mut self) -> bool {
        let removed = self.registry.remove_exact(&self.key, &self.entry);
        if removed {
            self.claimed = false;
        }
        removed
    }
}

impl Drop for RetiringBlockWrite {
    fn drop(&mut self) {
        if self.claimed {
            self.registry.release_cleanup_claim(&self.key, &self.entry);
        }
    }
}

impl BlockWriteRegistry {
    pub(crate) fn new() -> Self {
        Self {
            changed: Notify::new(),
            inner: Mutex::new(BlockWriteRegistryState {
                writes: HashMap::new(),
                cleanup_order: VecDeque::new(),
            }),
        }
    }

    /// A newer authorized writer retires the old stream and waits for its actual IO and cleanup.
    /// Same-epoch overlap is rejected; persisted tokens fence delayed requests after entries disappear.
    pub(crate) async fn register(
        self: &Arc<Self>,
        key: BlockWriteKey,
        rpc_permit: DataRpcPermit,
        token: FencingToken,
        block_pin: BlockPin,
    ) -> Option<BlockWriteRegistration> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut inner = self.inner.lock();
                if let Some(entry) = inner.writes.get(&key) {
                    if token.epoch <= entry.token.epoch {
                        return None;
                    }
                    entry.retire();
                } else {
                    let entry = Arc::new(BlockWriteEntry::new(rpc_permit, token, block_pin));
                    inner.writes.insert(key.clone(), Arc::clone(&entry));
                    inner.cleanup_order.push_back(key.clone());
                    return Some(BlockWriteRegistration {
                        registry: Arc::clone(self),
                        key,
                        entry,
                        completed: false,
                    });
                }
            }
            notified.await;
        }
    }

    /// Stops admitting IO before reclamation waits for the block's access pins.
    pub(crate) fn retire(&self, key: &BlockWriteKey) {
        if let Some(entry) = self.inner.lock().writes.get(key) {
            entry.retire();
        }
    }

    /// Selects and atomically claims at most one bounded batch. Normal passes
    /// only claim cancelled writes; shutdown drain first retires examined writes.
    pub(crate) fn take_cleanup_batch(self: &Arc<Self>, limit: usize, drain: bool) -> Vec<RetiringBlockWrite> {
        let mut inner = self.inner.lock();
        let examined = limit.min(inner.cleanup_order.len());
        let mut selected = Vec::with_capacity(examined);
        for _ in 0..examined {
            let Some(key) = inner.cleanup_order.pop_front() else {
                break;
            };
            let Some(entry) = inner.writes.get(&key).cloned() else {
                continue;
            };
            if entry.retire_and_claim_cleanup(drain) {
                selected.push(RetiringBlockWrite {
                    key: key.clone(),
                    registry: Arc::clone(self),
                    entry,
                    claimed: true,
                });
            }
            inner.cleanup_order.push_back(key);
        }
        selected
    }

    fn release_cleanup_claim(&self, key: &BlockWriteKey, entry: &Arc<BlockWriteEntry>) {
        let inner = self.inner.lock();
        if inner
            .writes
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, entry))
        {
            entry.release_cleanup();
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.inner.lock().writes.len()
    }

    fn complete_registration(&self, key: &BlockWriteKey, entry: &Arc<BlockWriteEntry>) -> bool {
        let mut inner = self.inner.lock();
        if !inner
            .writes
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, entry))
        {
            return false;
        }
        let mut io = entry.io.lock();
        if io.cleanup_running {
            return false;
        }
        debug_assert_eq!(io.inflight, 0);
        entry.retired.cancel();
        let resources = io.resources.take();
        drop(io);
        inner.writes.remove(key);
        self.changed.notify_waiters();
        if let Some(position) = inner.cleanup_order.iter().position(|queued| queued == key) {
            inner.cleanup_order.remove(position);
        }
        drop(inner);
        drop(resources);
        true
    }

    fn remove_exact(&self, key: &BlockWriteKey, entry: &Arc<BlockWriteEntry>) -> bool {
        let mut inner = self.inner.lock();
        if !inner
            .writes
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, entry))
        {
            return false;
        }
        let mut io = entry.io.lock();
        debug_assert_eq!(io.inflight, 0);
        entry.retired.cancel();
        let resources = io.resources.take();
        drop(io);
        inner.writes.remove(key);
        self.changed.notify_waiters();
        if let Some(position) = inner.cleanup_order.iter().position(|queued| queued == key) {
            inner.cleanup_order.remove(position);
        }
        drop(inner);
        drop(resources);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockWriteKey, BlockWriteRegistry};
    use crate::runtime::DataRpcPermit;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId};
    use beryl_types::GroupName;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    fn key() -> BlockWriteKey {
        BlockWriteKey {
            group_name: GroupName::parse("root").expect("group name"),
            block_id: BlockId::new(InodeId::new(7), BlockIndex::new(3)),
        }
    }

    fn rpc_permit() -> DataRpcPermit {
        let slots = Arc::new(Semaphore::new(1));
        DataRpcPermit::new(slots.try_acquire_owned().expect("test write capacity"), "write")
    }

    async fn register(registry: &Arc<BlockWriteRegistry>) -> Option<super::BlockWriteRegistration> {
        let key = key();
        let pin = crate::runtime::block::BlockManager::default()
            .pin_block(&key.group_name, key.block_id)
            .unwrap();
        let token = beryl_types::FencingToken::new(
            key.block_id,
            beryl_types::ClientId::new(9),
            beryl_types::LeaseEpoch::new(1),
        );
        registry.register(key, rpc_permit(), token, pin).await
    }

    #[tokio::test]
    async fn cancellation_retires_exact_owner_and_allows_reuse_after_cleanup() {
        let registry = Arc::new(BlockWriteRegistry::new());
        let registration = register(&registry).await.expect("first owner");
        assert!(register(&registry).await.is_none());

        drop(registration);
        let candidates = registry.take_cleanup_batch(1, false);
        assert_eq!(candidates.len(), 1);
        assert!(candidates.into_iter().next().expect("cleanup claim").complete());
        assert!(register(&registry).await.is_some());

        let registry = Arc::new(BlockWriteRegistry::new());
        let manager = crate::runtime::block::BlockManager::default();
        let slots = Arc::new(Semaphore::new(1));
        let key = key();
        let owner = registry
            .register(
                key.clone(),
                DataRpcPermit::new(Arc::clone(&slots).try_acquire_owned().unwrap(), "write"),
                beryl_types::FencingToken::new(
                    key.block_id,
                    beryl_types::ClientId::new(9),
                    beryl_types::LeaseEpoch::new(1),
                ),
                manager.pin_block(&key.group_name, key.block_id).unwrap(),
            )
            .await
            .unwrap();
        let io = owner.begin_io().unwrap();
        let reclaim = manager.begin_reclaim(&key.group_name, key.block_id).unwrap();
        registry.retire(&key);
        assert!(owner.begin_io().is_none());
        assert!(registry.take_cleanup_batch(1, false).is_empty());
        assert_eq!(slots.available_permits(), 0);
        drop(io);
        assert!(registry.take_cleanup_batch(1, false).pop().unwrap().complete());
        // The old RPC is retained and never polled or dropped while cleanup releases its resources.
        assert!(futures::poll!(std::pin::pin!(reclaim.wait_for_pins())).is_ready());
        assert_eq!(slots.available_permits(), 1);
        assert!(owner.begin_io().is_none());
        reclaim.complete();
        assert!(manager.pin_block(&key.group_name, key.block_id).is_ok());
    }
}
