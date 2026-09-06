// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata target selection and refresh cache updates.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::{GroupName, GroupStateWatermark};
use parking_lot::RwLock;

use crate::config::ClientConfig;
use crate::error::{ClientError, ClientResult, RefreshHint};
use crate::metadata::MetadataAuthorityUpdate;
use crate::runtime::context::{AttemptContext, OperationContext};

const METADATA_TARGET_CACHE_LIMIT: usize = 300;

/// Configured metadata group bootstrap target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetadataGroupTargets {
    /// Stable metadata group name.
    pub(crate) group_name: GroupName,
    /// Metadata endpoints for this group.
    pub(crate) endpoints: Vec<String>,
}

/// Routing and freshness caches protected by one lock so one response update
/// cannot become partially visible to a concurrent request.
#[derive(Debug)]
struct MetadataTargetState {
    groups: Vec<MetadataGroupTargets>,
    leader_cache: HashMap<GroupName, String>,
    route_cache: HashMap<String, GroupName>,
    route_cache_order: VecDeque<String>,
    mount_epoch_cache: HashMap<String, u64>,
    mount_epoch_cache_order: VecDeque<String>,
    route_epoch_cache: HashMap<String, u64>,
    route_epoch_cache_order: VecDeque<String>,
    watermarks: HashMap<GroupName, GroupStateWatermark>,
}

impl MetadataTargetState {
    fn insert_route(&mut self, path: String, group_name: GroupName) {
        let MetadataTargetState {
            route_cache,
            route_cache_order,
            ..
        } = self;
        insert_bounded(route_cache, route_cache_order, path, group_name);
    }

    fn record_mount_epoch_hint(&mut self, operation_path: Option<&str>, mount_prefix: Option<&str>, epoch: u64) {
        let MetadataTargetState {
            mount_epoch_cache,
            mount_epoch_cache_order,
            ..
        } = self;
        record_epoch_hint(
            mount_epoch_cache,
            mount_epoch_cache_order,
            operation_path,
            mount_prefix,
            epoch,
        );
    }

    fn record_route_epoch_hint(&mut self, operation_path: Option<&str>, mount_prefix: Option<&str>, epoch: u64) {
        let MetadataTargetState {
            route_epoch_cache,
            route_epoch_cache_order,
            ..
        } = self;
        record_epoch_hint(
            route_epoch_cache,
            route_epoch_cache_order,
            operation_path,
            mount_prefix,
            epoch,
        );
    }
}

/// Owns metadata target selection and monotonic correctness cache updates from
/// successful response authority and structured refresh signals.
#[derive(Clone, Debug)]
pub(crate) struct MetadataTargets {
    state: Arc<RwLock<MetadataTargetState>>,
}

impl MetadataTargets {
    /// Create metadata targets from configured metadata groups.
    pub(crate) fn new(groups: Vec<MetadataGroupTargets>) -> ClientResult<Self> {
        if groups.is_empty() {
            return Err(ClientError::invalid_argument(
                "MetadataTargets requires at least one metadata group".to_string(),
            ));
        }
        if let Some(group) = groups.iter().find(|group| group.endpoints.is_empty()) {
            return Err(ClientError::invalid_argument(format!(
                "MetadataTargets group {} requires at least one endpoint",
                group.group_name
            )));
        }
        Ok(Self {
            state: Arc::new(RwLock::new(MetadataTargetState {
                groups,
                leader_cache: HashMap::new(),
                route_cache: HashMap::new(),
                route_cache_order: VecDeque::new(),
                mount_epoch_cache: HashMap::new(),
                mount_epoch_cache_order: VecDeque::new(),
                route_epoch_cache: HashMap::new(),
                route_epoch_cache_order: VecDeque::new(),
                watermarks: HashMap::new(),
            })),
        })
    }

    /// Builds the single supported root route from sealed client configuration.
    pub(crate) fn from_config(config: &ClientConfig) -> ClientResult<Self> {
        let root = GroupName::parse("root")
            .map_err(|error| ClientError::invalid_configuration(format!("invalid built-in root group: {error}")))?;
        Self::new(vec![MetadataGroupTargets {
            group_name: root,
            endpoints: config.metadata_endpoints().to_vec(),
        }])
    }

    /// Choose the owner group for a path, using owner cache before bootstrap config.
    pub(crate) fn group_for_path(&self, path: &str) -> ClientResult<GroupName> {
        let state = self.state.read();
        if let Some(group_name) = state.route_cache.get(path) {
            return Ok(group_name.clone());
        }
        state
            .groups
            .first()
            .map(|group| group.group_name.clone())
            .ok_or_else(|| ClientError::invalid_configuration("metadata group configuration is empty".to_string()))
    }

    /// Choose the owner group for an operation.
    pub(crate) fn group_for_operation(&self, operation: &OperationContext) -> ClientResult<GroupName> {
        if let Some(path) = operation.original_target_path() {
            self.group_for_path(path)
        } else {
            self.group_for_path("")
        }
    }

    /// Return cached mount epoch for a path or its best matching mount prefix.
    pub(crate) fn cached_mount_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().mount_epoch_cache, path)
    }

    /// Return cached route epoch for a path or its best matching mount prefix.
    pub(crate) fn cached_route_epoch(&self, path: &str) -> Option<u64> {
        cached_epoch_for_path(&self.state.read().route_epoch_cache, path)
    }

    /// Select endpoint for the next attempt.
    pub(crate) fn endpoint_for_group(&self, group_name: &GroupName, attempt: u32) -> ClientResult<String> {
        let state = self.state.read();
        if let Some(endpoint) = state.leader_cache.get(group_name) {
            return Ok(endpoint.clone());
        }
        state
            .groups
            .iter()
            .find(|group| &group.group_name == group_name)
            .map(|group| {
                let index = attempt as usize % group.endpoints.len();
                group.endpoints[index].clone()
            })
            .ok_or_else(|| {
                ClientError::invalid_configuration(format!("metadata group {} is not configured", group_name))
            })
    }

    /// Clear a cached leader when transport failed against that exact endpoint.
    pub(crate) fn record_transport_failure(&self, group_name: &GroupName, endpoint: &str) {
        let mut state = self.state.write();
        if state
            .leader_cache
            .get(group_name)
            .is_some_and(|cached| cached == endpoint)
        {
            state.leader_cache.remove(group_name);
        }
    }

    /// Record a structured refresh decision and update correctness caches.
    pub(crate) fn record_refresh(
        &self,
        operation: &OperationContext,
        kind: ErrorKind,
        hint: &RefreshHint,
    ) -> ClientResult<()> {
        let mut state = self.state.write();
        match kind {
            ErrorKind::Metadata(MetadataErrorKind::NotLeader) => {
                if let (Some(group_name), Some(endpoint)) = (hint.group_name.as_ref(), hint.leader_endpoint.as_ref()) {
                    state.leader_cache.insert(group_name.clone(), endpoint.clone());
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch | MetadataErrorKind::GroupMismatch) => {
                let Some(group_name) = hint.group_name.as_ref() else {
                    return Err(ClientError::metadata(
                        "owner group mismatch refresh missing group_name hint".to_string(),
                    ));
                };
                if let Some(path) = operation.original_target_path() {
                    state.insert_route(path.to_string(), group_name.clone());
                }
                if let Some(endpoint) = hint.leader_endpoint.as_ref() {
                    state.leader_cache.insert(group_name.clone(), endpoint.clone());
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch) => {
                if let Some(mount_epoch) = hint.mount_epoch {
                    state.record_mount_epoch_hint(
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        mount_epoch,
                    );
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch) => {
                if let Some(route_epoch) = hint.route_epoch {
                    state.record_route_epoch_hint(
                        operation.original_target_path(),
                        hint.mount_prefix.as_deref(),
                        route_epoch,
                    );
                }
            }
            ErrorKind::Metadata(MetadataErrorKind::StaleState) | ErrorKind::Worker(WorkerErrorKind::RunMismatch) => {}
            _ => {
                return Err(ClientError::metadata(format!(
                    "unsupported metadata refresh error kind: {kind:?}"
                )))
            }
        }
        Ok(())
    }

    /// Atomically applies one validated successful response without allowing
    /// concurrent or late responses to move any authority value backwards.
    pub(crate) fn apply_authority_update(
        &self,
        operation: &OperationContext,
        update: MetadataAuthorityUpdate,
    ) -> ClientResult<()> {
        if update
            .state
            .iter()
            .any(|watermark| watermark.group_name != update.group_name)
        {
            return Err(ClientError::metadata(
                "metadata authority update contains a watermark for another group".to_string(),
            ));
        }
        let operation_path = operation.original_target_path();
        if operation_path.is_none() && (update.mount_epoch.is_some() || update.route_epoch.is_some()) {
            return Err(ClientError::metadata(
                "metadata authority epochs require an operation path".to_string(),
            ));
        }

        let mut state = self.state.write();
        for watermark in update.state {
            update_watermark_if_ahead(&mut state.watermarks, watermark);
        }
        if let Some(mount_epoch) = update.mount_epoch {
            state.record_mount_epoch_hint(operation_path, None, mount_epoch);
        }
        if let Some(route_epoch) = update.route_epoch {
            state.record_route_epoch_hint(operation_path, None, route_epoch);
        }
        Ok(())
    }

    /// Add cached freshness hints to an attempt context without inventing defaults.
    pub(crate) fn enrich_attempt_context(
        &self,
        operation: &OperationContext,
        mut ctx: AttemptContext,
    ) -> AttemptContext {
        let Some(path) = operation.original_target_path() else {
            return ctx;
        };
        if let Some(mount_epoch) = self.cached_mount_epoch(path) {
            ctx = ctx.with_mount_epoch(mount_epoch);
        }
        if let Some(route_epoch) = self.cached_route_epoch(path) {
            ctx = ctx.with_route_epoch(route_epoch);
        }
        ctx
    }

    /// Return cached watermark as proto for a group.
    pub(crate) fn state_watermark_proto(
        &self,
        group_name: &GroupName,
    ) -> Option<beryl_proto::common::GroupStateWatermarkProto> {
        self.state
            .read()
            .watermarks
            .get(group_name)
            .map(beryl_proto::common::GroupStateWatermarkProto::from)
    }
}

fn record_epoch_hint(
    cache: &mut HashMap<String, u64>,
    order: &mut VecDeque<String>,
    operation_path: Option<&str>,
    mount_prefix: Option<&str>,
    epoch: u64,
) {
    if let Some(path) = operation_path {
        insert_bounded_epoch(cache, order, path.to_string(), epoch);
    }
    if let Some(prefix) = mount_prefix {
        insert_bounded_epoch(cache, order, prefix.to_string(), epoch);
    }
}

/// Inserts one path-scoped epoch while preserving the highest observed value.
fn insert_bounded_epoch(cache: &mut HashMap<String, u64>, order: &mut VecDeque<String>, key: String, epoch: u64) {
    if let Some(existing) = cache.get_mut(&key) {
        *existing = (*existing).max(epoch);
        return;
    }
    insert_bounded(cache, order, key, epoch);
}

/// Advances one group watermark and ignores equal or older observations.
fn update_watermark_if_ahead(
    watermarks: &mut HashMap<GroupName, GroupStateWatermark>,
    new_watermark: GroupStateWatermark,
) {
    match watermarks.get_mut(&new_watermark.group_name) {
        Some(existing) if new_watermark.state_id > existing.state_id => *existing = new_watermark,
        None => {
            watermarks.insert(new_watermark.group_name.clone(), new_watermark);
        }
        Some(_) => {}
    }
}

fn insert_bounded<K, V>(cache: &mut HashMap<K, V>, order: &mut VecDeque<K>, key: K, value: V)
where
    K: Clone + Eq + Hash,
{
    if let Some(existing) = cache.get_mut(&key) {
        *existing = value;
        return;
    }
    while cache.len() >= METADATA_TARGET_CACHE_LIMIT {
        let Some(evicted) = order.pop_front() else {
            break;
        };
        if cache.remove(&evicted).is_some() {
            break;
        }
    }
    cache.insert(key.clone(), value);
    order.push_back(key);
}

fn cached_epoch_for_path(cache: &HashMap<String, u64>, path: &str) -> Option<u64> {
    cache.get(path).copied().or_else(|| {
        cache
            .iter()
            .filter(|(prefix, _)| path_matches_prefix(path, prefix))
            .max_by_key(|(prefix, _)| prefix.len())
            .map(|(_, epoch)| *epoch)
    })
}

fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    if prefix == "/" {
        return path.starts_with('/');
    }
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|remaining| remaining.starts_with('/'))
}

impl Default for MetadataTargets {
    fn default() -> Self {
        Self::new(vec![MetadataGroupTargets {
            group_name: GroupName::parse("root").expect("default group name is valid"),
            endpoints: vec!["127.0.0.1:18080".to_string()],
        }])
        .expect("default metadata group must be valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Operation, OperationContext, OperationDeadline};
    use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
    use beryl_proto::common::{GroupStateWatermarkProto, RaftLogIdProto};
    use beryl_types::{ClientId, GroupName};

    fn manager() -> MetadataTargets {
        MetadataTargets::new(vec![MetadataGroupTargets {
            group_name: group_name("root"),
            endpoints: vec!["http://127.0.0.1:18080".to_string()],
        }])
        .expect("refresh manager")
    }

    fn path_operation() -> OperationContext {
        OperationContext::new_named(
            ClientId::new(7),
            "test-client",
            Operation::OpenFile,
            Some("/alpha/file".to_string()),
            OperationDeadline::new(1_000),
        )
        .expect("operation context")
    }

    fn metadata_attempt(operation: &OperationContext) -> AttemptContext {
        AttemptContext::for_metadata(operation, group_name("root"), 0).expect("metadata attempt")
    }

    #[test]
    fn not_leader_hint_updates_leader_cache() {
        let manager = manager();
        let op = path_operation();

        manager
            .record_refresh(
                &op,
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                &RefreshHint {
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");

        assert_eq!(
            manager
                .endpoint_for_group(&group_name("root"), 0)
                .expect("leader endpoint"),
            "http://127.0.0.1:18081"
        );
    }

    #[test]
    fn transport_failure_clears_failed_cached_leader() {
        let targets = MetadataTargets::new(vec![MetadataGroupTargets {
            group_name: group_name("root"),
            endpoints: vec!["a".to_string(), "b".to_string()],
        }])
        .expect("metadata targets");
        let op = path_operation();

        targets
            .record_refresh(
                &op,
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                &RefreshHint {
                    group_name: Some(group_name("root")),
                    leader_endpoint: Some("leader".to_string()),
                    ..RefreshHint::default()
                },
            )
            .expect("refresh recorded");
        assert_eq!(targets.endpoint_for_group(&group_name("root"), 0).unwrap(), "leader");

        targets.record_transport_failure(&group_name("root"), "leader");

        assert_eq!(targets.endpoint_for_group(&group_name("root"), 1).unwrap(), "b");
    }

    #[test]
    fn authority_updates_and_refresh_hints_never_regress() {
        let manager = manager();
        let op = path_operation();

        manager
            .apply_authority_update(
                &op,
                MetadataAuthorityUpdate {
                    group_name: group_name("root"),
                    state: vec![GroupStateWatermark::try_from(watermark_proto("root", 10)).unwrap()],
                    mount_epoch: Some(31),
                    route_epoch: Some(41),
                },
            )
            .expect("new authority update");
        for (kind, hint) in [
            (
                MetadataErrorKind::MountEpochMismatch,
                RefreshHint {
                    mount_epoch: Some(30),
                    mount_prefix: Some("/alpha".to_string()),
                    ..RefreshHint::default()
                },
            ),
            (
                MetadataErrorKind::RouteEpochMismatch,
                RefreshHint {
                    route_epoch: Some(40),
                    mount_prefix: Some("/alpha".to_string()),
                    ..RefreshHint::default()
                },
            ),
        ] {
            manager
                .record_refresh(&op, ErrorKind::Metadata(kind), &hint)
                .expect("older refresh hint");
        }
        manager
            .apply_authority_update(
                &op,
                MetadataAuthorityUpdate {
                    group_name: group_name("root"),
                    state: vec![GroupStateWatermark::try_from(watermark_proto("root", 8)).unwrap()],
                    mount_epoch: Some(29),
                    route_epoch: Some(39),
                },
            )
            .expect("older authority update");

        let header = manager
            .enrich_attempt_context(&op, metadata_attempt(&op))
            .with_state(manager.state_watermark_proto(&group_name("root")).into_iter().collect())
            .metadata_header()
            .expect("metadata header");
        assert_eq!(header.mount_epoch, Some(31));
        assert_eq!(header.route_epoch, Some(41));
        assert_eq!(header.state[0].state_id.as_ref().map(|state| state.index), Some(10));
    }

    fn watermark_proto(group_name: &str, index: u64) -> GroupStateWatermarkProto {
        GroupStateWatermarkProto {
            group_name: group_name.to_string(),
            state_id: Some(RaftLogIdProto {
                term: 1,
                leader_node_id: 1,
                index,
            }),
        }
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }
}
