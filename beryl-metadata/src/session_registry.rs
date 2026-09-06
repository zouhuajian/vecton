// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Leader-local write-session admission and lifecycle state.
//!
//! The registry owns write mutual exclusion, capacity, expiration, and
//! continuation state for the current Metadata process. The persisted inode
//! lease epoch remains the durable fencing authority across replay and restart.

use crate::config::MetadataConfig;
use crate::observe;
use beryl_types::ids::{InodeId, MountId};
use beryl_types::{
    BlockId, BlockShape, CallId, ClientId, ContentGeneration, FileLayout, LeaseEpoch, LocatedBlock, WriteMode,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum number of expired entries retired by one cleanup invocation.
pub(crate) const MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL: usize = 64;

/// Leader-local continuation state for one admitted write.
///
/// This record is not durable authority. The persisted fencing epoch fences
/// writers across replay and leader restart.
#[derive(Clone, Debug)]
pub struct WriteSession {
    /// Inode ID being written.
    pub inode_id: InodeId,
    /// Mount ID.
    pub mount_id: MountId,
    /// Lease epoch (for fencing validation).
    pub lease_epoch: LeaseEpoch,
    /// Base file size at open time (for append-only validation).
    pub base_size: u64,
    /// Last durable content generation observed by this session.
    pub generation: ContentGeneration,
    /// Session intent: replace visible contents or append after the visible end.
    pub mode: WriteMode,
    /// Client that owns the OpenWrite call.
    pub open_client_id: ClientId,
    /// Layout returned by OpenWrite.
    pub layout: FileLayout,
    /// Exact lease expiry returned by OpenWrite.
    pub expires_at_ms: u64,
    /// Bounded mount-root-to-file chain captured while namespace topology was stable.
    ancestor_inode_ids: Vec<InodeId>,
    /// Targets already issued to the client through AllocateBlock.
    pub issued_targets: Vec<LocatedBlock>,
    /// Logical AllocateBlock steps issued for predecessor-based replay.
    issued_steps: HashMap<Option<BlockId>, usize>,
    /// The one logical AllocateBlock step allowed to cross Raft allocation at a time.
    pending_allocate_block: Option<PendingAllocateBlock>,
    /// Exact local publication currently freezing the issued-target sequence.
    active_publication: Option<WritePublicationId>,
    /// Pins GC, namespace exclusion, and capacity while a submitted publication awaits Raft.
    publication_submitted: bool,
    /// Immutable identity and response retained for exact CreateFile replay.
    create_replay: Option<ActiveCreateReplay>,
}

/// Small active-session snapshot used before AllocateBlock reserves target state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteSessionIdentity {
    pub(crate) mount_id: MountId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) open_client_id: ClientId,
}

/// Validated inputs needed before one `OpenWrite` crosses its Raft proposal.
#[derive(Clone)]
pub(crate) struct BeginSessionInput {
    /// Normalized path used to exclude an unbound CreateFile opening.
    pub normalized_path: String,
    pub mount_id: MountId,
    pub inode_id: InodeId,
    pub current_lease_epoch: LeaseEpoch,
    pub mode: WriteMode,
    pub open_client_id: ClientId,
    pub layout: FileLayout,
    pub ancestor_inode_ids: Vec<InodeId>,
}

/// Stable identity of one replayable atomic CreateFile operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct CreateFileOperationId {
    pub(crate) client_id: ClientId,
    pub(crate) call_id: CallId,
}

/// Inputs reserved before an atomic CreateFile may cross the Raft boundary.
pub(crate) struct BeginCreateSessionInput {
    pub(crate) operation_id: CreateFileOperationId,
    pub(crate) request_deadline_ms: u64,
    pub(crate) normalized_path: String,
    pub(crate) mount_id: MountId,
    pub(crate) expected_mount_epoch: u64,
    pub(crate) mount_root_inode_id: InodeId,
    pub(crate) open_client_id: ClientId,
    /// Mount-root-to-parent chain captured while namespace topology is stable.
    pub(crate) parent_ancestor_inode_ids: Vec<InodeId>,
}

/// Process-local identity for one exact `OpenWrite` attempt.
///
/// The durable fencing epoch can be proposed by more than one attempt before
/// either proposal applies. This identity prevents a cancelled stale attempt
/// from removing a replacement `Opening` entry that has the same candidate
/// epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct WriteOpeningId(u64);

/// Leader-local capacity reserved before CreateFile has a durable inode ID.
#[derive(Clone, Debug)]
struct CreateOpeningSession {
    opening_id: WriteOpeningId,
    operation_id: CreateFileOperationId,
    request_deadline_ms: u64,
    normalized_path: String,
    mount_id: MountId,
    expected_mount_epoch: u64,
    mount_root_inode_id: InodeId,
    open_client_id: ClientId,
    expires_at_ms: u64,
    parent_ancestor_inode_ids: Vec<InodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ActiveCreateReplay {
    operation_id: CreateFileOperationId,
    request_deadline_ms: u64,
    normalized_path: String,
    mount_id: MountId,
    expected_mount_epoch: u64,
    mount_root_inode_id: InodeId,
    response: CreateSessionReplay,
}

/// Process-local identity for one exact SyncWrite or CommitFile attempt.
///
/// The identity prevents a cancelled stale owner from clearing a later
/// publication on a replacement session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WritePublicationId(u64);

/// Leader-local state held while `OpenWrite` waits for durable epoch fencing.
#[derive(Clone, Debug)]
struct OpeningSession {
    opening_id: WriteOpeningId,
    inode_id: InodeId,
    mount_id: MountId,
    proposed_lease_epoch: LeaseEpoch,
    mode: WriteMode,
    open_client_id: ClientId,
    layout: FileLayout,
    expires_at_ms: u64,
    ancestor_inode_ids: Vec<InodeId>,
}

/// Complete leader-local lifecycle state for one inode.
#[derive(Clone, Debug)]
enum WriteSessionEntry {
    /// `OpenWrite` owns capacity and inode exclusion while Raft fencing is pending.
    Opening(OpeningSession),
    /// The durable epoch was acquired and the session may continue write operations.
    Active(Box<WriteSession>),
}

impl WriteSessionEntry {
    fn client_id(&self) -> ClientId {
        match self {
            Self::Opening(opening) => opening.open_client_id,
            Self::Active(session) => session.open_client_id,
        }
    }

    fn expires_at_ms(&self) -> u64 {
        match self {
            Self::Opening(opening) => opening.expires_at_ms,
            Self::Active(session) => session.expires_at_ms,
        }
    }

    /// Submitted publications outlive their lease deadline until durable resolution.
    fn retirement_at_ms(&self) -> u64 {
        match self {
            Self::Active(session) if session.publication_submitted => u64::MAX,
            _ => self.expires_at_ms(),
        }
    }

    fn ancestor_inode_ids(&self) -> &[InodeId] {
        match self {
            Self::Opening(opening) => &opening.ancestor_inode_ids,
            Self::Active(session) => &session.ancestor_inode_ids,
        }
    }

    fn is_opening(&self) -> bool {
        matches!(self, Self::Opening(_))
    }
}

/// One predecessor-addressed AllocateBlock step reserved before Raft allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingAllocateBlock {
    previous_block_id: Option<BlockId>,
}

/// In-memory, leader-local registry of write sessions and capacity indexes.
///
/// One lock protects the primary session map and every derived inode,
/// ancestor, and expiry index so readers never observe a partially updated
/// session lifecycle.
pub struct SessionRegistry {
    state: RwLock<SessionRegistryState>,
    max_sessions: usize,
    max_sessions_per_client: usize,
    max_write_targets: usize,
    max_write_targets_per_session: usize,
    /// Fixed lifetime assigned at opening and extended by successful renewal.
    session_ttl_ms: u64,
}

/// Capacity boundary that rejected one write-session reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteSessionLimit {
    /// Process-wide opening plus active session capacity.
    Global,
    /// Opening plus active capacity attributed to one client ID.
    PerClient,
}

/// Capacity boundary that rejected one pending write target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteTargetLimit {
    /// Pending plus issued targets across the Metadata process.
    Global,
    /// Pending plus issued targets owned by one write session.
    PerSession,
}

impl WriteTargetLimit {
    /// Stable low-cardinality label used by capacity metrics and logs.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::PerSession => "per_session",
        }
    }
}

/// Exact write-target limit reached before Raft block allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteTargetLimitExceeded {
    pub(crate) limit: WriteTargetLimit,
    pub(crate) maximum: usize,
}

/// Outcome of beginning one predecessor-addressed AllocateBlock step.
pub(crate) enum BeginAllocateBlock<'a> {
    /// The logical step was already issued and can be replayed without capacity.
    Replay(LocatedBlock),
    /// New capacity is reserved and must be completed or released before return.
    Reserved(WriteTargetReservation<'a>),
}

/// Exact failure returned before an AllocateBlock step may allocate through Raft.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BeginAllocateBlockError {
    /// The active session no longer exists or the presented epoch is stale.
    Session(String),
    /// The registry's compact replay index no longer resolves to its target.
    Internal(String),
    /// The predecessor is invalid for the active session.
    InvalidArgument(String),
    /// An identical logical step is already allocating and should be retried.
    Pending,
    /// SyncWrite or CommitFile is freezing the issued-target sequence.
    PublicationInProgress,
    /// Leader-local target capacity is exhausted.
    LimitExceeded(WriteTargetLimitExceeded),
}

/// Exact failure returned while converting a pending AllocateBlock into an issued target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompleteWriteTargetError {
    /// Expiry, abort, or replacement removed the reservation's active session.
    NotCurrent,
    /// The completed target no longer matches the reserved session state.
    InvalidTarget(String),
}

/// Exact leader-local target capacity held across Raft allocation and placement.
///
/// Dropping this owner releases only the matching pending step. Completing it
/// atomically converts the pending slot into one issued target without changing
/// the total occupied target count.
#[must_use = "dropping the reservation releases pending write-target capacity"]
pub(crate) struct WriteTargetReservation<'a> {
    registry: &'a SessionRegistry,
    inode_id: InodeId,
    lease_epoch: LeaseEpoch,
    pending: PendingAllocateBlock,
    layout: FileLayout,
    open_client_id: ClientId,
    file_offset: u64,
    armed: bool,
}

/// Exact leader-local ownership of a stable issued-target sequence.
///
/// While this owner is alive, new AllocateBlock steps are rejected before block
/// allocation. A submitted publication transfers this owner to a completion task so
/// RPC cancellation cannot expose unpublished blocks to GC. Expiry stays pinned
/// until Raft finishes; only the matching publication can retire its session.
#[must_use = "a submitted publication must survive until durable resolution"]
pub(crate) struct WritePublication {
    registry: Arc<SessionRegistry>,
    session: WriteSession,
    publication_id: WritePublicationId,
    armed: bool,
    submitted: bool,
}

/// Exact reason why SyncWrite or CommitFile cannot freeze a session snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BeginWritePublicationError {
    /// The active session no longer exists or the presented epoch is stale.
    Session(String),
    /// One AllocateBlock step is already crossing allocation or placement.
    AllocateBlockPending,
    /// Another SyncWrite or CommitFile already owns the session boundary.
    PublicationInProgress,
    /// The process-local publication identity cannot advance without reuse.
    PublicationIdExhausted,
}

impl WritePublication {
    /// Return the exact session snapshot frozen for this publication.
    pub(crate) fn session(&self) -> &WriteSession {
        &self.session
    }

    /// Refresh the frozen session after asynchronous Worker readiness checks.
    pub(crate) fn revalidate(&self) -> Result<WriteSession, String> {
        self.registry.revalidate_publication(
            self.session.inode_id,
            self.session.lease_epoch,
            self.publication_id,
            current_time_ms(),
        )
    }

    /// Install the successful SyncWrite generation and release the boundary.
    pub(crate) fn complete_sync(mut self, generation: ContentGeneration, file_size: u64) -> Result<(), String> {
        let result = self.registry.complete_sync_publication(
            self.session.inode_id,
            self.session.lease_epoch,
            self.publication_id,
            generation,
            file_size,
            current_time_ms(),
        );
        if result.is_ok() {
            self.armed = false;
        }
        result
    }

    /// Pin the exact live session before transferring ownership to the publication task.
    pub(crate) fn mark_submitted(&mut self) -> Result<(), String> {
        if self.submitted {
            return Ok(());
        }
        if self.session.lease_epoch.checked_next().is_none() {
            return Err("file publication write lease epoch exhausted".into());
        }
        let mut state = self.registry.state.write();
        let session = SessionRegistry::active_session_mut(&mut state, self.session.inode_id)?;
        if session.lease_epoch != self.session.lease_epoch
            || session.active_publication != Some(self.publication_id)
            || session.expires_at_ms <= current_time_ms()
        {
            return Err("file publication session changed or expired before submission".into());
        }
        let previous = session.expires_at_ms;
        let ancestors = session.ancestor_inode_ids.clone();
        session.publication_submitted = true;
        SessionRegistry::move_expiry_indexes(&mut state, self.session.inode_id, &ancestors, previous, u64::MAX);
        self.submitted = true;
        Ok(())
    }

    /// Retire only this publication's session; a missing/replaced one is already retired.
    pub(crate) fn complete_commit(mut self) {
        self.registry
            .complete_commit_publication(self.session.inode_id, self.session.lease_epoch, self.publication_id);
        self.armed = false;
    }
}

impl WriteTargetReservation<'_> {
    /// Return the persisted layout that the allocated block must use.
    pub(crate) fn layout(&self) -> FileLayout {
        self.layout
    }

    /// Return the client identity embedded in the target fencing token.
    pub(crate) fn open_client_id(&self) -> ClientId {
        self.open_client_id
    }

    /// Return the file offset reserved for this logical AllocateBlock step.
    pub(crate) fn file_offset(&self) -> u64 {
        self.file_offset
    }

    /// Atomically install a validated target in place of this pending slot.
    pub(crate) fn complete(mut self, target: LocatedBlock) -> Result<LocatedBlock, CompleteWriteTargetError> {
        let result = self.registry.complete_write_target(
            self.inode_id,
            self.lease_epoch,
            &self.pending,
            target,
            current_time_ms(),
        );
        self.armed = false;
        result
    }
}

impl WriteSessionLimit {
    /// Stable low-cardinality label used by capacity metrics and logs.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::PerClient => "per_client",
        }
    }
}

/// Exact reason why a write session could not reserve leader-local capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteSessionLimitExceeded {
    pub(crate) limit: WriteSessionLimit,
    pub(crate) maximum: usize,
}

/// Exact leader-local `Opening` ownership held across the Raft proposal.
///
/// Dropping the owner after cancellation or an early error removes only the
/// matching opening identity and releases all derived accounting atomically.
#[must_use = "dropping the opening releases its leader-local write session"]
pub(crate) struct WriteOpening<'a> {
    registry: &'a SessionRegistry,
    inode_id: InodeId,
    opening_id: WriteOpeningId,
    proposed_lease_epoch: LeaseEpoch,
    armed: bool,
}

/// Exact unbound CreateFile reservation held across its Raft proposal.
#[must_use = "dropping the opening releases its leader-local create session"]
pub(crate) struct CreateOpening<'a> {
    registry: &'a SessionRegistry,
    operation_id: CreateFileOperationId,
    opening_id: WriteOpeningId,
    expires_at_ms: u64,
    armed: bool,
}

/// Outcome of reserving leader-local ownership for atomic CreateFile.
pub(crate) enum BeginCreateSession<'a> {
    /// The same operation already owns a non-expired active session.
    Replay(CreateSessionReplay),
    /// New capacity is reserved until Raft creates or replays the file.
    Reserved(CreateOpening<'a>),
}

/// Minimal active-session state returned by a leader-local CreateFile replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CreateSessionReplay {
    pub(crate) inode_id: InodeId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) layout: FileLayout,
    pub(crate) expires_at_ms: u64,
    pub(crate) generation: ContentGeneration,
}

/// Exact reason an atomic CreateFile session cannot be reserved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BeginCreateSessionError {
    /// The same operation is already waiting for its Raft result.
    Pending,
    /// The same operation identity was reused for another request authority.
    IdentityMismatch,
    /// Another CreateFile operation is still reserving the same path.
    PathBusy,
    /// Admission capacity was exhausted before any durable mutation.
    LimitExceeded(WriteSessionLimitExceeded),
    /// The process-local opening identity cannot advance without reuse.
    OpeningIdExhausted,
    /// The captured namespace parent path is empty, cyclic, or too deep.
    InvalidAncestorChain,
}

impl WriteOpening<'_> {
    /// Return the exact epoch that the matching Raft command must acquire.
    pub(crate) fn proposed_lease_epoch(&self) -> LeaseEpoch {
        self.proposed_lease_epoch
    }

    /// Atomically convert the matching, non-expired opening into an active session.
    pub(crate) fn activate(
        mut self,
        returned_lease_epoch: LeaseEpoch,
        file: &crate::inode::FileData,
        tail: Option<LocatedBlock>,
    ) -> Result<WriteSession, WriteOpeningError> {
        let result = self.registry.activate_opening(
            self.inode_id,
            self.opening_id,
            returned_lease_epoch,
            current_time_ms(),
            file,
            tail,
        );
        if result.is_ok() || matches!(&result, Err(WriteOpeningError::NotCurrent | WriteOpeningError::Expired)) {
            self.armed = false;
        }
        result
    }
}

impl CreateOpening<'_> {
    /// Return the fixed write-session expiry replicated with this create.
    pub(crate) fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }

    /// Bind the durable CreateFile result and activate its write session atomically.
    pub(crate) fn activate(
        mut self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        expires_at_ms: u64,
        layout: FileLayout,
        generation: ContentGeneration,
    ) -> Result<WriteSession, WriteOpeningError> {
        let result = self.registry.activate_create_opening(
            self.operation_id,
            self.opening_id,
            inode_id,
            lease_epoch,
            expires_at_ms,
            layout,
            generation,
            current_time_ms(),
        );
        if result.is_ok() {
            self.armed = false;
        }
        result
    }
}

/// Exact reason why an opening cannot become an active session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteOpeningError {
    /// Opening an existing tail requires one slot in the bounded target registry.
    TargetLimit,
    /// The opening expired before the Raft result could be installed.
    Expired,
    /// Cleanup or replacement removed the exact opening identity.
    NotCurrent,
    /// The Raft result did not match the proposed fencing epoch.
    LeaseEpochMismatch { expected: LeaseEpoch, got: LeaseEpoch },
}

/// Exact leader-local failure returned while beginning one write session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BeginSessionError {
    /// Another non-expired opening or active session owns the inode.
    Busy,
    /// Admission capacity was exhausted before any Raft mutation.
    LimitExceeded(WriteSessionLimitExceeded),
    /// The durable fencing epoch cannot advance.
    LeaseEpochExhausted,
    /// The process-local opening identity cannot advance without reuse.
    OpeningIdExhausted,
    /// The captured namespace path is empty, cyclic, too deep, or ends elsewhere.
    InvalidAncestorChain,
}

/// Exact failure returned by active-session validation or renewal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteSessionError {
    /// No active session exists for the inode.
    NotFound,
    /// The presented epoch does not identify the active session.
    LeaseEpochMismatch { expected: LeaseEpoch, got: LeaseEpoch },
    /// The presented client does not own the active session.
    OwnerMismatch,
    /// The active session has expired and was retired.
    Expired,
}

/// Primary write-session state and all indexes that must change atomically with it.
struct SessionRegistryState {
    /// At most one opening or active session exists for one inode.
    entries: HashMap<InodeId, WriteSessionEntry>,
    /// Create openings that own capacity before an inode identity exists.
    create_openings: HashMap<CreateFileOperationId, CreateOpeningSession>,
    /// Exact path exclusion held until an unbound create is activated or cancelled.
    create_openings_by_path: HashMap<(MountId, String), CreateFileOperationId>,
    /// Reverse identity used by the bounded expiry index.
    create_opening_operations: HashMap<WriteOpeningId, CreateFileOperationId>,
    /// Create openings ordered by expiry without requiring an inode identity.
    create_openings_by_expiry: BTreeSet<(u64, WriteOpeningId)>,
    /// Active CreateFile operations mapped to their bound inode sessions.
    active_create_operations: HashMap<CreateFileOperationId, InodeId>,
    /// Number of primary entries still waiting for durable fencing.
    opening_sessions: usize,
    /// Opening plus active sessions attributed to each client ID.
    occupied_sessions_by_client: HashMap<ClientId, usize>,
    /// Bounded activity state for ancestors of both opening and active writes.
    ancestor_activity: HashMap<InodeId, AncestorWriteActivity>,
    /// All primary entries ordered by expiry for bounded cleanup.
    entries_by_expiry: BTreeSet<(u64, InodeId)>,
    /// Next process-local opening identity; zero is never issued.
    next_opening_id: u64,
    /// Next process-local publication identity; zero is never issued.
    next_publication_id: u64,
    /// Pending plus issued targets retained across every active session.
    outstanding_write_targets: usize,
    /// Subset of outstanding targets currently crossing allocation or placement.
    pending_write_targets: usize,
}

impl Default for SessionRegistryState {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            create_openings: HashMap::new(),
            create_openings_by_path: HashMap::new(),
            create_opening_operations: HashMap::new(),
            create_openings_by_expiry: BTreeSet::new(),
            active_create_operations: HashMap::new(),
            opening_sessions: 0,
            occupied_sessions_by_client: HashMap::new(),
            ancestor_activity: HashMap::new(),
            entries_by_expiry: BTreeSet::new(),
            next_opening_id: 1,
            next_publication_id: 1,
            outstanding_write_targets: 0,
            pending_write_targets: 0,
        }
    }
}

/// Expiry multiset for every write session whose captured path contains one inode.
///
/// The ancestor entry exists exactly while this multiset is non-empty. Counts
/// distinguish sessions that share the same expiry timestamp.
struct AncestorWriteActivity {
    sessions_by_expiry: BTreeMap<u64, usize>,
}

impl SessionRegistry {
    /// Create an empty leader-local registry with fixed process limits.
    pub(crate) fn new(
        max_sessions: usize,
        max_sessions_per_client: usize,
        max_write_targets: usize,
        max_write_targets_per_session: usize,
        session_ttl_ms: u64,
    ) -> Self {
        assert!(max_sessions > 0, "global write-session limit must be positive");
        assert!(
            max_sessions_per_client > 0 && max_sessions_per_client <= max_sessions,
            "per-client write-session limit must be positive and not exceed the global limit"
        );
        assert!(max_write_targets > 0, "global write-target limit must be positive");
        assert!(
            max_write_targets_per_session > 0 && max_write_targets_per_session <= max_write_targets,
            "per-session write-target limit must be positive and not exceed the global limit"
        );
        observe::set_write_sessions(0, 0);
        observe::set_write_targets(0, 0);
        Self {
            state: RwLock::new(SessionRegistryState::default()),
            max_sessions,
            max_sessions_per_client,
            max_write_targets,
            max_write_targets_per_session,
            session_ttl_ms,
        }
    }

    /// Admit one exact `OpenWrite` attempt before it proposes a durable epoch.
    ///
    /// The opening immediately owns inode exclusion, ancestor activity, and
    /// global and per-client capacity. Dropping the returned owner rolls back
    /// only this exact process-local identity.
    pub(crate) fn begin_session(&self, input: BeginSessionInput) -> Result<WriteOpening<'_>, BeginSessionError> {
        self.begin_session_at(input, current_time_ms())
    }

    /// Reserve CreateFile session capacity before the operation enters Raft.
    ///
    /// A retry of an already active operation returns the same leader-local
    /// session. A concurrent retry of a pending operation is rejected without
    /// submitting a duplicate proposal.
    pub(crate) fn begin_create_session(
        &self,
        input: BeginCreateSessionInput,
    ) -> Result<BeginCreateSession<'_>, BeginCreateSessionError> {
        self.begin_create_session_at(input, current_time_ms())
    }

    fn begin_create_session_at(
        &self,
        input: BeginCreateSessionInput,
        now_ms: u64,
    ) -> Result<BeginCreateSession<'_>, BeginCreateSessionError> {
        if input.operation_id.client_id != input.open_client_id {
            return Err(BeginCreateSessionError::IdentityMismatch);
        }
        Self::validate_parent_ancestor_chain(&input.parent_ancestor_inode_ids)
            .map_err(|_| BeginCreateSessionError::InvalidAncestorChain)?;

        let mut state = self.state.write();
        Self::retire_expired_create_opening_for_operation(&mut state, input.operation_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        if let Some(inode_id) = state.active_create_operations.get(&input.operation_id).copied() {
            let session = match state.entries.get(&inode_id) {
                Some(WriteSessionEntry::Active(session))
                    if session.create_replay.as_ref().map(|replay| replay.operation_id) == Some(input.operation_id) =>
                {
                    session
                }
                _ => panic!("active CreateFile operation index must identify its exact session"),
            };
            let replay = session
                .create_replay
                .as_ref()
                .expect("active CreateFile operation must retain its replay identity");
            if replay.request_deadline_ms != input.request_deadline_ms
                || replay.normalized_path != input.normalized_path
                || replay.mount_id != input.mount_id
                || replay.expected_mount_epoch != input.expected_mount_epoch
                || replay.mount_root_inode_id != input.mount_root_inode_id
            {
                return Err(BeginCreateSessionError::IdentityMismatch);
            }
            return Ok(BeginCreateSession::Replay(replay.response));
        }
        if let Some(opening) = state.create_openings.get(&input.operation_id) {
            return if opening.request_deadline_ms == input.request_deadline_ms
                && opening.normalized_path == input.normalized_path
                && opening.mount_id == input.mount_id
                && opening.expected_mount_epoch == input.expected_mount_epoch
                && opening.mount_root_inode_id == input.mount_root_inode_id
            {
                Err(BeginCreateSessionError::Pending)
            } else {
                Err(BeginCreateSessionError::IdentityMismatch)
            };
        }
        if state
            .create_openings_by_path
            .contains_key(&(input.mount_id, input.normalized_path.clone()))
        {
            return Err(BeginCreateSessionError::PathBusy);
        }
        if state.entries.len() + state.create_openings.len() >= self.max_sessions {
            observe::record_write_session_rejected(WriteSessionLimit::Global.label());
            return Err(BeginCreateSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::Global,
                maximum: self.max_sessions,
            }));
        }
        let client_occupied = state
            .occupied_sessions_by_client
            .get(&input.open_client_id)
            .copied()
            .unwrap_or_default();
        if client_occupied >= self.max_sessions_per_client {
            observe::record_write_session_rejected(WriteSessionLimit::PerClient.label());
            return Err(BeginCreateSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::PerClient,
                maximum: self.max_sessions_per_client,
            }));
        }

        let opening_id = WriteOpeningId(state.next_opening_id);
        state.next_opening_id = state
            .next_opening_id
            .checked_add(1)
            .ok_or(BeginCreateSessionError::OpeningIdExhausted)?;
        let operation_id = input.operation_id;
        let expires_at_ms = now_ms.saturating_add(self.session_ttl_ms);
        Self::insert_create_opening(
            &mut state,
            CreateOpeningSession {
                opening_id,
                operation_id,
                request_deadline_ms: input.request_deadline_ms,
                normalized_path: input.normalized_path,
                mount_id: input.mount_id,
                expected_mount_epoch: input.expected_mount_epoch,
                mount_root_inode_id: input.mount_root_inode_id,
                open_client_id: input.open_client_id,
                expires_at_ms,
                parent_ancestor_inode_ids: input.parent_ancestor_inode_ids,
            },
        );
        Ok(BeginCreateSession::Reserved(CreateOpening {
            registry: self,
            operation_id,
            opening_id,
            expires_at_ms,
            armed: true,
        }))
    }

    fn begin_session_at(&self, input: BeginSessionInput, now_ms: u64) -> Result<WriteOpening<'_>, BeginSessionError> {
        Self::validate_ancestor_chain(input.inode_id, &input.ancestor_inode_ids)
            .map_err(|_| BeginSessionError::InvalidAncestorChain)?;

        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, input.inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        if state.entries.contains_key(&input.inode_id)
            || state
                .create_openings_by_path
                .contains_key(&(input.mount_id, input.normalized_path.clone()))
        {
            return Err(BeginSessionError::Busy);
        }
        if state.entries.len() + state.create_openings.len() >= self.max_sessions {
            observe::record_write_session_rejected(WriteSessionLimit::Global.label());
            return Err(BeginSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::Global,
                maximum: self.max_sessions,
            }));
        }
        let client_occupied = state
            .occupied_sessions_by_client
            .get(&input.open_client_id)
            .copied()
            .unwrap_or_default();
        if client_occupied >= self.max_sessions_per_client {
            observe::record_write_session_rejected(WriteSessionLimit::PerClient.label());
            return Err(BeginSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::PerClient,
                maximum: self.max_sessions_per_client,
            }));
        }

        let proposed_lease_epoch = input
            .current_lease_epoch
            .checked_next()
            .ok_or(BeginSessionError::LeaseEpochExhausted)?;
        let opening_id = WriteOpeningId(state.next_opening_id);
        state.next_opening_id = state
            .next_opening_id
            .checked_add(1)
            .ok_or(BeginSessionError::OpeningIdExhausted)?;
        let expires_at_ms = now_ms.saturating_add(self.session_ttl_ms);
        let inode_id = input.inode_id;
        let opening = OpeningSession {
            opening_id,
            inode_id,
            mount_id: input.mount_id,
            proposed_lease_epoch,
            mode: input.mode,
            open_client_id: input.open_client_id,
            layout: input.layout,
            expires_at_ms,
            ancestor_inode_ids: input.ancestor_inode_ids,
        };
        let entry = WriteSessionEntry::Opening(opening);
        Self::insert_entry(&mut state, entry);

        Ok(WriteOpening {
            registry: self,
            inode_id,
            opening_id,
            proposed_lease_epoch,
            armed: true,
        })
    }

    /// Convert only the matching, non-expired opening into an active session.
    fn activate_opening(
        &self,
        inode_id: InodeId,
        opening_id: WriteOpeningId,
        returned_lease_epoch: LeaseEpoch,
        now_ms: u64,
        file: &crate::inode::FileData,
        tail: Option<LocatedBlock>,
    ) -> Result<WriteSession, WriteOpeningError> {
        let mut state = self.state.write();
        let opening = match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Opening(opening)) if opening.opening_id == opening_id => opening.clone(),
            _ => return Err(WriteOpeningError::NotCurrent),
        };
        if opening.expires_at_ms <= now_ms {
            Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
            return Err(WriteOpeningError::Expired);
        }
        if opening.proposed_lease_epoch != returned_lease_epoch {
            return Err(WriteOpeningError::LeaseEpochMismatch {
                expected: opening.proposed_lease_epoch,
                got: returned_lease_epoch,
            });
        }

        if file.lease_epoch != returned_lease_epoch || file.layout != opening.layout {
            return Err(WriteOpeningError::NotCurrent);
        }
        if tail.is_some() && state.outstanding_write_targets >= self.max_write_targets {
            return Err(WriteOpeningError::TargetLimit);
        }
        state.outstanding_write_targets += usize::from(tail.is_some());
        let session = WriteSession {
            inode_id: opening.inode_id,
            mount_id: opening.mount_id,
            lease_epoch: opening.proposed_lease_epoch,
            base_size: file.len,
            generation: file.generation,
            mode: opening.mode,
            open_client_id: opening.open_client_id,
            layout: opening.layout,
            expires_at_ms: opening.expires_at_ms,
            ancestor_inode_ids: opening.ancestor_inode_ids,
            issued_targets: tail.into_iter().collect(),
            issued_steps: HashMap::new(),
            pending_allocate_block: None,
            active_publication: None,
            publication_submitted: false,
            create_replay: None,
        };
        let previous = state
            .entries
            .insert(inode_id, WriteSessionEntry::Active(Box::new(session.clone())));
        assert!(
            matches!(previous, Some(WriteSessionEntry::Opening(current)) if current.opening_id == opening_id),
            "validated write opening must remain current under the registry lock"
        );
        state.opening_sessions = state
            .opening_sessions
            .checked_sub(1)
            .expect("activated write session must own one opening count");
        Self::record_session_gauges(&state);
        Ok(session)
    }

    /// Convert one exact unbound CreateFile reservation into an active inode session.
    #[allow(clippy::too_many_arguments)]
    fn activate_create_opening(
        &self,
        operation_id: CreateFileOperationId,
        opening_id: WriteOpeningId,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        expires_at_ms: u64,
        layout: FileLayout,
        generation: ContentGeneration,
        now_ms: u64,
    ) -> Result<WriteSession, WriteOpeningError> {
        let mut state = self.state.write();
        let opening = match state.create_openings.get(&operation_id) {
            Some(opening) if opening.opening_id == opening_id => opening.clone(),
            _ => return Err(WriteOpeningError::NotCurrent),
        };
        if opening.expires_at_ms <= now_ms || expires_at_ms <= now_ms {
            Self::remove_create_opening(&mut state, operation_id);
            observe::record_write_session_expired();
            return Err(WriteOpeningError::Expired);
        }
        if lease_epoch.as_raw() == 0 || state.entries.contains_key(&inode_id) {
            return Err(WriteOpeningError::NotCurrent);
        }
        let mut ancestor_inode_ids = opening.parent_ancestor_inode_ids.clone();
        ancestor_inode_ids.push(inode_id);
        Self::validate_ancestor_chain(inode_id, &ancestor_inode_ids).map_err(|_| WriteOpeningError::NotCurrent)?;

        let removed = Self::remove_create_opening(&mut state, operation_id)
            .expect("validated CreateFile opening must remain current under the registry lock");
        let response = CreateSessionReplay {
            inode_id,
            lease_epoch,
            layout,
            expires_at_ms,
            generation,
        };
        let session = WriteSession {
            inode_id,
            mount_id: removed.mount_id,
            lease_epoch,
            base_size: 0,
            generation,
            mode: WriteMode::Overwrite,
            open_client_id: removed.open_client_id,
            layout,
            expires_at_ms,
            ancestor_inode_ids,
            issued_targets: Vec::new(),
            issued_steps: HashMap::new(),
            pending_allocate_block: None,
            active_publication: None,
            publication_submitted: false,
            create_replay: Some(ActiveCreateReplay {
                operation_id,
                request_deadline_ms: removed.request_deadline_ms,
                normalized_path: removed.normalized_path,
                mount_id: removed.mount_id,
                expected_mount_epoch: removed.expected_mount_epoch,
                mount_root_inode_id: removed.mount_root_inode_id,
                response,
            }),
        };
        Self::insert_entry(&mut state, WriteSessionEntry::Active(Box::new(session.clone())));
        Ok(session)
    }

    /// Replay an issued AllocateBlock step or reserve capacity before Raft allocation.
    ///
    /// Replay is resolved before capacity checks. A new step installs the one
    /// pending slot and increments global occupancy under the registry lock, so
    /// limit-plus-one cannot cross the subsequent Raft boundary concurrently.
    /// A SyncWrite-retired suffix is no longer replayable: its predecessor may
    /// become the end of the current chain and identify a fresh allocation.
    pub(crate) fn begin_allocate_block(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        previous_block_id: Option<BlockId>,
    ) -> Result<BeginAllocateBlock<'_>, BeginAllocateBlockError> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let pending = PendingAllocateBlock { previous_block_id };
        let (layout, open_client_id, file_offset) = {
            let session = Self::active_session_mut(&mut state, inode_id).map_err(BeginAllocateBlockError::Session)?;
            if session.lease_epoch != lease_epoch {
                return Err(BeginAllocateBlockError::Session(
                    "write session lease epoch mismatch".to_string(),
                ));
            }
            if let Some(target_index) = session.issued_steps.get(&previous_block_id) {
                let target = session.issued_targets.get(*target_index).cloned().ok_or_else(|| {
                    BeginAllocateBlockError::Internal("issued AllocateBlock target index is inconsistent".to_string())
                })?;
                return Ok(BeginAllocateBlock::Replay(target));
            }

            if session.active_publication.is_some() {
                return Err(BeginAllocateBlockError::PublicationInProgress);
            }

            let expected_previous = session.issued_targets.last().map(|target| target.block_id);
            if previous_block_id != expected_previous {
                return Err(BeginAllocateBlockError::InvalidArgument(format!(
                    "AllocateBlock predecessor mismatch: expected {expected_previous:?}, got {previous_block_id:?}"
                )));
            }
            if session.pending_allocate_block.is_some() {
                return Err(BeginAllocateBlockError::Pending);
            }
            let file_offset =
                Self::next_target_file_offset(session).map_err(BeginAllocateBlockError::InvalidArgument)?;
            if session.issued_targets.len() >= self.max_write_targets_per_session {
                observe::record_write_target_rejected(WriteTargetLimit::PerSession.label());
                return Err(BeginAllocateBlockError::LimitExceeded(WriteTargetLimitExceeded {
                    limit: WriteTargetLimit::PerSession,
                    maximum: self.max_write_targets_per_session,
                }));
            }
            (session.layout, session.open_client_id, file_offset)
        };

        if state.outstanding_write_targets >= self.max_write_targets {
            observe::record_write_target_rejected(WriteTargetLimit::Global.label());
            return Err(BeginAllocateBlockError::LimitExceeded(WriteTargetLimitExceeded {
                limit: WriteTargetLimit::Global,
                maximum: self.max_write_targets,
            }));
        }
        let session = Self::active_session_mut(&mut state, inode_id)
            .expect("validated active session must remain current under the registry lock");
        assert!(session.pending_allocate_block.replace(pending.clone()).is_none());
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_add(1)
            .expect("write-target occupancy below its limit must increment");
        state.pending_write_targets = state
            .pending_write_targets
            .checked_add(1)
            .expect("pending write-target occupancy must increment");
        Self::record_write_target_gauges(&state);

        Ok(BeginAllocateBlock::Reserved(WriteTargetReservation {
            registry: self,
            inode_id,
            lease_epoch,
            pending,
            layout,
            open_client_id,
            file_offset,
            armed: true,
        }))
    }

    /// Freeze one active session's issued-target sequence for file publication.
    ///
    /// The pending-target check and publication identity installation happen
    /// under the same lock used by AllocateBlock, closing both allocation-completion
    /// and pre-proposal races.
    pub(crate) fn begin_publication(
        self: &Arc<Self>,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    ) -> Result<WritePublication, BeginWritePublicationError> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        {
            let session =
                Self::active_session_mut(&mut state, inode_id).map_err(BeginWritePublicationError::Session)?;
            if session.lease_epoch != lease_epoch {
                return Err(BeginWritePublicationError::Session(
                    "write session lease epoch mismatch".to_string(),
                ));
            }
            if session.pending_allocate_block.is_some() {
                return Err(BeginWritePublicationError::AllocateBlockPending);
            }
            if session.active_publication.is_some() {
                return Err(BeginWritePublicationError::PublicationInProgress);
            }
        }

        let publication_id = WritePublicationId(state.next_publication_id);
        state.next_publication_id = state
            .next_publication_id
            .checked_add(1)
            .ok_or(BeginWritePublicationError::PublicationIdExhausted)?;
        let session = Self::active_session_mut(&mut state, inode_id)
            .expect("validated active session must remain current under the registry lock");
        assert!(session.active_publication.replace(publication_id).is_none());
        let session = session.clone();
        Ok(WritePublication {
            registry: Arc::clone(self),
            session,
            publication_id,
            armed: true,
            submitted: false,
        })
    }

    /// Replace only the matching pending step with one fully validated target.
    fn complete_write_target(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        pending: &PendingAllocateBlock,
        target: LocatedBlock,
        now_ms: u64,
    ) -> Result<LocatedBlock, CompleteWriteTargetError> {
        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let validation = {
            let session =
                Self::active_session_mut(&mut state, inode_id).map_err(|_| CompleteWriteTargetError::NotCurrent)?;
            if session.lease_epoch != lease_epoch {
                return Err(CompleteWriteTargetError::NotCurrent);
            }
            if session.pending_allocate_block.as_ref() != Some(pending) {
                return Err(CompleteWriteTargetError::NotCurrent);
            }
            Self::validate_write_target(session, lease_epoch, &target)
        };
        if let Err(error) = validation {
            Self::cancel_write_target_locked(&mut state, inode_id, lease_epoch, pending);
            return Err(CompleteWriteTargetError::InvalidTarget(error));
        }

        let session = Self::active_session_mut(&mut state, inode_id)
            .expect("validated active session must remain current under the registry lock");
        let target_index = session.issued_targets.len();
        session.issued_targets.push(target.clone());
        assert!(
            session
                .issued_steps
                .insert(pending.previous_block_id, target_index)
                .is_none(),
            "reserved AllocateBlock predecessor must not already be issued"
        );
        assert_eq!(session.pending_allocate_block.take().as_ref(), Some(pending));
        state.pending_write_targets = state
            .pending_write_targets
            .checked_sub(1)
            .expect("completed target must own one pending count");
        Self::record_write_target_gauges(&state);
        Ok(target)
    }

    /// Revalidate fencing, layout, offset, and generation before issuing a reserved target.
    fn validate_write_target(
        session: &WriteSession,
        lease_epoch: LeaseEpoch,
        target: &LocatedBlock,
    ) -> Result<(), String> {
        if target.block_id.inode_id != session.inode_id {
            return Err("write target inode mismatch".to_string());
        }
        if target.fencing_token.block_id != target.block_id
            || target.fencing_token.owner != session.open_client_id
            || target.fencing_token.epoch != lease_epoch
        {
            return Err("write target fencing token mismatch".to_string());
        }
        let next_file_offset = Self::next_target_file_offset(session)?;
        if target.file_offset != next_file_offset {
            return Err(format!(
                "write target file offset changed: expected {next_file_offset}, got {}",
                target.file_offset
            ));
        }
        let target_shape = BlockShape::new(
            target.block_format_id,
            target.block_size,
            target.chunk_size,
            target.block_size,
        )
        .map_err(|error| format!("invalid write target shape: {error}"))?;
        let expected_shape = BlockShape::for_effective_len(&session.layout, u64::from(session.layout.block_size))
            .map_err(|error| format!("invalid session layout shape: {error}"))?;
        if target_shape != expected_shape {
            return Err("write target shape does not match the session layout".to_string());
        }
        if target.write_offset != 0 {
            return Err("new block must start at offset zero".into());
        }
        Ok(())
    }

    /// Return the next capacity-aligned target offset for the active session.
    ///
    /// Targets beginning below `base_size` belong to the already published
    /// prefix, including a partial block finalized by a previous SyncWrite.
    /// An unpublished target begins at or after `base_size` and advances the
    /// next offset by its full authorized capacity.
    fn next_target_file_offset(session: &WriteSession) -> Result<u64, String> {
        let Some(last) = session.issued_targets.last() else {
            return Ok(if session.mode == WriteMode::Overwrite {
                0
            } else {
                session.base_size
            });
        };
        last.file_offset
            .checked_add(last.block_size)
            .ok_or_else(|| "write target file offset overflow".to_string())
    }

    /// Get a live session or a submitted publication retained for resource protection.
    /// Callers must validate the actual lease deadline before admitting new work.
    pub fn get_session(&self, inode_id: InodeId) -> Option<WriteSession> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => Some((**session).clone()),
            Some(WriteSessionEntry::Opening(_)) | None => None,
        }
    }

    /// Get lightweight session presence, including an expired but still pending Commit.
    /// Presence protects resources; it does not extend the writer's lease deadline.
    pub(crate) fn get_session_identity(&self, inode_id: InodeId) -> Option<WriteSessionIdentity> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => Some(WriteSessionIdentity {
                mount_id: session.mount_id,
                lease_epoch: session.lease_epoch,
                open_client_id: session.open_client_id,
            }),
            Some(WriteSessionEntry::Opening(_)) | None => None,
        }
    }

    /// Remove only the session identified by the presented lease epoch.
    pub fn remove_session_if_epoch(&self, inode_id: InodeId, lease_epoch: LeaseEpoch) -> Option<WriteSession> {
        let mut state = self.state.write();
        match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) if session.lease_epoch == lease_epoch => {}
            Some(WriteSessionEntry::Opening(_) | WriteSessionEntry::Active(_)) | None => return None,
        }
        match Self::remove_entry(&mut state, inode_id) {
            Some(WriteSessionEntry::Active(session)) => Some(*session),
            Some(WriteSessionEntry::Opening(_)) | None => {
                unreachable!("validated active session must remain current under the registry lock")
            }
        }
    }

    /// Validate that a non-expired active session owns the presented epoch.
    pub(crate) fn validate_session(&self, inode_id: InodeId, lease_epoch: LeaseEpoch) -> Result<(), WriteSessionError> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        if state
            .entries
            .get(&inode_id)
            .is_some_and(|entry| entry.expires_at_ms() <= now_ms)
        {
            Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
            return Err(WriteSessionError::Expired);
        }
        Self::retire_expired_entries(&mut state, now_ms);
        let session = match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => session,
            Some(WriteSessionEntry::Opening(_)) | None => return Err(WriteSessionError::NotFound),
        };
        if session.lease_epoch != lease_epoch {
            return Err(WriteSessionError::LeaseEpochMismatch {
                expected: session.lease_epoch,
                got: lease_epoch,
            });
        }
        Ok(())
    }

    /// Atomically validate ownership and move every expiry index forward.
    pub(crate) fn renew_session(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        client_id: ClientId,
    ) -> Result<u64, WriteSessionError> {
        self.renew_session_at(inode_id, lease_epoch, client_id, current_time_ms())
    }

    fn renew_session_at(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        client_id: ClientId,
        now_ms: u64,
    ) -> Result<u64, WriteSessionError> {
        let mut state = self.state.write();
        if state
            .entries
            .get(&inode_id)
            .is_some_and(|entry| entry.expires_at_ms() <= now_ms)
        {
            Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
            return Err(WriteSessionError::Expired);
        }
        Self::retire_expired_entries(&mut state, now_ms);
        let (ancestor_inode_ids, previous_expires_at_ms, submitted) = match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => {
                if session.lease_epoch != lease_epoch {
                    return Err(WriteSessionError::LeaseEpochMismatch {
                        expected: session.lease_epoch,
                        got: lease_epoch,
                    });
                }
                if session.open_client_id != client_id {
                    return Err(WriteSessionError::OwnerMismatch);
                }
                (
                    session.ancestor_inode_ids.clone(),
                    session.expires_at_ms,
                    session.publication_submitted,
                )
            }
            Some(WriteSessionEntry::Opening(_)) | None => return Err(WriteSessionError::NotFound),
        };
        let expires_at_ms = now_ms.saturating_add(self.session_ttl_ms).max(previous_expires_at_ms);

        if !submitted {
            Self::move_expiry_indexes(
                &mut state,
                inode_id,
                &ancestor_inode_ids,
                previous_expires_at_ms,
                expires_at_ms,
            );
        }
        match state.entries.get_mut(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => session.expires_at_ms = expires_at_ms,
            Some(WriteSessionEntry::Opening(_)) | None => {
                unreachable!("validated active session must remain current under the registry lock")
            }
        }
        Ok(expires_at_ms)
    }

    /// Retire at most one bounded batch of expired opening and active sessions.
    pub(crate) fn retire_expired_batch(&self) -> usize {
        let mut state = self.state.write();
        Self::retire_expired_entries(&mut state, current_time_ms())
    }

    /// Return whether this inode has a live writer or a pending Commit protection.
    pub(crate) fn has_active_write(&self, inode_id: InodeId) -> bool {
        self.has_active_write_under(inode_id)
    }

    /// Return whether the inode contains a live writer or a pending Commit protection.
    ///
    /// This does not walk namespace descendants. A bounded sweep may leave
    /// physically stale entries, but the maximum mirrored expiry prevents them
    /// from producing a false `EBUSY`. Submitted publications deliberately retain
    /// exclusion until publication or fencing has a durable result.
    pub fn has_active_write_under(&self, inode_id: InodeId) -> bool {
        self.has_active_write_under_at(inode_id, current_time_ms())
    }

    fn has_active_write_under_at(&self, inode_id: InodeId, now_ms: u64) -> bool {
        let mut state = self.state.write();
        Self::retire_expired_entries(&mut state, now_ms);
        state
            .ancestor_activity
            .get(&inode_id)
            .and_then(|activity| activity.sessions_by_expiry.last_key_value())
            .is_some_and(|(expires_at_ms, _)| *expires_at_ms > now_ms)
    }

    /// Validate the bounded, acyclic path identity stored by one write session.
    pub(crate) fn validate_ancestor_chain(inode_id: InodeId, ancestor_inode_ids: &[InodeId]) -> Result<(), String> {
        if ancestor_inode_ids.is_empty() {
            return Err("write session ancestor chain cannot be empty".to_string());
        }
        if ancestor_inode_ids.len() > crate::path_resolver::MAX_PATH_COMPONENTS + 1 {
            return Err("write session ancestor chain exceeds the path depth limit".to_string());
        }
        if ancestor_inode_ids.last() != Some(&inode_id) {
            return Err("write session ancestor chain must end at the file inode".to_string());
        }
        let mut unique_inode_ids = HashSet::with_capacity(ancestor_inode_ids.len());
        if ancestor_inode_ids
            .iter()
            .any(|ancestor_inode_id| !unique_inode_ids.insert(*ancestor_inode_id))
        {
            return Err("write session ancestor chain contains a cycle".to_string());
        }
        Ok(())
    }

    /// Validate the bounded mount-root-to-parent path held before inode creation.
    fn validate_parent_ancestor_chain(ancestor_inode_ids: &[InodeId]) -> Result<(), String> {
        if ancestor_inode_ids.is_empty() {
            return Err("create session parent ancestor chain cannot be empty".to_string());
        }
        if ancestor_inode_ids.len() > crate::path_resolver::MAX_PATH_COMPONENTS {
            return Err("create session parent ancestor chain exceeds the path depth limit".to_string());
        }
        let mut unique_inode_ids = HashSet::with_capacity(ancestor_inode_ids.len());
        if ancestor_inode_ids
            .iter()
            .any(|ancestor_inode_id| !unique_inode_ids.insert(*ancestor_inode_id))
        {
            return Err("create session parent ancestor chain contains a cycle".to_string());
        }
        Ok(())
    }

    /// Revalidate that this exact publication still owns a non-expired session.
    fn revalidate_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publication_id: WritePublicationId,
        now_ms: u64,
    ) -> Result<WriteSession, String> {
        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let session = Self::active_session_mut(&mut state, inode_id)?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if session.active_publication != Some(publication_id) {
            return Err("write publication is no longer current".to_string());
        }
        Ok(session.clone())
    }

    /// Apply one successful SyncWrite result and release its exact ownership.
    fn complete_sync_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publication_id: WritePublicationId,
        generation: ContentGeneration,
        file_size: u64,
        now_ms: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let session = Self::active_session_mut(&mut state, inode_id)?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if session.active_publication != Some(publication_id) {
            return Err("write publication is no longer current".to_string());
        }
        if session.generation == generation {
            if session.base_size != file_size {
                return Err(format!(
                    "replayed SyncWrite size changed: expected {}, got {file_size}",
                    session.base_size
                ));
            }
            session.mode = WriteMode::Append;
            Self::release_sync_publication(&mut state, inode_id);
            return Ok(());
        }
        let expected_generation = session
            .generation
            .checked_next()
            .ok_or_else(|| "content generation overflow".to_string())?;
        if generation != expected_generation {
            return Err(format!(
                "SyncWrite content generation changed: expected {expected_generation}, got {generation}"
            ));
        }

        // Before the new generation is installed, every target at or beyond the
        // visible end still belongs to the old session generation. Removing that
        // suffix prevents stale capacity-based offsets from being replayed.
        let retained_target_count = session
            .issued_targets
            .partition_point(|target| target.file_offset < file_size);
        let removed_target_count = session.issued_targets.len() - retained_target_count;
        session.issued_targets.truncate(retained_target_count);
        session
            .issued_steps
            .retain(|_, target_index| *target_index < retained_target_count);
        session.generation = generation;
        session.base_size = file_size;
        session.mode = WriteMode::Append;
        session.active_publication = None;
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_sub(removed_target_count)
            .expect("discarded issued targets must be included in global occupancy");
        Self::release_sync_publication(&mut state, inode_id);
        Self::record_write_target_gauges(&state);
        Ok(())
    }

    /// Restore ordinary lease expiry only after the visible prefix is durable.
    fn release_sync_publication(state: &mut SessionRegistryState, inode_id: InodeId) {
        let session = Self::active_session_mut(state, inode_id).expect("publication owns the session");
        session.active_publication = None;
        if session.publication_submitted {
            session.publication_submitted = false;
            let expires_at_ms = session.expires_at_ms;
            let ancestors = session.ancestor_inode_ids.clone();
            Self::move_expiry_indexes(state, inode_id, &ancestors, u64::MAX, expires_at_ms);
        }
    }

    /// Remove the active session only when the successful CommitFile still owns it.
    fn complete_commit_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publication_id: WritePublicationId,
    ) {
        let mut state = self.state.write();
        if matches!(state.entries.get(&inode_id),
            Some(WriteSessionEntry::Active(session)) if session.lease_epoch == lease_epoch
                && session.active_publication == Some(publication_id))
        {
            Self::remove_entry(&mut state, inode_id);
        }
    }

    /// Release only the matching publication so stale owners cannot clear new state.
    fn cancel_publication(&self, inode_id: InodeId, lease_epoch: LeaseEpoch, publication_id: WritePublicationId) {
        let mut state = self.state.write();
        if let Ok(session) = Self::active_session_mut(&mut state, inode_id) {
            if session.lease_epoch == lease_epoch && session.active_publication == Some(publication_id) {
                session.active_publication = None;
            }
        }
    }

    /// Remove one exact opening after its owner is cancelled or returns early.
    fn cancel_opening(&self, inode_id: InodeId, opening_id: WriteOpeningId) {
        let mut state = self.state.write();
        if matches!(
            state.entries.get(&inode_id),
            Some(WriteSessionEntry::Opening(opening)) if opening.opening_id == opening_id
        ) {
            Self::remove_entry(&mut state, inode_id);
        }
    }

    /// Remove only the matching unbound CreateFile opening.
    fn cancel_create_opening(&self, operation_id: CreateFileOperationId, opening_id: WriteOpeningId) {
        let mut state = self.state.write();
        if matches!(
            state.create_openings.get(&operation_id),
            Some(opening) if opening.opening_id == opening_id
        ) {
            Self::remove_create_opening(&mut state, operation_id);
        }
    }

    /// Release only the matching pending AllocateBlock step after failure or cancellation.
    fn cancel_write_target(&self, inode_id: InodeId, lease_epoch: LeaseEpoch, pending: &PendingAllocateBlock) {
        let mut state = self.state.write();
        Self::cancel_write_target_locked(&mut state, inode_id, lease_epoch, pending);
    }

    /// Release only the exact pending step so a stale guard cannot cancel replacement state.
    fn cancel_write_target_locked(
        state: &mut SessionRegistryState,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        pending: &PendingAllocateBlock,
    ) -> bool {
        let matches = matches!(
            state.entries.get(&inode_id),
            Some(WriteSessionEntry::Active(session))
                if session.lease_epoch == lease_epoch && session.pending_allocate_block.as_ref() == Some(pending)
        );
        if !matches {
            return false;
        }
        let session = Self::active_session_mut(state, inode_id)
            .expect("matching pending target must belong to an active session");
        assert_eq!(session.pending_allocate_block.take().as_ref(), Some(pending));
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_sub(1)
            .expect("pending target must own one outstanding count");
        state.pending_write_targets = state
            .pending_write_targets
            .checked_sub(1)
            .expect("pending target must own one pending count");
        Self::record_write_target_gauges(state);
        true
    }

    /// Insert one primary entry and every derived index under the state lock.
    fn insert_entry(state: &mut SessionRegistryState, entry: WriteSessionEntry) {
        let inode_id = match &entry {
            WriteSessionEntry::Opening(opening) => opening.inode_id,
            WriteSessionEntry::Active(session) => session.inode_id,
        };
        let client_id = entry.client_id();
        let expires_at_ms = entry.retirement_at_ms();
        for ancestor_inode_id in entry.ancestor_inode_ids() {
            let activity = state
                .ancestor_activity
                .entry(*ancestor_inode_id)
                .or_insert(AncestorWriteActivity {
                    sessions_by_expiry: BTreeMap::new(),
                });
            *activity.sessions_by_expiry.entry(expires_at_ms).or_default() += 1;
        }
        assert!(
            state.entries_by_expiry.insert((expires_at_ms, inode_id)),
            "new write session expiry index must be unique"
        );
        if entry.is_opening() {
            state.opening_sessions += 1;
        }
        *state.occupied_sessions_by_client.entry(client_id).or_default() += 1;
        if let WriteSessionEntry::Active(session) = &entry {
            if let Some(operation_id) = session.create_replay.as_ref().map(|replay| replay.operation_id) {
                assert!(state.active_create_operations.insert(operation_id, inode_id).is_none());
            }
        }
        assert!(state.entries.insert(inode_id, entry).is_none());
        Self::record_session_gauges(state);
    }

    /// Insert an unbound CreateFile opening and all of its shared capacity indexes.
    fn insert_create_opening(state: &mut SessionRegistryState, opening: CreateOpeningSession) {
        assert!(state
            .create_openings_by_path
            .insert(
                (opening.mount_id, opening.normalized_path.clone()),
                opening.operation_id,
            )
            .is_none());
        for ancestor_inode_id in &opening.parent_ancestor_inode_ids {
            let activity = state
                .ancestor_activity
                .entry(*ancestor_inode_id)
                .or_insert(AncestorWriteActivity {
                    sessions_by_expiry: BTreeMap::new(),
                });
            *activity.sessions_by_expiry.entry(opening.expires_at_ms).or_default() += 1;
        }
        assert!(state
            .create_openings_by_expiry
            .insert((opening.expires_at_ms, opening.opening_id)));
        assert!(state
            .create_opening_operations
            .insert(opening.opening_id, opening.operation_id)
            .is_none());
        *state
            .occupied_sessions_by_client
            .entry(opening.open_client_id)
            .or_default() += 1;
        assert!(state.create_openings.insert(opening.operation_id, opening).is_none());
        Self::record_session_gauges(state);
    }

    /// Remove one unbound CreateFile opening and every derived index.
    fn remove_create_opening(
        state: &mut SessionRegistryState,
        operation_id: CreateFileOperationId,
    ) -> Option<CreateOpeningSession> {
        let opening = state.create_openings.remove(&operation_id)?;
        assert_eq!(
            state
                .create_openings_by_path
                .remove(&(opening.mount_id, opening.normalized_path.clone())),
            Some(operation_id)
        );
        assert!(state
            .create_openings_by_expiry
            .remove(&(opening.expires_at_ms, opening.opening_id)));
        assert_eq!(
            state.create_opening_operations.remove(&opening.opening_id),
            Some(operation_id)
        );
        for ancestor_inode_id in &opening.parent_ancestor_inode_ids {
            let remove_entry = {
                let activity = state
                    .ancestor_activity
                    .get_mut(ancestor_inode_id)
                    .expect("create opening ancestor index must exist");
                Self::decrement_expiry_count(&mut activity.sessions_by_expiry, opening.expires_at_ms);
                activity.sessions_by_expiry.is_empty()
            };
            if remove_entry {
                state.ancestor_activity.remove(ancestor_inode_id);
            }
        }
        Self::decrement_client_occupancy(state, opening.open_client_id);
        Self::record_session_gauges(state);
        Some(opening)
    }

    /// Remove one primary entry and every derived index under the state lock.
    fn remove_entry(state: &mut SessionRegistryState, inode_id: InodeId) -> Option<WriteSessionEntry> {
        let entry = state.entries.get(&inode_id)?;
        let client_id = entry.client_id();
        let (owned_write_targets, pending_write_targets) = match entry {
            WriteSessionEntry::Opening(_) => (0, 0),
            WriteSessionEntry::Active(session) => (
                session.issued_targets.len() + usize::from(session.pending_allocate_block.is_some()),
                usize::from(session.pending_allocate_block.is_some()),
            ),
        };
        assert!(
            state
                .occupied_sessions_by_client
                .get(&client_id)
                .copied()
                .unwrap_or_default()
                > 0,
            "write session entry must own one client capacity slot"
        );
        let entry = state.entries.remove(&inode_id)?;
        if let WriteSessionEntry::Active(session) = &entry {
            if let Some(operation_id) = session.create_replay.as_ref().map(|replay| replay.operation_id) {
                assert_eq!(state.active_create_operations.remove(&operation_id), Some(inode_id));
            }
        }
        if entry.is_opening() {
            state.opening_sessions = state
                .opening_sessions
                .checked_sub(1)
                .expect("opening entry must own one opening count");
        }
        Self::remove_from_indexes(state, inode_id, &entry);
        Self::decrement_client_occupancy(state, client_id);
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_sub(owned_write_targets)
            .expect("removed session targets must be included in global occupancy");
        state.pending_write_targets = state
            .pending_write_targets
            .checked_sub(pending_write_targets)
            .expect("removed session pending target must be included in pending occupancy");
        Self::record_session_gauges(state);
        Self::record_write_target_gauges(state);
        Some(entry)
    }

    /// Decrement one opening-or-active client slot and remove empty keys.
    fn decrement_client_occupancy(state: &mut SessionRegistryState, client_id: ClientId) {
        let remove_client = {
            let count = state
                .occupied_sessions_by_client
                .get_mut(&client_id)
                .expect("validated write-session client occupancy must exist");
            *count = count
                .checked_sub(1)
                .expect("validated write-session client occupancy must be positive");
            *count == 0
        };
        if remove_client {
            state.occupied_sessions_by_client.remove(&client_id);
        }
    }

    /// Move expiry and namespace-exclusion indexes under the registry lock.
    fn move_expiry_indexes(
        state: &mut SessionRegistryState,
        inode_id: InodeId,
        ancestors: &[InodeId],
        previous: u64,
        next: u64,
    ) {
        assert!(state.entries_by_expiry.remove(&(previous, inode_id)));
        state.entries_by_expiry.insert((next, inode_id));
        for ancestor in ancestors {
            let activity = state
                .ancestor_activity
                .get_mut(ancestor)
                .expect("session ancestor index");
            Self::decrement_expiry_count(&mut activity.sessions_by_expiry, previous);
            *activity.sessions_by_expiry.entry(next).or_default() += 1;
        }
    }

    /// Remove the exact global-expiry and ancestor-expiry entries for an entry.
    fn remove_from_indexes(state: &mut SessionRegistryState, inode_id: InodeId, entry: &WriteSessionEntry) {
        let expires_at_ms = entry.retirement_at_ms();
        assert!(
            state.entries_by_expiry.remove(&(expires_at_ms, inode_id)),
            "write session expiry index must exist"
        );
        for ancestor_inode_id in entry.ancestor_inode_ids() {
            let remove_entry = {
                let activity = state
                    .ancestor_activity
                    .get_mut(ancestor_inode_id)
                    .expect("write session ancestor index must exist");
                Self::decrement_expiry_count(&mut activity.sessions_by_expiry, expires_at_ms);
                activity.sessions_by_expiry.is_empty()
            };
            if remove_entry {
                state.ancestor_activity.remove(ancestor_inode_id);
            }
        }
    }

    /// Retire the earliest expired entries without exceeding the sweep budget.
    fn retire_expired_entries(state: &mut SessionRegistryState, now_ms: u64) -> usize {
        let mut retired = 0;
        while retired < MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL {
            let session_expiry = state.entries_by_expiry.first().copied();
            let create_expiry = state.create_openings_by_expiry.first().copied();
            let next_is_create = match (session_expiry, create_expiry) {
                (None, None) => break,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (Some((session_ms, _)), Some((create_ms, _))) => create_ms < session_ms,
            };
            let expires_at_ms = if next_is_create {
                create_expiry.expect("selected CreateFile expiry").0
            } else {
                session_expiry.expect("selected write-session expiry").0
            };
            if expires_at_ms > now_ms {
                break;
            }
            if next_is_create {
                let (_, opening_id) = create_expiry.expect("selected CreateFile expiry");
                let operation_id = state
                    .create_opening_operations
                    .get(&opening_id)
                    .copied()
                    .expect("CreateFile expiry must identify an operation");
                assert!(Self::remove_create_opening(state, operation_id).is_some());
            } else {
                let (_, inode_id) = session_expiry.expect("selected write-session expiry");
                if Self::remove_entry(state, inode_id).is_none() {
                    state.entries_by_expiry.remove(&(expires_at_ms, inode_id));
                }
            }
            observe::record_write_session_expired();
            retired += 1;
        }
        retired
    }

    /// Retire one exact pending CreateFile operation after its lease deadline.
    fn retire_expired_create_opening_for_operation(
        state: &mut SessionRegistryState,
        operation_id: CreateFileOperationId,
        now_ms: u64,
    ) -> bool {
        let is_expired = state
            .create_openings
            .get(&operation_id)
            .is_some_and(|opening| opening.expires_at_ms <= now_ms);
        if is_expired && Self::remove_create_opening(state, operation_id).is_some() {
            observe::record_write_session_expired();
            return true;
        }
        false
    }

    /// Retire one requested inode even when it lies beyond the sweep budget.
    fn retire_expired_entry_for_inode(state: &mut SessionRegistryState, inode_id: InodeId, now_ms: u64) -> bool {
        let is_expired = state
            .entries
            .get(&inode_id)
            .is_some_and(|entry| entry.retirement_at_ms() <= now_ms);
        if is_expired && Self::remove_entry(state, inode_id).is_some() {
            observe::record_write_session_expired();
            return true;
        }
        false
    }

    fn decrement_expiry_count(expirations: &mut BTreeMap<u64, usize>, expires_at_ms: u64) {
        let remove_expiry = {
            let count = expirations
                .get_mut(&expires_at_ms)
                .expect("write session ancestor expiry must exist");
            *count -= 1;
            *count == 0
        };
        if remove_expiry {
            expirations.remove(&expires_at_ms);
        }
    }

    fn active_session_mut(state: &mut SessionRegistryState, inode_id: InodeId) -> Result<&mut WriteSession, String> {
        match state.entries.get_mut(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => Ok(session),
            Some(WriteSessionEntry::Opening(_)) => Err("write session is still opening".to_string()),
            None => Err("write session not found".to_string()),
        }
    }

    fn record_session_gauges(state: &SessionRegistryState) {
        let opening_sessions = state.opening_sessions + state.create_openings.len();
        let active_sessions = state
            .entries
            .len()
            .checked_sub(state.opening_sessions)
            .expect("opening session count cannot exceed primary entries");
        observe::set_write_sessions(opening_sessions, active_sessions);
    }

    /// Publish issued occupancy as total outstanding capacity minus pending reservations.
    fn record_write_target_gauges(state: &SessionRegistryState) {
        let issued = state
            .outstanding_write_targets
            .checked_sub(state.pending_write_targets)
            .expect("pending write targets cannot exceed total occupancy");
        observe::set_write_targets(state.pending_write_targets, issued);
    }
}

impl Drop for WriteOpening<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.cancel_opening(self.inode_id, self.opening_id);
            self.armed = false;
        }
    }
}

impl Drop for CreateOpening<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.cancel_create_opening(self.operation_id, self.opening_id);
            self.armed = false;
        }
    }
}

impl Drop for WriteTargetReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry
                .cancel_write_target(self.inode_id, self.lease_epoch, &self.pending);
            self.armed = false;
        }
    }
}

impl Drop for WritePublication {
    fn drop(&mut self) {
        if self.armed && !self.submitted {
            self.registry
                .cancel_publication(self.session.inode_id, self.session.lease_epoch, self.publication_id);
        }
        // A submitted owner belongs to the completion task. If that task cannot
        // prove completion or fencing, retain bounded protection until recovery.
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Default for SessionRegistry {
    fn default() -> Self {
        let config = MetadataConfig::default();
        Self::new(
            config.write_session_limits.max_active,
            config.write_session_limits.max_active_per_client,
            config.write_target_limits.max_outstanding,
            config.write_target_limits.max_outstanding_per_session,
            config.write_lease_timeout_ms,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::ids::BlockIndex;
    use beryl_types::lease::FencingToken;
    use beryl_types::{BlockFormatId, Tier};
    use std::sync::{Arc, Barrier, Condvar, Mutex};

    fn write_target(inode_id: InodeId, index: u32) -> LocatedBlock {
        let block_id = BlockId::new(inode_id, BlockIndex::new(index));
        LocatedBlock {
            write_offset: 0,
            block_id,
            file_offset: 0,
            block_size: 64,
            worker_endpoints: Vec::new(),
            fencing_token: FencingToken {
                block_id,
                owner: ClientId::new(1),
                epoch: LeaseEpoch::new(7),
            },

            chunk_size: BlockFormatId::CURRENT_FOR_NEW_FILE.spec().unwrap().storage_chunk_size,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: Tier::Hdd,
        }
    }

    fn create_input(inode_id: InodeId) -> BeginSessionInput {
        BeginSessionInput {
            normalized_path: format!("/inode-{}", inode_id.as_raw()),
            inode_id,
            mount_id: MountId::new(1),
            current_lease_epoch: LeaseEpoch::new(6),
            mode: WriteMode::Overwrite,
            open_client_id: ClientId::new(1),
            layout: FileLayout::new(64),
            ancestor_inode_ids: vec![inode_id],
        }
    }

    fn opened_file(opening: &WriteOpening<'_>) -> crate::inode::FileData {
        let state = opening.registry.state.read();
        let Some(WriteSessionEntry::Opening(entry)) = state.entries.get(&opening.inode_id) else {
            panic!("opening fixture")
        };
        crate::inode::FileData {
            layout: entry.layout,
            len: 0,
            generation: ContentGeneration::default(),
            blocks: Vec::new(),
            next_index: 0,
            lease_epoch: entry.proposed_lease_epoch,
            last_commit: None,
        }
    }

    fn install_session(registry: &SessionRegistry, input: BeginSessionInput) -> Result<WriteSession, String> {
        let opening = registry
            .begin_session(input)
            .map_err(|error| format!("write session opening failed: {error:?}"))?;
        let lease_epoch = opening.proposed_lease_epoch();
        let file = opened_file(&opening);
        opening
            .activate(lease_epoch, &file, None)
            .map_err(|error| format!("write session activation failed: {error:?}"))
    }

    fn install_session_at(
        registry: &SessionRegistry,
        input: BeginSessionInput,
        now_ms: u64,
    ) -> Result<WriteSession, String> {
        let mut opening = registry
            .begin_session_at(input, now_ms)
            .map_err(|error| format!("write session opening failed: {error:?}"))?;
        let result = registry.activate_opening(
            opening.inode_id,
            opening.opening_id,
            opening.proposed_lease_epoch,
            now_ms,
            &opened_file(&opening),
            None,
        );
        if result.is_ok() || matches!(&result, Err(WriteOpeningError::NotCurrent | WriteOpeningError::Expired)) {
            opening.armed = false;
        }
        result.map_err(|error| format!("write session activation failed: {error:?}"))
    }

    fn begin_opening(
        registry: &SessionRegistry,
        inode_id: InodeId,
        client_id: ClientId,
    ) -> Result<WriteOpening<'_>, BeginSessionError> {
        let mut input = create_input(inode_id);
        input.open_client_id = client_id;
        registry.begin_session(input)
    }

    fn begin_create_input(operation_id: CreateFileOperationId) -> BeginCreateSessionInput {
        BeginCreateSessionInput {
            operation_id,
            request_deadline_ms: 100,
            normalized_path: "/created".to_string(),
            mount_id: MountId::new(1),
            expected_mount_epoch: 1,
            mount_root_inode_id: InodeId::new(1),
            open_client_id: operation_id.client_id,
            parent_ancestor_inode_ids: vec![InodeId::new(1)],
        }
    }

    fn issue_target(
        registry: &SessionRegistry,
        inode_id: InodeId,
        previous_block_id: Option<BlockId>,
        index: u32,
        file_offset: u64,
    ) -> LocatedBlock {
        let mut target = write_target(inode_id, index);
        target.file_offset = file_offset;
        let reservation = match registry
            .begin_allocate_block(inode_id, LeaseEpoch::new(7), previous_block_id)
            .unwrap()
        {
            BeginAllocateBlock::Reserved(reservation) => reservation,
            BeginAllocateBlock::Replay(_) => panic!("new test target must reserve capacity"),
        };
        reservation.complete(target).unwrap()
    }

    #[test]
    fn stale_opening_drop_cannot_remove_replacement_with_same_proposed_epoch() {
        let registry = SessionRegistry::new(1, 1, 100, 100, 1);
        let inode_id = InodeId::new(31);
        let input = create_input(inode_id);
        let stale = registry.begin_session_at(input.clone(), 0).unwrap();
        let mut replacement = registry.begin_session_at(input, 1).unwrap();
        assert_eq!(stale.proposed_lease_epoch, replacement.proposed_lease_epoch);
        assert_ne!(stale.opening_id, replacement.opening_id);

        drop(stale);
        assert!(matches!(
            registry.state.read().entries.get(&inode_id),
            Some(WriteSessionEntry::Opening(opening)) if opening.opening_id == replacement.opening_id
        ));

        let session = registry
            .activate_opening(
                replacement.inode_id,
                replacement.opening_id,
                replacement.proposed_lease_epoch,
                1,
                &opened_file(&replacement),
                None,
            )
            .unwrap();
        replacement.armed = false;
        assert_eq!(session.lease_epoch, LeaseEpoch::new(7));
    }

    #[test]
    fn concurrent_openings_never_exceed_global_capacity() {
        let registry = Arc::new(SessionRegistry::new(4, 4, 100, 100, 60_000));
        let contender_count = 16;
        let start = Arc::new(Barrier::new(contender_count + 1));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut joins = Vec::new();

        for contender in 0..contender_count {
            let registry = Arc::clone(&registry);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            let result_tx = result_tx.clone();
            joins.push(std::thread::spawn(move || {
                start.wait();
                let opening = begin_opening(&registry, InodeId::new(contender as u64 + 1), ClientId::new(1));
                result_tx.send(opening.is_ok()).unwrap();
                if let Ok(opening) = opening {
                    let (released, wake) = &*release;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = wake.wait(released).unwrap();
                    }
                    drop(opening);
                }
            }));
        }
        drop(result_tx);
        start.wait();
        let reserved = (0..contender_count)
            .filter(|_| result_rx.recv().expect("reservation result"))
            .count();
        assert_eq!(reserved, 4);

        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_all();
        for join in joins {
            join.join().unwrap();
        }
        let state = registry.state.read();
        assert_eq!(state.opening_sessions, 0);
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn create_opening_reserves_capacity_and_replays_after_activation() {
        let registry = Arc::new(SessionRegistry::new(2, 1, 100, 100, 60_000));
        let now_ms = current_time_ms();
        let operation_id = CreateFileOperationId {
            client_id: ClientId::new(9),
            call_id: CallId::new(),
        };
        let mut opening = match registry
            .begin_create_session_at(begin_create_input(operation_id), now_ms)
            .unwrap()
        {
            BeginCreateSession::Reserved(opening) => opening,
            BeginCreateSession::Replay(_) => panic!("first CreateFile must reserve capacity"),
        };
        let expires_at_ms = opening.expires_at_ms;
        let mut wrong_deadline = begin_create_input(operation_id);
        wrong_deadline.request_deadline_ms += 1;
        assert!(matches!(
            registry.begin_create_session_at(wrong_deadline, now_ms + 1),
            Err(BeginCreateSessionError::IdentityMismatch)
        ));
        let other_operation = CreateFileOperationId {
            client_id: ClientId::new(8),
            call_id: CallId::new(),
        };
        assert!(matches!(
            registry.begin_create_session_at(begin_create_input(other_operation), now_ms + 1),
            Err(BeginCreateSessionError::PathBusy)
        ));
        let mut competing = create_input(InodeId::new(2));
        competing.open_client_id = ClientId::new(9);
        competing.normalized_path = "/created".to_string();
        assert!(matches!(
            registry.begin_session_at(competing.clone(), now_ms + 1),
            Err(BeginSessionError::Busy)
        ));
        competing.normalized_path = "/other".to_string();
        assert!(matches!(
            registry.begin_session_at(competing, now_ms + 1),
            Err(BeginSessionError::LimitExceeded(_))
        ));

        let session = registry
            .activate_create_opening(
                opening.operation_id,
                opening.opening_id,
                InodeId::new(2),
                LeaseEpoch::new(1),
                expires_at_ms,
                FileLayout::new(64),
                ContentGeneration::new(0),
                now_ms + 1,
            )
            .unwrap();
        opening.armed = false;
        assert_eq!(session.inode_id, InodeId::new(2));

        registry
            .begin_publication(session.inode_id, session.lease_epoch)
            .unwrap()
            .complete_sync(ContentGeneration::new(1), 0)
            .unwrap();
        let renewed_expires_at_ms = registry
            .renew_session_at(
                session.inode_id,
                session.lease_epoch,
                session.open_client_id,
                now_ms + 10,
            )
            .unwrap();
        assert!(renewed_expires_at_ms >= expires_at_ms);

        let mut wrong_mount = begin_create_input(operation_id);
        wrong_mount.mount_id = MountId::new(2);
        assert!(matches!(
            registry.begin_create_session_at(wrong_mount, now_ms + 10),
            Err(BeginCreateSessionError::IdentityMismatch)
        ));
        let mut wrong_mount_epoch = begin_create_input(operation_id);
        wrong_mount_epoch.expected_mount_epoch = 2;
        assert!(matches!(
            registry.begin_create_session_at(wrong_mount_epoch, now_ms + 10),
            Err(BeginCreateSessionError::IdentityMismatch)
        ));
        let mut wrong_deadline = begin_create_input(operation_id);
        wrong_deadline.request_deadline_ms += 1;
        assert!(matches!(
            registry.begin_create_session_at(wrong_deadline, now_ms + 10),
            Err(BeginCreateSessionError::IdentityMismatch)
        ));
        let replay = registry
            .begin_create_session_at(begin_create_input(operation_id), now_ms + 10)
            .unwrap();
        let BeginCreateSession::Replay(current) = replay else {
            panic!("active CreateFile must replay its session")
        };
        assert_eq!(current.inode_id, session.inode_id);
        assert_eq!(current.lease_epoch, session.lease_epoch);
        assert_eq!(current.expires_at_ms, expires_at_ms);
        assert_eq!(current.generation, ContentGeneration::new(0));
        assert_eq!(
            registry.get_session(session.inode_id).unwrap().generation,
            ContentGeneration::new(1)
        );
    }

    #[test]
    fn failed_create_activation_releases_capacity_and_path_exclusion() {
        let registry = SessionRegistry::new(2, 2, 100, 100, 60_000);
        let operation_id = CreateFileOperationId {
            client_id: ClientId::new(9),
            call_id: CallId::new(),
        };
        let opening = match registry.begin_create_session(begin_create_input(operation_id)).unwrap() {
            BeginCreateSession::Reserved(opening) => opening,
            BeginCreateSession::Replay(_) => panic!("first CreateFile must reserve capacity"),
        };
        let expires_at_ms = opening.expires_at_ms();
        let inode_id = InodeId::new(2);
        install_session(&registry, create_input(inode_id)).unwrap();

        assert!(matches!(
            opening.activate(
                inode_id,
                LeaseEpoch::new(1),
                expires_at_ms,
                FileLayout::new(64),
                ContentGeneration::new(0)
            ),
            Err(WriteOpeningError::NotCurrent)
        ));
        assert!(registry.state.read().create_openings.is_empty());
        assert!(registry.state.read().create_openings_by_path.is_empty());

        registry.remove_session_if_epoch(inode_id, LeaseEpoch::new(7)).unwrap();
        assert!(matches!(
            registry.begin_create_session(begin_create_input(operation_id)),
            Ok(BeginCreateSession::Reserved(_))
        ));
    }

    #[test]
    fn delayed_cleanup_cannot_remove_a_newer_session() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(20);
        install_session(&registry, create_input(inode_id)).unwrap();
        registry.remove_session_if_epoch(inode_id, LeaseEpoch::new(7)).unwrap();
        let mut replacement = create_input(inode_id);
        replacement.current_lease_epoch = LeaseEpoch::new(7);
        install_session(&registry, replacement).unwrap();

        assert!(registry.remove_session_if_epoch(inode_id, LeaseEpoch::new(7)).is_none());
        assert_eq!(registry.get_session(inode_id).unwrap().lease_epoch, LeaseEpoch::new(8));
    }

    #[test]
    fn expiry_sweep_is_bounded_and_queries_ignore_residual_expired_entries() {
        let historical_expired_count = MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL * 3 + 17;
        let registry = SessionRegistry::new(historical_expired_count + 1, historical_expired_count + 1, 100, 100, 10);
        let residual_expired_count = MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL * 2 + 3;
        for raw in 1..=historical_expired_count {
            let inode_id = InodeId::new(raw as u64);
            install_session_at(&registry, create_input(inode_id), 0).unwrap();
        }
        for raw in 1..=(historical_expired_count - residual_expired_count) {
            registry
                .remove_session_if_epoch(InodeId::new(raw as u64), LeaseEpoch::new(7))
                .unwrap();
        }
        let active_inode_id = InodeId::new(20_000);
        let mut active = create_input(active_inode_id);
        active.ancestor_inode_ids = vec![active_inode_id];
        install_session_at(&registry, active, 1).unwrap();

        assert!(registry.has_active_write_under_at(active_inode_id, 10));
        {
            let state = registry.state.read();
            assert_eq!(
                state.entries.len(),
                residual_expired_count + 1 - MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL
            );
            assert!(state
                .entries_by_expiry
                .iter()
                .any(|(expires_at_ms, _)| *expires_at_ms == 10));
        }

        let residual_expired_inode_id = {
            let state = registry.state.read();
            state
                .entries
                .values()
                .find(|entry| entry.expires_at_ms() == 10)
                .and_then(|entry| match entry {
                    WriteSessionEntry::Active(session) => Some(session.inode_id),
                    WriteSessionEntry::Opening(_) => None,
                })
                .expect("one expired active session must remain after a bounded sweep")
        };
        assert!(!registry.has_active_write_under_at(residual_expired_inode_id, 10));
        while registry
            .state
            .read()
            .entries_by_expiry
            .iter()
            .any(|(expires_at_ms, _)| *expires_at_ms == 10)
        {
            assert!(registry.has_active_write_under_at(active_inode_id, 10));
        }

        registry
            .remove_session_if_epoch(active_inode_id, LeaseEpoch::new(7))
            .unwrap();
        let state = registry.state.read();
        assert!(state.entries.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.entries_by_expiry.is_empty());
    }

    #[test]
    fn concurrent_duplicate_allocate_block_reserves_one_target_before_completion() {
        let registry = Arc::new(SessionRegistry::default());
        let inode_id = InodeId::new(15);
        install_session(&registry, create_input(inode_id)).unwrap();
        let start = Arc::new(Barrier::new(2));
        let reserved = Arc::new(Barrier::new(2));

        let mut joins = Vec::new();
        for index in 0..2 {
            let registry = Arc::clone(&registry);
            let start = Arc::clone(&start);
            let reserved = Arc::clone(&reserved);
            joins.push(std::thread::spawn(move || {
                start.wait();
                match registry.begin_allocate_block(inode_id, LeaseEpoch::new(7), None) {
                    Ok(BeginAllocateBlock::Reserved(reservation)) => {
                        reserved.wait();
                        Some(reservation.complete(write_target(inode_id, index)).unwrap())
                    }
                    Err(BeginAllocateBlockError::Pending) => {
                        reserved.wait();
                        None
                    }
                    Ok(BeginAllocateBlock::Replay(_)) | Err(_) => panic!("unexpected concurrent AllocateBlock outcome"),
                }
            }));
        }

        let first = joins.remove(0).join().unwrap();
        let second = joins.remove(0).join().unwrap();
        let issued = first.or(second).expect("one request must issue the target");
        let session = registry.get_session(inode_id).unwrap();
        assert_eq!(session.issued_targets, vec![issued]);
        assert_eq!(session.issued_steps.len(), 1);
        assert_eq!(registry.state.read().outstanding_write_targets, 1);
        assert_eq!(registry.state.read().pending_write_targets, 0);
    }

    #[test]
    fn allocation_and_publication_preserve_only_the_active_replay_chain() {
        let registry = Arc::new(SessionRegistry::default());
        let inode_id = InodeId::new(16);
        install_session(&registry, create_input(inode_id)).unwrap();
        let first = issue_target(&registry, inode_id, None, 0, 0);
        let reservation = match registry
            .begin_allocate_block(inode_id, LeaseEpoch::new(7), Some(first.block_id))
            .unwrap()
        {
            BeginAllocateBlock::Reserved(reservation) => reservation,
            BeginAllocateBlock::Replay(_) => panic!("new predecessor must reserve"),
        };
        assert!(matches!(
            registry.begin_publication(inode_id, LeaseEpoch::new(7)),
            Err(BeginWritePublicationError::AllocateBlockPending)
        ));
        drop(reservation);

        let publication = registry
            .begin_publication(inode_id, LeaseEpoch::new(7))
            .expect("released AllocateBlock must unblock publication");
        assert!(matches!(
            registry.begin_allocate_block(inode_id, LeaseEpoch::new(7), Some(first.block_id)),
            Err(BeginAllocateBlockError::PublicationInProgress)
        ));
        drop(publication);

        let second = issue_target(&registry, inode_id, Some(first.block_id), 1, 64);
        assert_eq!(second.file_offset, 64);
        assert_eq!(registry.state.read().outstanding_write_targets, 2);
        assert_eq!(registry.state.read().pending_write_targets, 0);

        let publication = registry.begin_publication(inode_id, LeaseEpoch::new(7)).unwrap();
        assert!(matches!(
            registry.begin_allocate_block(inode_id, LeaseEpoch::new(7), None),
            Ok(BeginAllocateBlock::Replay(block)) if block == first
        ));
        publication.complete_sync(ContentGeneration::new(1), 64).unwrap();
        assert_eq!(registry.state.read().outstanding_write_targets, 1);
        for previous in [second.block_id, BlockId::from_u64_u32(999, 0)] {
            assert!(matches!(
                registry.begin_allocate_block(inode_id, LeaseEpoch::new(7), Some(previous)),
                Err(BeginAllocateBlockError::InvalidArgument(_))
            ));
        }
        let replacement = issue_target(&registry, inode_id, Some(first.block_id), 2, 64);
        for (previous, expected) in [(None, first.clone()), (Some(first.block_id), replacement)] {
            assert!(matches!(
                registry.begin_allocate_block(inode_id, LeaseEpoch::new(7), previous),
                Ok(BeginAllocateBlock::Replay(block)) if block == expected
            ));
        }
        let mut publication = registry.begin_publication(inode_id, LeaseEpoch::new(7)).unwrap();
        publication.mark_submitted().unwrap();
        let expired_at = publication.session().expires_at_ms + 1;
        assert!(registry.has_active_write_under_at(inode_id, expired_at));
        assert!(!SessionRegistry::retire_expired_entry_for_inode(
            &mut registry.state.write(),
            inode_id,
            expired_at
        ));
        assert_eq!(
            SessionRegistry::retire_expired_entries(&mut registry.state.write(), expired_at),
            0
        );
        assert!(matches!(
            registry.renew_session_at(inode_id, LeaseEpoch::new(7), ClientId::new(1), expired_at),
            Err(WriteSessionError::Expired)
        ));
        assert!(registry.get_session_identity(inode_id).is_some());
        publication.complete_commit();
        assert!(registry.get_session(inode_id).is_none());
        assert_eq!(registry.state.read().outstanding_write_targets, 0);
        install_session(&registry, create_input(inode_id)).unwrap();
        let old = registry.begin_publication(inode_id, LeaseEpoch::new(7)).unwrap();
        registry.remove_session_if_epoch(inode_id, LeaseEpoch::new(7)).unwrap();
        let mut input = create_input(inode_id);
        input.current_lease_epoch = LeaseEpoch::new(7);
        install_session(&registry, input).unwrap();
        old.complete_commit();
        assert_eq!(registry.get_session(inode_id).unwrap().lease_epoch, LeaseEpoch::new(8));
    }

    #[test]
    fn submitted_sync_releases_expiry_pin_after_apply_or_exact_replay() {
        for changes_content in [false, true] {
            let registry = Arc::new(SessionRegistry::default());
            let inode_id = InodeId::new(16);
            install_session(&registry, create_input(inode_id)).unwrap();
            let mut publication = registry.begin_publication(inode_id, LeaseEpoch::new(7)).unwrap();
            publication.mark_submitted().unwrap();
            let expired_at = publication.session().expires_at_ms + 1;
            assert!(registry.has_active_write_under_at(inode_id, expired_at));
            registry
                .complete_sync_publication(
                    inode_id,
                    LeaseEpoch::new(7),
                    publication.publication_id,
                    ContentGeneration::new(u64::from(changes_content)),
                    if changes_content { 64 } else { 0 },
                    expired_at,
                )
                .unwrap();
            drop(publication);
            assert!(!registry.has_active_write_under_at(inode_id, expired_at));
            let state = registry.state.read();
            assert!(state.entries.is_empty());
            assert!(state.entries_by_expiry.is_empty());
            assert!(state.ancestor_activity.is_empty());
        }
    }

    #[test]
    fn target_limits_count_pending_and_issued_while_replay_bypasses_capacity() {
        let registry = SessionRegistry::new(3, 3, 2, 1, 60_000);
        let first_inode = InodeId::new(31);
        let second_inode = InodeId::new(32);
        let third_inode = InodeId::new(33);
        for inode_id in [first_inode, second_inode, third_inode] {
            install_session(&registry, create_input(inode_id)).unwrap();
        }

        let first = issue_target(&registry, first_inode, None, 0, 0);
        assert!(matches!(
            registry.begin_allocate_block(first_inode, LeaseEpoch::new(7), Some(first.block_id)),
            Err(BeginAllocateBlockError::LimitExceeded(WriteTargetLimitExceeded {
                limit: WriteTargetLimit::PerSession,
                maximum: 1,
            }))
        ));
        issue_target(&registry, second_inode, None, 0, 0);
        assert!(matches!(
            registry.begin_allocate_block(third_inode, LeaseEpoch::new(7), None),
            Err(BeginAllocateBlockError::LimitExceeded(WriteTargetLimitExceeded {
                limit: WriteTargetLimit::Global,
                maximum: 2,
            }))
        ));
        assert!(matches!(
            registry.begin_allocate_block(first_inode, LeaseEpoch::new(7), None),
            Ok(BeginAllocateBlock::Replay(target)) if target == first
        ));

        registry
            .remove_session_if_epoch(second_inode, LeaseEpoch::new(7))
            .unwrap();
        assert!(matches!(
            registry.begin_allocate_block(third_inode, LeaseEpoch::new(7), None),
            Ok(BeginAllocateBlock::Reserved(_))
        ));
    }
}
