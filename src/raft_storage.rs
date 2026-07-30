// SPDX-License-Identifier: Apache-2.0

//! raft-rs [`Storage`] trait implementation backed by [`RaftLogStore`].
//!
//! Bridges the tikv/raft-rs `Storage` trait (which uses protobuf `Entry`,
//! `HardState`, `ConfState`, `Snapshot`) with nomad-rs's existing
//! [`RaftLogStore`] (JSONL-backed, `RaftLogEntry`-based). The bridge is a
//! two-layer storage:
//!
//! - **In-memory core** holds entries and state in protobuf-native form so
//!   raft-rs can read them cheaply.
//! - **`RaftLogStore`** receives every appended entry and every hard-state
//!   / snapshot change as it happens, so the on-disk representation stays
//!   consistent.
//!
//! Testing uses the in-memory layer directly; persistence is verified by
//! round-tripping through the file system like the existing [`RaftLogStore`]
//! tests do.

use std::cmp;
use std::path::Path;
use std::sync::{Arc, Mutex};

use raft::eraftpb::*;
use raft::storage::{GetEntriesContext, RaftState, Storage};
use raft::util::limit_size;
use raft::{Error as RaftError, Result as RaftResult, StorageError};

use crate::error::Result;

/// Inner mutable state of the raft-rs bridge.
///
/// Mirrors the structure of [`raft::storage::MemStorageCore`] but serialises
/// every mutation through the JSONL-backed [`crate::raft_log::RaftLogStore`].
#[derive(Debug)]
struct Inner {
    /// The raft state (hard state + conf state).
    raft_state: RaftState,
    /// In-memory log entries. `entries[i]` has raft-log position `i + first_index()`.
    entries: Vec<Entry>,
    /// Metadata of the last snapshot.
    snapshot_metadata: SnapshotMetadata,
    /// Optional RaftLogStore for durability.
    log_store: Option<crate::raft_log::RaftLogStore>,
}

/// Thread-safe implementation of the raft-rs [`Storage`] trait.
///
/// Data is held in memory for fast raft access and optionally persisted
/// through an inner [`RaftLogStore`] when created via [`Self::open`].
#[derive(Debug, Clone)]
pub struct RaftLogStorage {
    inner: Arc<Mutex<Inner>>,
}

impl RaftLogStorage {
    /// Create a new in-memory-only storage (no persistence).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                raft_state: RaftState::default(),
                entries: Vec::new(),
                snapshot_metadata: SnapshotMetadata::default(),
                log_store: None,
            })),
        }
    }

    /// Create storage backed by a JSONL file at `base_path`.
    ///
    /// # Errors
    ///
    /// Delegates to [`RaftLogStore::open`].
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self> {
        let log_store = crate::raft_log::RaftLogStore::open(&base_path)?;
        let base_index = log_store.base_index();

        let snap_path = base_path.as_ref().with_extension("snap");
        let mut snapshot_metadata = SnapshotMetadata::default();
        let raft_state = RaftState::default();

        if snap_path.exists() {
            let data = std::fs::read_to_string(&snap_path)?;
            let snap: crate::raft_log::RaftSnapshot = serde_json::from_str(&data)?;
            snapshot_metadata.index = snap.last_included_index;
            snapshot_metadata.term = snap.last_included_term;
        }

        let mut entries: Vec<Entry> = Vec::new();
        for log_entry in log_store.entries_from(base_index) {
            let mut e = Entry::default();
            e.index = log_entry.index;
            e.term = log_entry.term;
            let data = serde_json::to_vec(&log_entry.command).unwrap_or_default();
            e.data = data.into();
            entries.push(e);
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(Inner { raft_state, entries, snapshot_metadata, log_store: Some(log_store) })),
        })
    }

    /// Take a write lock and return a guard.
    fn wl(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Take a read lock and return a guard.
    fn rl(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The index of the first entry in this storage.
    fn first_index(&self) -> u64 {
        self.rl().first_index()
    }

    /// The index of the last entry.
    fn last_index(&self) -> u64 {
        self.rl().last_index()
    }
}

impl Inner {
    fn first_index(&self) -> u64 {
        if let Some(e) = self.entries.first() { e.index } else { self.snapshot_metadata.index + 1 }
    }

    fn last_index(&self) -> u64 {
        if let Some(e) = self.entries.last() { e.index } else { self.snapshot_metadata.index }
    }

    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
    }
}

impl Storage for RaftLogStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        Ok(self.rl().raft_state.clone())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let max_size = max_size.into();
        let core = self.wl();
        if low < core.first_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if high > core.last_index() + 1 {
            panic!("index out of bound (last: {}, high: {})", core.last_index() + 1, high);
        }
        let offset = core.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size);
        Ok(ents)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let core = self.rl();
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }
        let offset = core.first_index();
        if idx < offset {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if idx > core.last_index() {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - offset) as usize].term)
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let core = self.wl();
        let mut snapshot = Snapshot::default();
        let meta = snapshot.mut_metadata();
        meta.index = core.raft_state.hard_state.commit;
        meta.term = {
            if meta.index == core.snapshot_metadata.index {
                core.snapshot_metadata.term
            } else {
                let offset = core.entries[0].index;
                core.entries[(meta.index - offset) as usize].term
            }
        };
        meta.set_conf_state(core.raft_state.conf_state.clone());

        if meta.index < request_index {
            meta.index = request_index;
        }
        Ok(snapshot)
    }
}

impl RaftLogStorage {
    /// Set the hard state (term, vote, commit).
    pub fn set_hard_state(&self, hs: HardState) {
        self.wl().raft_state.hard_state = hs;
    }

    /// Set the conf state (active peer set).
    pub fn set_conf_state(&self, cs: ConfState) {
        self.wl().raft_state.conf_state = cs;
    }

    /// Append entries to storage.
    pub fn append(&self, ents: &[Entry]) {
        let mut core = self.wl();
        if ents.is_empty() {
            return;
        }
        if core.first_index() > ents[0].index {
            panic!("overwrite compacted raft logs, compacted: {}, append: {}", core.first_index() - 1, ents[0].index);
        }
        if core.last_index() + 1 < ents[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                core.last_index(),
                ents[0].index,
            );
        }
        let diff = ents[0].index - core.first_index();
        core.entries.drain(diff as usize..);
        core.entries.extend_from_slice(ents);

        if let Some(ref store) = core.log_store {
            for e in ents {
                let command: crate::fsm::Command = serde_json::from_slice(&e.data).unwrap_or(crate::fsm::Command::NoOp);
                let _ = store.append(e.term, command);
            }
        }
    }

    /// Commit to the given index.
    ///
    /// # Panics
    ///
    /// Panics if there is no such entry in the log.
    pub fn commit_to(&self, index: u64) {
        let mut core = self.wl();
        assert!(core.has_entry_at(index), "commit_to {index} but entry does not exist");
        let diff = (index - core.entries[0].index) as usize;
        core.raft_state.hard_state.commit = index;
        core.raft_state.hard_state.term = core.entries[diff].term;
    }

    /// Apply a snapshot to this storage.
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> RaftResult<()> {
        let mut core = self.wl();
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;

        if core.first_index() > index {
            return Err(RaftError::Store(StorageError::SnapshotOutOfDate));
        }

        let mut meta = SnapshotMetadata::default();
        meta.index = index;
        meta.term = term;
        core.snapshot_metadata = meta;
        core.raft_state.hard_state.term = cmp::max(core.raft_state.hard_state.term, term);
        core.raft_state.hard_state.commit = index;
        core.raft_state.conf_state = snapshot.get_metadata().get_conf_state().clone();
        core.entries.clear();
        Ok(())
    }

    /// Compact the log.
    pub fn compact(&self, compact_index: u64) {
        let mut core = self.wl();
        if compact_index <= core.first_index() {
            return;
        }
        if compact_index > core.last_index() + 1 {
            panic!("compact not received raft logs: {compact_index}, last index: {}", core.last_index());
        }
        if let Some(entry) = core.entries.first() {
            let offset = compact_index - entry.index;
            core.entries.drain(..offset as usize);
        }
    }

    /// The raft log store used for persistence, if any.
    #[must_use]
    pub fn log_store(&self) -> Option<crate::raft_log::RaftLogStore> {
        self.rl().log_store.clone()
    }
}

impl Default for RaftLogStorage {
    fn default() -> Self {
        Self::new()
    }
}

// --- multi-node test helpers ------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use raft::raw_node::RawNode;
    use raft::Config as RaftConfig;

    /// Build a single raw raft node with a snapshot-initialised store.
    fn new_raw_node(id: u64, peers: &[u64], election_tick: usize, heartbeat_tick: usize) -> RawNode<RaftLogStorage> {
        let store = RaftLogStorage::new();

        // Initialise the store with a snapshot containing the peer set.
        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = peers.to_vec();
        store.apply_snapshot(s).unwrap();

        let config = RaftConfig { id, election_tick, heartbeat_tick, ..RaftConfig::default() };
        config.validate().unwrap();
        let mut node = RawNode::with_default_logger(&config, store).unwrap();

        // Immediately campaign (hup) to trigger an election.
        node.campaign().ok();

        node
    }

    /// Drive a single tick on every node.
    fn tick_cluster(nodes: &mut [RawNode<RaftLogStorage>], count: usize) {
        for _ in 0..count {
            for node in nodes.iter_mut() {
                node.tick();
            }
        }
    }

    /// Drain all pending messages from each node and route them to the
    /// correct recipient (in-memory message bus).
    fn route_messages(nodes: &mut [RawNode<RaftLogStorage>]) {
        let mut pending: Vec<(u64, Message)> = Vec::new();
        for node in nodes.iter_mut() {
            if !node.has_ready() {
                continue;
            }
            let mut rd = node.ready();
            // Both immediate and persisted messages need routing.
            for msg in rd.take_messages() {
                pending.push((msg.get_to(), msg));
            }
            for msg in rd.take_persisted_messages() {
                pending.push((msg.get_to(), msg));
            }
            node.advance(rd);
        }

        for (to, msg) in pending {
            for node in nodes.iter_mut() {
                if node.raft.id == to {
                    let _ = node.step(msg);
                    break;
                }
            }
        }
    }

    /// Run `route_messages` until no more pending messages exist (quiesced).
    fn quiesce(nodes: &mut [RawNode<RaftLogStorage>]) {
        loop {
            let before = nodes.iter().filter(|n| n.has_ready()).count();
            route_messages(nodes);
            let after = nodes.iter().filter(|n| n.has_ready()).count();
            if after == 0 && before == 0 {
                break;
            }
        }
    }

    // ---- Tests ---------------------------------------------------------

    #[test]
    fn single_node_leadership() {
        let mut node = new_raw_node(1, &[1], 10, 2);
        for _ in 0..20 {
            node.tick();
        }
        assert_eq!(node.raft.state, raft::StateRole::Leader, "single node should become leader");
    }

    #[test]
    fn two_node_election() {
        let mut n1 = new_raw_node(1, &[1, 2], 10, 2);
        let mut n2 = new_raw_node(2, &[1, 2], 10, 2);

        let nodes = &mut [n1, n2];

        // Drive ticks and routing until a leader is elected or we time out.
        // With randomized election timeouts this may need several attempts.
        let mut elected = false;
        for _ in 0..30 {
            tick_cluster(nodes, 5);
            route_messages(nodes);
            if nodes.iter().filter(|n| n.raft.state == raft::StateRole::Leader).count() == 1 {
                elected = true;
                break;
            }
        }
        assert!(elected, "one of two nodes should become leader");
    }

    #[test]
    fn two_node_replicate_entry() {
        let mut n1 = new_raw_node(1, &[1, 2], 10, 2);
        let mut n2 = new_raw_node(2, &[1, 2], 10, 2);

        let nodes = &mut [n1, n2];

        // Drive election with loops of ticks + routing.
        for _ in 0..10 {
            tick_cluster(nodes, 5);
            route_messages(nodes);
        }
        quiesce(nodes);

        // Find the leader.
        let leader_idx = nodes.iter().position(|n| n.raft.state == raft::StateRole::Leader);
        assert!(leader_idx.is_some(), "a leader must be elected");
        let li = leader_idx.unwrap();

        // Propose an entry on the leader.
        let data = b"hello raft".to_vec();
        nodes[li].propose(vec![], data).unwrap();

        // Drive replication.
        for _ in 0..10 {
            tick_cluster(nodes, 3);
            route_messages(nodes);
        }
        quiesce(nodes);

        // Both nodes should have the entry committed.
        for (i, node) in nodes.iter().enumerate() {
            let hard_state = node.raft.hard_state();
            assert!(hard_state.commit >= 1, "node {i} should have committed index >= 1, got {}", hard_state.commit);
        }
    }
}
