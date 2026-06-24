// SPDX-License-Identifier: Apache-2.0

//! Disk-backed Raft log — JSONL entries + periodic snapshots.
//!
//! The [`RaftLogStore`](crate::raft_log::RaftLogStore) append-commits commands to a
//! newline-delimited JSON file so the cluster can rebuild state after a restart.
//! Snapshot support compacts the log: a snapshot stores the full [`StateStore`]
//! along with the log index/term it covers, and the log is truncated up to that
//! point.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::fsm::Command;
use crate::state::StateStore;

/// A single entry in the replicated Raft log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RaftLogEntry {
    /// Log index (monotonically increasing, 1-based).
    pub index: u64,
    /// Term in which the entry was appended by the leader.
    pub term: u64,
    /// Wall-clock timestamp when the entry was appended.
    pub timestamp: DateTime<Utc>,
    /// The state-machine command to apply when this entry is committed.
    pub command: Command,
}

/// A snapshot of the state machine at a given log position.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RaftSnapshot {
    /// The full state store at snapshot time.
    pub state: StateStore,
    /// The last log index included in this snapshot.
    pub last_included_index: u64,
    /// The term of the last included log entry.
    pub last_included_term: u64,
}

/// Inner mutable state of the log store.
#[derive(Debug)]
struct Inner {
    /// Entries held in memory, indexed by `(vec_pos + 1) == index` so
    /// `entries[0]` has index 1 when `base_index == 1`.
    entries: Vec<RaftLogEntry>,
    /// The log index of the first entry in `entries` (normally 1, or
    /// `snapshot_last_index + 1` after a truncation).
    base_index: u64,
    /// Path to the JSONL log file.
    log_path: PathBuf,
    /// Path to the snapshot file.
    snap_path: PathBuf,
}

/// A thread-safe, disk-backed Raft log.
#[derive(Debug, Clone)]
pub struct RaftLogStore {
    /// Shared mutable log state.
    inner: Arc<std::sync::Mutex<Inner>>,
}

impl RaftLogStore {
    /// Open (or create) a log store rooted at `base_path`.
    ///
    /// If a log file exists at `{base_path}.log` it is loaded into memory on
    /// construction.  If a snapshot exists at `{base_path}.snap` it is applied
    /// first.
    ///
    /// # Errors
    ///
    /// Returns an error if the log or snapshot file cannot be read or parsed.
    pub fn open(base_path: impl AsRef<Path>) -> Result<Self> {
        let log_path = base_path.as_ref().with_extension("log");
        let snap_path = base_path.as_ref().with_extension("snap");

        let mut base_index: u64 = 1;
        let mut entries: Vec<RaftLogEntry> = Vec::new();

        // Load snapshot first so we know where to start indexing.
        if snap_path.exists() {
            let data = std::fs::read_to_string(&snap_path)?;
            let snap: RaftSnapshot = serde_json::from_str(&data)?;
            base_index = snap.last_included_index + 1;
        }

        // Append existing log entries.
        if log_path.exists() {
            let file = std::fs::File::open(&log_path)?;
            let reader = std::io::BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.is_empty() {
                    continue;
                }
                let entry: RaftLogEntry = serde_json::from_str(&line)?;
                entries.push(entry);
            }
        }

        Ok(Self { inner: Arc::new(std::sync::Mutex::new(Inner { entries, base_index, log_path, snap_path })) })
    }

    /// Append a command to the log and persist it to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the disk write fails.
    pub fn append(&self, term: u64, command: Command) -> Result<RaftLogEntry> {
        let entry = RaftLogEntry {
            index: 0, // assigned below
            term,
            timestamp: Utc::now(),
            command,
        };

        let Ok(mut inner) = self.inner.lock() else {
            return Err(crate::error::Error::Runtime("raft log mutex poisoned".to_owned()));
        };

        let index = inner.base_index + inner.entries.len() as u64;

        // Assign index.
        let entry = RaftLogEntry { index, ..entry };

        // Persist to disk.
        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&inner.log_path)?;
        file.write_all(line.as_bytes())?;

        inner.entries.push(entry.clone());
        Ok(entry)
    }

    /// Get an entry by its log index.
    #[must_use]
    pub fn get(&self, index: u64) -> Option<RaftLogEntry> {
        let Ok(inner) = self.inner.lock() else { return None };
        if index < inner.base_index {
            return None;
        }
        let pos = usize::try_from(index - inner.base_index).ok()?;
        inner.entries.get(pos).cloned()
    }

    /// All entries from `from_index` (inclusive) to the end of the log.
    #[must_use]
    pub fn entries_from(&self, from_index: u64) -> Vec<RaftLogEntry> {
        let Ok(inner) = self.inner.lock() else { return Vec::new() };
        let pos = if from_index < inner.base_index {
            return inner.entries.clone();
        } else {
            usize::try_from(from_index - inner.base_index).unwrap_or(0)
        };
        inner.entries.get(pos..).map_or_else(Vec::new, <[_]>::to_vec)
    }

    /// The index of the last entry, or `0` if the log is empty.
    #[must_use]
    pub fn last_index(&self) -> u64 {
        let Ok(inner) = self.inner.lock() else { return 0 };
        if inner.entries.is_empty() {
            return inner.base_index.saturating_sub(1);
        }
        inner.base_index + inner.entries.len() as u64 - 1
    }

    /// The term of the last entry, or `0` if the log is empty.
    #[must_use]
    pub fn last_term(&self) -> u64 {
        let Ok(inner) = self.inner.lock() else { return 0 };
        inner.entries.last().map_or(0, |e| e.term)
    }

    /// The number of entries currently held in memory.
    #[must_use]
    pub fn len(&self) -> usize {
        let Ok(inner) = self.inner.lock() else { return 0 };
        inner.entries.len()
    }

    /// Whether the in-memory log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a snapshot of the current state and truncate the log up to
    /// `last_included_index`.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be written to disk or the log
    /// cannot be truncated.
    pub fn create_snapshot(&self, state: StateStore, last_included_term: u64) -> Result<RaftSnapshot> {
        let Ok(mut inner) = self.inner.lock() else {
            return Err(crate::error::Error::Runtime("raft log mutex poisoned".to_owned()));
        };

        let last_included_index = if inner.entries.is_empty() {
            inner.base_index.saturating_sub(1)
        } else {
            inner.base_index + inner.entries.len() as u64 - 1
        };
        let snapshot = RaftSnapshot { state, last_included_index, last_included_term };

        // Write snapshot to temp file, then rename for atomicity.
        let tmp_path = inner.snap_path.with_extension("snap.tmp");
        let data = serde_json::to_string(&snapshot)?;
        std::fs::write(&tmp_path, &data)?;
        std::fs::rename(&tmp_path, &inner.snap_path)?;

        // Truncate log — keep entries after the snapshot point.
        let truncate_at = if last_included_index >= inner.base_index {
            usize::try_from(last_included_index - inner.base_index + 1).unwrap_or(0)
        } else {
            0
        };
        let truncate_count = truncate_at.min(inner.entries.len());
        inner.entries.drain(..truncate_count);
        inner.base_index = last_included_index + 1;

        // Rewrite the log file without the truncated entries.
        let mut file = std::fs::File::create(&inner.log_path)?;
        for entry in &inner.entries {
            let mut line = serde_json::to_string(entry)?;
            line.push('\n');
            file.write_all(line.as_bytes())?;
        }

        Ok(snapshot)
    }

    /// Replace the current log state with a snapshot (used when a follower
    /// receives a snapshot from the leader).
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be written to disk.
    pub fn install_snapshot(&self, snapshot: &RaftSnapshot) -> Result<()> {
        let Ok(mut inner) = self.inner.lock() else {
            return Err(crate::error::Error::Runtime("raft log mutex poisoned".to_owned()));
        };

        // Write snapshot.
        let tmp_path = inner.snap_path.with_extension("snap.tmp");
        let data = serde_json::to_string(&snapshot)?;
        std::fs::write(&tmp_path, &data)?;
        std::fs::rename(&tmp_path, &inner.snap_path)?;

        // Clear in-memory entries and reset.
        inner.entries.clear();
        inner.base_index = snapshot.last_included_index + 1;

        // Rewrite log file (empty now).
        let _ = std::fs::File::create(&inner.log_path)?;

        Ok(())
    }

    /// Remove all entries at or above `from_index`.
    ///
    /// # Errors
    ///
    /// Returns an error if the log file cannot be rewritten.
    pub fn truncate(&self, from_index: u64) -> Result<()> {
        let Ok(mut inner) = self.inner.lock() else {
            return Err(crate::error::Error::Runtime("raft log mutex poisoned".to_owned()));
        };

        let pos = if from_index >= inner.base_index {
            usize::try_from(from_index - inner.base_index).unwrap_or(usize::MAX)
        } else {
            0
        };

        if pos >= inner.entries.len() {
            return Ok(());
        }

        inner.entries.truncate(pos);

        // Rewrite log file.
        let mut file = std::fs::File::create(&inner.log_path)?;
        for entry in &inner.entries {
            let mut line = serde_json::to_string(entry)?;
            line.push('\n');
            file.write_all(line.as_bytes())?;
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::jobspec::Job;

    fn test_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("nomad_raft_{}_{}", std::process::id(), name));
        std::fs::create_dir_all(&dir).ok();
        dir.join("raft")
    }

    fn cleanup(path: &Path) {
        for ext in &["log", "snap", "snap.tmp"] {
            std::fs::remove_file(path.with_extension(ext)).ok();
        }
    }

    fn make_cmd() -> Command {
        Command::UpsertJob(Job { name: "redis".to_owned(), priority: 50, ..Job::default() })
    }

    #[test]
    fn new_store_has_empty_log() {
        let path = test_path("new");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.last_index(), 0);
        assert_eq!(store.last_term(), 0);
        cleanup(&path);
    }

    #[test]
    fn append_increases_len() {
        let path = test_path("append");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        store.append(1, make_cmd()).unwrap();
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
        cleanup(&path);
    }

    #[test]
    fn get_returns_entry() {
        let path = test_path("get");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        let entry = store.append(1, make_cmd()).unwrap();
        let fetched = store.get(entry.index).unwrap();
        assert_eq!(fetched.index, entry.index);
        assert_eq!(fetched.term, 1);
        cleanup(&path);
    }

    #[test]
    fn get_missing_returns_none() {
        let path = test_path("get_miss");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        assert!(store.get(42).is_none());
        cleanup(&path);
    }

    #[test]
    fn entries_from_returns_suffix() {
        let path = test_path("entries_from");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        store.append(1, make_cmd()).unwrap();
        store.append(1, make_cmd()).unwrap();
        store.append(2, make_cmd()).unwrap();
        let suffix = store.entries_from(2);
        assert_eq!(suffix.len(), 2);
        assert_eq!(suffix[0].index, 2);
        assert_eq!(suffix[1].index, 3);
        cleanup(&path);
    }

    #[test]
    fn last_term_returns_last() {
        let path = test_path("last_term");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        store.append(1, make_cmd()).unwrap();
        store.append(5, make_cmd()).unwrap();
        assert_eq!(store.last_term(), 5);
        cleanup(&path);
    }

    #[test]
    fn create_and_install_snapshot() {
        let path = test_path("snapshot");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        store.append(1, make_cmd()).unwrap();
        store.append(1, make_cmd()).unwrap();

        let state = StateStore::new();
        let snap = store.create_snapshot(state, 1).unwrap();
        assert!(snap.last_included_index >= 2);
        // Entries should be truncated.
        assert!(store.is_empty());

        // Re-open — snapshot means no log entries.
        let store2 = RaftLogStore::open(&path).unwrap();
        assert!(store2.is_empty());
        cleanup(&path);
    }

    #[test]
    fn persistence_round_trip() {
        let path = test_path("persist");
        cleanup(&path);
        {
            let store = RaftLogStore::open(&path).unwrap();
            store.append(1, make_cmd()).unwrap();
            store.append(1, make_cmd()).unwrap();
            store.append(2, make_cmd()).unwrap();
        }
        let store2 = RaftLogStore::open(&path).unwrap();
        assert_eq!(store2.len(), 3);
        assert_eq!(store2.last_term(), 2);
        cleanup(&path);
    }

    #[test]
    fn truncate_removes_from_index() {
        let path = test_path("trunc");
        cleanup(&path);
        let store = RaftLogStore::open(&path).unwrap();
        store.append(1, make_cmd()).unwrap();
        store.append(1, make_cmd()).unwrap();
        store.append(1, make_cmd()).unwrap();

        store.truncate(2).unwrap();
        assert_eq!(store.len(), 1);
        assert!(store.get(2).is_none());
        assert!(store.get(1).is_some());
        cleanup(&path);
    }
}
