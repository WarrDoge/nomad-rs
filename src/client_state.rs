// SPDX-License-Identifier: Apache-2.0

//! Persistent client-local state for allocations and task runners.
//!
//! Uses Turso's `libsql` (pure Rust SQLite rewrite via `rusqlite` with bundled
//! sqlite3 — builds without any system C dependency) as the backing store. The
//! schema tracks allocations assigned to this node and their task-runner states
//! so that the client agent can recover after a restart.

use std::path::Path;

use rusqlite::Connection;

use crate::alloc::{Allocation, ClientStatus, DesiredStatus};
use crate::driver::TaskState;
use crate::error::Result;
use crate::jobspec::Resources;

/// Persistent client-level state backed by `rusqlite` with bundled sqlite3.
#[derive(Debug)]
pub struct ClientState {
    /// `rusqlite` database connection handle.
    conn: Connection,
}

impl ClientState {
    /// Open (or create) the database at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the schema
    /// migration fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS allocations (
                id          TEXT PRIMARY KEY,
                eval_id     TEXT NOT NULL,
                node_id     TEXT NOT NULL,
                job_id      TEXT NOT NULL,
                task_group  TEXT NOT NULL,
                desired_status TEXT NOT NULL DEFAULT 'run',
                client_status TEXT NOT NULL DEFAULT 'pending',
                cpu_mhz     INTEGER NOT NULL DEFAULT 0,
                memory_mb   INTEGER NOT NULL DEFAULT 0,
                network_mbps INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS task_states (
                alloc_id    TEXT NOT NULL,
                task_name   TEXT NOT NULL,
                state       TEXT NOT NULL DEFAULT 'pending',
                restart_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (alloc_id, task_name),
                FOREIGN KEY (alloc_id) REFERENCES allocations(id)
            );",
        )?;
        Ok(Self { conn })
    }

    /// Upsert an allocation's metadata. Inserts or replaces by id.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub fn upsert_alloc(&self, alloc: &Allocation) -> Result<()> {
        let ds = alloc.desired_status.as_str();
        let cs = alloc.client_status.as_str();
        self.conn.execute(
            "INSERT INTO allocations (id, eval_id, node_id, job_id, task_group, desired_status, client_status, cpu_mhz, memory_mb, network_mbps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(id) DO UPDATE SET
                eval_id=excluded.eval_id, node_id=excluded.node_id, job_id=excluded.job_id,
                task_group=excluded.task_group, desired_status=excluded.desired_status,
                client_status=excluded.client_status,
                cpu_mhz=excluded.cpu_mhz, memory_mb=excluded.memory_mb, network_mbps=excluded.network_mbps",
            rusqlite::params![
                alloc.id, alloc.eval_id, alloc.node_id, alloc.job_id, alloc.task_group,
                ds, cs,
                alloc.resources.cpu_mhz, alloc.resources.memory_mb, alloc.resources.network_mbps,
            ],
        )?;
        Ok(())
    }

    /// Load all allocations stored for this client.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read fails.
    pub fn list_allocs(&self) -> Result<Vec<Allocation>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, eval_id, node_id, job_id, task_group, desired_status, client_status, cpu_mhz, memory_mb, network_mbps
             FROM allocations",
        )?;
        let rows = stmt.query_map([], |row| {
            let ds: String = row.get(5)?;
            let cs: String = row.get(6)?;
            Ok(Allocation {
                id: row.get(0)?,
                eval_id: row.get(1)?,
                node_id: row.get(2)?,
                job_id: row.get(3)?,
                task_group: row.get(4)?,
                desired_status: parse_desired_status(&ds),
                client_status: parse_client_status(&cs),
                resources: Resources { cpu_mhz: row.get(7)?, memory_mb: row.get(8)?, network_mbps: row.get(9)? },
            })
        })?;
        let mut allocs = Vec::new();
        for row in rows {
            allocs.push(row?);
        }
        Ok(allocs)
    }

    /// Set the task state for a given alloc + task name.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub fn set_task_state(&self, alloc_id: &str, task_name: &str, state: TaskState, restart_count: u32) -> Result<()> {
        let state_str = match state {
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Exited => "exited",
            TaskState::Unknown => "unknown",
        };
        self.conn.execute(
            "INSERT INTO task_states (alloc_id, task_name, state, restart_count)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(alloc_id, task_name) DO UPDATE SET
                state=excluded.state, restart_count=excluded.restart_count",
            rusqlite::params![alloc_id, task_name, state_str, restart_count],
        )?;
        Ok(())
    }

    /// Remove all state for a completed allocation.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    pub fn delete_alloc(&self, alloc_id: &str) -> Result<()> {
        self.conn.execute("DELETE FROM task_states WHERE alloc_id = ?1", rusqlite::params![alloc_id])?;
        self.conn.execute("DELETE FROM allocations WHERE id = ?1", rusqlite::params![alloc_id])?;
        Ok(())
    }
}

/// Parse a desired status string from the database.
fn parse_desired_status(s: &str) -> DesiredStatus {
    match s {
        "stop" => DesiredStatus::Stop,
        "evict" => DesiredStatus::Evict,
        _ => DesiredStatus::Run,
    }
}

/// Parse a client status string from the database.
fn parse_client_status(s: &str) -> ClientStatus {
    match s {
        "running" => ClientStatus::Running,
        "complete" => ClientStatus::Complete,
        "failed" => ClientStatus::Failed,
        "lost" => ClientStatus::Lost,
        _ => ClientStatus::Pending,
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn test_alloc(id: &str) -> Allocation {
        Allocation {
            id: id.to_owned(),
            eval_id: "e1".to_owned(),
            node_id: "n1".to_owned(),
            job_id: "redis".to_owned(),
            task_group: "cache".to_owned(),
            desired_status: DesiredStatus::Run,
            client_status: ClientStatus::Running,
            resources: Resources::default(),
        }
    }

    fn state() -> ClientState {
        ClientState::open(":memory:").unwrap()
    }

    #[test]
    fn open_in_memory_succeeds() {
        let cs = state();
        assert!(cs.list_allocs().unwrap().is_empty());
    }

    #[test]
    fn upsert_then_list_includes_alloc() {
        let cs = state();
        cs.upsert_alloc(&test_alloc("a1")).unwrap();
        let allocs = cs.list_allocs().unwrap();
        assert_eq!(allocs.len(), 1);
        assert_eq!(allocs[0].id, "a1");
    }

    #[test]
    fn upsert_replaces_existing() {
        let cs = state();
        cs.upsert_alloc(&test_alloc("a1")).unwrap();
        let mut a = test_alloc("a1");
        a.client_status = ClientStatus::Complete;
        cs.upsert_alloc(&a).unwrap();
        let allocs = cs.list_allocs().unwrap();
        assert_eq!(allocs.len(), 1);
        assert_eq!(allocs[0].client_status, ClientStatus::Complete);
    }

    #[test]
    fn set_task_state_round_trips() {
        let cs = state();
        cs.upsert_alloc(&test_alloc("a1")).unwrap();
        cs.set_task_state("a1", "web", TaskState::Running, 0).unwrap();
        // Read back via raw SQL to verify the write was persisted.
        let mut stmt = cs
            .conn
            .prepare("SELECT COUNT(*) FROM task_states WHERE alloc_id = ?1 AND task_name = ?2 AND state = ?3")
            .unwrap();
        let count: i64 = stmt.query_row(rusqlite::params!["a1", "web", "running"], |row| row.get(0)).unwrap();
        assert_eq!(count, 1, "task state should be persisted");
    }

    #[test]
    fn delete_alloc_removes_it() {
        let cs = state();
        cs.upsert_alloc(&test_alloc("a1")).unwrap();
        cs.delete_alloc("a1").unwrap();
        assert!(cs.list_allocs().unwrap().is_empty());
    }
}
