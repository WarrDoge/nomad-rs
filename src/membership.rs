// SPDX-License-Identifier: Apache-2.0

//! Cluster membership / gossip — Apache-clean, in-tree.
//!
//! [`GossipMembership`] is a SWIM-lite implementation over tokio UDP: nodes
//! exchange their full roster on `join` (push-pull) and disseminate status
//! changes (e.g. a graceful `leave`) via gossip datagrams. Conflicting updates
//! about the same member are ordered by a per-member incarnation number; a node
//! never accepts a peer's claim about *itself*.
//!
//! Not yet implemented (deliberate, see `ponytail:` notes): periodic
//! ping/ack failure detection (Suspect → Failed), indirect probes, and
//! incarnation-based self-refutation. The wire format is JSON over UDP — swap
//! for a compact codec if rosters outgrow a datagram.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

use crate::error::{Error, Result};

/// Liveness of a cluster member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberStatus {
    /// Reachable and healthy.
    Alive,
    /// Gracefully leaving.
    Leaving,
    /// Has left the cluster.
    Left,
    /// Unreachable / failed.
    Failed,
}

/// A member of the cluster.
#[derive(Debug, Clone)]
pub struct Member {
    /// Member name.
    pub name: String,
    /// Advertised gossip address (`host:port`).
    pub addr: String,
    /// Current liveness.
    pub status: MemberStatus,
}

/// A roster entry: the member plus the incarnation used to order updates.
#[derive(Debug, Clone)]
struct Entry {
    /// The member's public-facing record.
    member: Member,
    /// Monotonic counter; higher wins when reconciling conflicting reports.
    incarnation: u64,
}

/// One member as it travels on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Wire {
    /// Member name.
    name: String,
    /// Advertised address.
    addr: String,
    /// Liveness.
    status: MemberStatus,
    /// Incarnation for conflict resolution.
    incarnation: u64,
}

/// A single UDP datagram exchanged between members.
#[derive(Debug, Serialize, Deserialize)]
enum Msg {
    /// Full-roster exchange. The receiver merges `peers`; if `reply` is set it
    /// answers with its own roster (`reply: false`).
    PushPull {
        /// Sender's known roster.
        peers: Vec<Wire>,
        /// Whether the receiver should answer with its own roster.
        reply: bool,
    },
    /// One-way dissemination of roster changes; never answered.
    Gossip {
        /// Updated member records.
        peers: Vec<Wire>,
    },
}

/// The in-tree gossip-based membership.
#[derive(Debug)]
pub struct GossipMembership {
    /// This member's name.
    name: String,
    /// Bound UDP socket, shared with the receive loop.
    socket: Arc<UdpSocket>,
    /// Shared roster, keyed by member name.
    state: Arc<Mutex<HashMap<String, Entry>>>,
    /// This node's own incarnation; bumped to advertise self-status changes.
    incarnation: Arc<AtomicU64>,
}

/// Lock the roster, recovering from a poisoned mutex (a panicked holder still
/// leaves the membership map structurally intact).
fn lock(state: &Mutex<HashMap<String, Entry>>) -> MutexGuard<'_, HashMap<String, Entry>> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Merge `peers` into the roster, ignoring any claim about `self_name` and any
/// update whose incarnation does not beat what we already hold.
fn merge(state: &Mutex<HashMap<String, Entry>>, self_name: &str, peers: Vec<Wire>) {
    let mut roster = lock(state);
    for w in peers {
        if w.name == self_name {
            continue;
        }
        let supersedes = roster.get(&w.name).is_none_or(|e| w.incarnation > e.incarnation);
        if supersedes {
            roster.insert(
                w.name.clone(),
                Entry { member: Member { name: w.name, addr: w.addr, status: w.status }, incarnation: w.incarnation },
            );
        }
    }
}

/// Snapshot the roster as wire records.
fn snapshot(state: &Mutex<HashMap<String, Entry>>) -> Vec<Wire> {
    lock(state)
        .values()
        .map(|e| Wire {
            name: e.member.name.clone(),
            addr: e.member.addr.clone(),
            status: e.member.status,
            incarnation: e.incarnation,
        })
        .collect()
}

/// Receive loop: services push-pull exchanges and gossip until the socket dies.
async fn recv_loop(name: String, socket: Arc<UdpSocket>, state: Arc<Mutex<HashMap<String, Entry>>>) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let Ok((n, src)) = socket.recv_from(&mut buf).await else { continue };
        let Ok(msg) = serde_json::from_slice::<Msg>(&buf[..n]) else { continue };
        match msg {
            Msg::PushPull { peers, reply } => {
                merge(&state, &name, peers);
                if reply {
                    let out = serde_json::to_vec(&Msg::PushPull { peers: snapshot(&state), reply: false })
                        .unwrap_or_default();
                    let _ = socket.send_to(&out, src).await;
                }
            },
            Msg::Gossip { peers } => merge(&state, &name, peers),
        }
    }
}

impl GossipMembership {
    /// Bind a membership endpoint named `name` to `bind` (e.g. `127.0.0.1:0`)
    /// and start servicing gossip in the background.
    ///
    /// # Errors
    ///
    /// Returns an error if the UDP socket cannot be bound.
    pub async fn start(name: &str, bind: &str) -> Result<Self> {
        let socket = Arc::new(UdpSocket::bind(bind).await?);
        let addr = socket.local_addr()?.to_string();
        let mut roster = HashMap::new();
        roster.insert(
            name.to_owned(),
            Entry { member: Member { name: name.to_owned(), addr, status: MemberStatus::Alive }, incarnation: 0 },
        );
        let state = Arc::new(Mutex::new(roster));
        tokio::spawn(recv_loop(name.to_owned(), Arc::clone(&socket), Arc::clone(&state)));
        Ok(Self { name: name.to_owned(), socket, state, incarnation: Arc::new(AtomicU64::new(0)) })
    }

    /// The advertised gossip address of this node.
    #[must_use]
    pub fn local_addr(&self) -> String {
        self.socket.local_addr().map(|a| a.to_string()).unwrap_or_default()
    }

    /// Join the cluster by push-pulling our roster against one or more peer
    /// addresses; returns the number of peers the datagram reached.
    ///
    /// # Errors
    ///
    /// Returns an error if `addrs` is non-empty but no peer could be reached.
    pub async fn join(&self, addrs: &[String]) -> Result<usize> {
        let payload = serde_json::to_vec(&Msg::PushPull { peers: snapshot(&self.state), reply: true })?;
        let mut reached = 0;
        for addr in addrs {
            if self.socket.send_to(&payload, addr.as_str()).await.is_ok() {
                reached += 1;
            }
        }
        if reached == 0 && !addrs.is_empty() {
            return Err(Error::Runtime("membership join: no peers reachable".to_owned()));
        }
        // ponytail: poll until the roster grows past self (1s ceiling). Replace
        // with an acked join if a hard convergence guarantee is ever needed.
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if lock(&self.state).len() > 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(reached)
    }

    /// The currently known members.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        lock(&self.state).values().map(|e| e.member.clone()).collect()
    }

    /// Gracefully leave the cluster: mark ourselves `Left` and gossip the change
    /// to every known member.
    ///
    /// # Errors
    ///
    /// Currently infallible; returns `Result` for API stability as failure
    /// detection lands.
    pub async fn leave(&self) -> Result<()> {
        let inc = self.incarnation.fetch_add(1, Ordering::SeqCst) + 1;
        let targets: Vec<String> = {
            let mut roster = lock(&self.state);
            if let Some(e) = roster.get_mut(&self.name) {
                e.member.status = MemberStatus::Left;
                e.incarnation = inc;
            }
            roster.values().filter(|e| e.member.name != self.name).map(|e| e.member.addr.clone()).collect()
        };
        let payload = serde_json::to_vec(&Msg::Gossip { peers: snapshot(&self.state) })?;
        for addr in targets {
            let _ = self.socket.send_to(&payload, addr.as_str()).await;
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use std::collections::HashSet;

    async fn node(name: &str) -> GossipMembership {
        GossipMembership::start(name, "127.0.0.1:0").await.unwrap()
    }

    fn names(m: &GossipMembership) -> HashSet<String> {
        m.members().into_iter().map(|x| x.name).collect()
    }

    /// Poll `m` until `pred` holds or 1s elapses; returns whether it held.
    async fn eventually(m: &GossipMembership, pred: impl Fn(&GossipMembership) -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if pred(m) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        pred(m)
    }

    #[tokio::test]
    async fn fresh_node_lists_only_itself() {
        let n = node("s1").await;
        assert_eq!(names(&n), HashSet::from(["s1".to_owned()]));
    }

    #[tokio::test]
    async fn two_nodes_discover_each_other_after_join() {
        let n1 = node("s1").await;
        let n2 = node("s2").await;
        n2.join(&[n1.local_addr()]).await.unwrap();
        let both = HashSet::from(["s1".to_owned(), "s2".to_owned()]);
        assert_eq!(names(&n2), both, "joiner learns the peer");
        assert!(eventually(&n1, |m| names(m) == both).await, "peer learns the joiner");
    }

    #[tokio::test]
    async fn join_reports_peers_reached() {
        let n1 = node("s1").await;
        let n2 = node("s2").await;
        assert_eq!(n2.join(&[n1.local_addr()]).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn join_with_no_reachable_peer_errors() {
        let n = node("s1").await;
        // Reserved-for-documentation address that drops: unreachable but the
        // send itself fails to resolve/route quickly enough to count as 0.
        let bad = "256.256.256.256:1".to_owned();
        assert!(n.join(&[bad]).await.is_err());
    }

    #[tokio::test]
    async fn leave_propagates_left_status_to_peer() {
        let n1 = node("s1").await;
        let n2 = node("s2").await;
        n2.join(&[n1.local_addr()]).await.unwrap();
        assert!(eventually(&n1, |m| m.members().iter().any(|x| x.name == "s2")).await);

        n2.leave().await.unwrap();
        let saw_left =
            eventually(&n1, |m| m.members().iter().any(|x| x.name == "s2" && x.status == MemberStatus::Left)).await;
        assert!(saw_left, "peer learns s2 has Left via gossip");
    }
}
