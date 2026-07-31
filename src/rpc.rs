// SPDX-License-Identifier: Apache-2.0

//! RPC — dependency-agnostic.
//!
//! Defines the request/response surface servers expose to nodes and each other,
//! processed by the in-tree [`RpcEndpoint`](crate::rpc::RpcEndpoint) (forwarding writes
//! to the leader). [`RpcServer`](crate::rpc::RpcServer)/[`RpcClient`](crate::rpc::RpcClient)
//! carry [`Request`](crate::rpc::Request)/[`Response`](crate::rpc::Response)
//! over a length-prefixed JSON frame on a tokio TCP stream; mTLS is layered by
//! wrapping the stream (see [`crate::tls`]).

use crate::error::{Error, Result};
use crate::eval::{EvalStatus, EvalTrigger, Evaluation};
use crate::eval_queue::EvalQueue;
use crate::fsm::Command;
use crate::id::EvalId;
use crate::jobspec::Job;
use crate::node::Node;
use crate::raft::RaftNode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsStream;
use tokio_rustls::client::TlsConnector;

/// Hard cap on a single RPC frame (8 MiB). The length prefix is network-supplied
/// and untrusted; a claim larger than this is rejected rather than allocated, so
/// a malicious or corrupt peer cannot induce a huge buffer allocation.
const MAX_FRAME: usize = 8 * 1024 * 1024;

/// A request a server can handle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Register or update a job.
    JobRegister(Job),
    /// Deregister the job with the given name.
    JobDeregister(String),
    /// Register or update a client node.
    NodeRegister(Node),
    /// Dequeue a pending evaluation for the given scheduler types.
    EvalDequeue {
        /// Scheduler types the worker can handle (e.g. `["service","batch"]`).
        schedulers: Vec<String>,
    },
    /// Heartbeat from a client node to the server.
    NodeHeartbeat {
        /// The client node's identifier.
        node_id: crate::id::NodeId,
    },
    /// Request all allocations placed on a given node.
    NodeGetAllocs {
        /// The node whose allocations to return.
        node_id: crate::id::NodeId,
    },
}

/// A response to a [`Request`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// A job was registered; an evaluation was created.
    JobRegistered {
        /// Id of the evaluation created for the registration.
        eval_id: EvalId,
    },
    /// The request was applied with no payload.
    Ack,
    /// The dequeued evaluation, if any was available.
    Eval(Option<Evaluation>),
    /// This node is not the leader; the caller should forward to `leader_addr`.
    NotLeader {
        /// Address of the current leader, if known.
        leader_addr: Option<String>,
    },
    /// All allocations for a requested node.
    NodeAllocs {
        /// Allocations assigned to the node.
        allocs: Vec<crate::alloc::Allocation>,
    },
}

/// The in-tree RPC handler.
///
/// Writes are committed through the local [`RaftNode`] (so they land in the
/// FSM-backed state), then any follow-up eval is enqueued. On a follower, a
/// write returns [`Response::NotLeader`].
#[derive(Debug)]
pub struct RpcEndpoint {
    /// Priority eval queue shared with the scheduler loop.
    eval_queue: EvalQueue,
    /// Consensus node writes are committed through.
    raft: Arc<Mutex<RaftNode>>,
}

impl RpcEndpoint {
    /// Create an endpoint with its own single-node bootstrap leader.
    #[must_use]
    pub fn new(eval_queue: EvalQueue) -> Self {
        Self { eval_queue, raft: Arc::new(Mutex::new(RaftNode::bootstrap("rpc-local"))) }
    }

    /// Create an endpoint wired to an existing consensus node.
    #[must_use]
    pub const fn with_raft(eval_queue: EvalQueue, raft: Arc<Mutex<RaftNode>>) -> Self {
        Self { eval_queue, raft }
    }

    /// The consensus node this endpoint commits through.
    #[must_use]
    pub fn raft(&self) -> Arc<Mutex<RaftNode>> {
        Arc::clone(&self.raft)
    }

    /// Commit a write command through consensus.
    ///
    /// Returns `Ok(Some(NotLeader))` if this node is a follower (caller should
    /// forward), or `Ok(None)` once the command is committed and applied.
    fn commit(&self, command: Command) -> Result<Option<Response>> {
        let raft = self.raft.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !raft.is_leader() {
            return Ok(Some(Response::NotLeader { leader_addr: raft.leader_addr() }));
        }
        drop(raft); // release before propose to avoid holding lock
        self.raft.lock().unwrap_or_else(std::sync::PoisonError::into_inner).propose(command)?;
        Ok(None)
    }

    /// Handle a request and produce a response.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid, the node cannot reach the
    /// leader, or the underlying state operation fails.
    pub fn handle(&self, request: Request) -> Result<Response> {
        match request {
            Request::JobRegister(job) => {
                job.validate()?;
                let (name, priority) = (job.name.clone(), job.priority);
                // Commit the job through consensus first; bail with NotLeader
                // on a follower so the caller can forward.
                if let Some(resp) = self.commit(Command::UpsertJob(job))? {
                    return Ok(resp);
                }
                let eval_id = eval_id_for(&name);
                let eval = Evaluation {
                    id: eval_id.clone(),
                    job_id: name.into(),
                    priority,
                    trigger: EvalTrigger::JobRegister,
                    status: EvalStatus::Pending,
                };
                self.eval_queue.enqueue(eval)?;
                Ok(Response::JobRegistered { eval_id })
            },
            Request::JobDeregister(name) => {
                if let Some(resp) = self.commit(Command::DeregisterJob(name.clone()))? {
                    return Ok(resp);
                }
                // Enqueue a cleanup eval so the scheduler stops the allocs.
                self.eval_queue.enqueue(Evaluation {
                    id: eval_id_for(&name),
                    job_id: name.into(),
                    priority: 50,
                    trigger: EvalTrigger::JobDeregister,
                    status: EvalStatus::Pending,
                })?;
                Ok(Response::Ack)
            },
            Request::NodeRegister(node) => {
                if let Some(resp) = self.commit(Command::UpsertNode(node))? {
                    return Ok(resp);
                }
                // New/updated node may have freed capacity — re-try blocked evals.
                self.eval_queue.unblock_all()?;
                Ok(Response::Ack)
            },
            Request::EvalDequeue { schedulers: _ } => {
                // Only one scheduler type exists at this stage (service), so
                // the type filter is a no-op. Once batch/system/sysbatch types
                // land, filter self.eval_queue.dequeue() by the requested
                // schedulers BEFORE popping from the heap — otherwise the
                // wrong scheduler type burns an eval meant for another.
                Ok(Response::Eval(self.eval_queue.dequeue()?))
            },
            Request::NodeHeartbeat { node_id } => {
                let raft = self.raft.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                drop(raft.heartbeat(&node_id));
                Ok(Response::Ack)
            },
            Request::NodeGetAllocs { node_id } => {
                let raft = self.raft.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let allocs: Vec<_> = raft.state().list_allocs().into_iter().filter(|a| a.node_id == node_id).collect();
                Ok(Response::NodeAllocs { allocs })
            },
        }
    }
}

/// A non-deterministic eval id (nanosecond timestamp); tests must not assert
/// on its exact value.
fn eval_id_for(job_name: &str) -> EvalId {
    format!(
        "eval-{}-{}",
        job_name,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
    )
    .into()
}

/// Write `msg` as a length-prefixed JSON frame: a 4-byte big-endian length
/// followed by that many bytes of JSON.
///
/// # Errors
///
/// Returns an error if serialisation fails, the frame exceeds [`MAX_FRAME`], or
/// the underlying write fails.
async fn write_frame<W>(w: &mut W, msg: &impl Serialize) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    let bytes = serde_json::to_vec(msg)?;
    if bytes.len() > MAX_FRAME {
        return Err(Error::Runtime("rpc frame exceeds maximum size".to_owned()));
    }
    let len = u32::try_from(bytes.len()).map_err(|_| Error::Runtime("rpc frame length overflow".to_owned()))?;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Read one length-prefixed JSON frame. Returns `Ok(None)` on a clean
/// end-of-stream before any bytes of a new frame (peer closed the connection).
///
/// # Errors
///
/// Returns an error on a partial frame, a length over [`MAX_FRAME`], or a
/// deserialisation failure.
async fn read_frame<R, T>(r: &mut R) -> Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    if let Err(e) = r.read_exact(&mut len_buf).await {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e.into());
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(Error::Runtime("rpc frame exceeds maximum size".to_owned()));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(Some(serde_json::from_slice(&buf)?))
}

// ---- mTLS-capable transport -----------------------------------------------

/// Internal transport: plain TCP or mTLS-encrypted.
enum Transport {
    /// Plain TCP connection.
    Plain(TcpStream),
    /// mTLS-wrapped connection.
    Tls(Box<TlsStream<TcpStream>>),
}

// ---- server side ---------------------------------------------------------

/// A TCP RPC server: accepts connections and dispatches framed [`Request`]s
/// through a shared [`RpcEndpoint`], writing back each [`Response`].
///
/// When a TLS acceptor is configured, accepted connections are wrapped in mTLS
/// before serving. Plain TCP is used when the acceptor is `None` (backward
/// compatible).
pub struct RpcServer {
    /// The endpoint requests are dispatched through.
    endpoint: Arc<RpcEndpoint>,
    /// Optional mTLS acceptor. When set, every accepted connection is wrapped
    /// in a rustls server-side session before reading requests.
    tls_acceptor: Option<TlsAcceptor>,
}

impl std::fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcServer")
            .field("endpoint", &self.endpoint)
            .field("tls_acceptor", &self.tls_acceptor.as_ref().map(|_| "<config>"))
            .finish()
    }
}

impl RpcServer {
    /// Create a server over the given endpoint, without TLS.
    #[must_use]
    pub const fn new(endpoint: Arc<RpcEndpoint>) -> Self {
        Self { endpoint, tls_acceptor: None }
    }

    /// Attach an mTLS acceptor. When set, every accepted connection is wrapped
    /// in TLS before dispatching.
    #[must_use]
    pub fn with_tls(mut self, tls: TlsAcceptor) -> Self {
        self.tls_acceptor = Some(tls);
        self
    }

    /// Accept connections on `listener` forever, serving each on its own task.
    /// Loops until the task is dropped/aborted by the caller.
    ///
    /// # Errors
    ///
    /// Returns an error if accepting a connection fails fatally.
    pub async fn serve(&self, listener: TcpListener) -> Result<()> {
        loop {
            let (stream, _peer) = listener.accept().await?;
            let endpoint = Arc::clone(&self.endpoint);
            let tls = self.tls_acceptor.clone();
            tokio::spawn(async move {
                let transport = match tls {
                    Some(acceptor) => match acceptor.accept(stream).await {
                        Ok(tls_stream) => Transport::Tls(Box::new(TlsStream::Server(tls_stream))),
                        Err(e) => {
                            tracing::warn!("mTLS handshake failed: {e}");
                            return;
                        },
                    },
                    None => Transport::Plain(stream),
                };
                serve_conn(transport, endpoint).await;
            });
        }
    }
}

/// Serve a single connection: read requests until the peer closes, dispatching
/// each through `endpoint`. A handler error closes the connection.
///
/// ponytail: a handler error closes the conn rather than returning a typed error
/// frame — add a `Response::Error` variant if clients need the failure reason.
async fn serve_conn(mut transport: Transport, endpoint: Arc<RpcEndpoint>) {
    loop {
        let req = match transport {
            Transport::Plain(ref mut s) => match read_frame::<_, Request>(s).await {
                Ok(Some(r)) => r,
                _ => return,
            },
            Transport::Tls(ref mut s) => match read_frame::<_, Request>(s.as_mut()).await {
                Ok(Some(r)) => r,
                _ => return,
            },
        };
        let Ok(resp) = endpoint.handle(req) else { return };
        let ok = match transport {
            Transport::Plain(ref mut s) => write_frame(s, &resp).await.is_ok(),
            Transport::Tls(ref mut s) => write_frame(s.as_mut(), &resp).await.is_ok(),
        };
        if !ok {
            return;
        }
    }
}

// ---- client side ---------------------------------------------------------

/// A TCP RPC client: one connection, one request/response at a time.
///
/// When a `rustls::ClientConfig` is provided via [`connect_tls`](Self::connect_tls),
/// the TCP stream is wrapped in mTLS before exchanging frames.
/// On receiving `NotLeader`, the client automatically reconnects to the
/// leader and retries once (transparent to callers).
pub struct RpcClient {
    /// The inner transport — either a raw TCP stream or an mTLS-wrapped one.
    transport: Transport,
    /// Saved TLS connector + server name for reconnection on `NotLeader`.
    tls_state: Option<(TlsConnector, rustls::pki_types::ServerName<'static>)>,
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field(
                "transport",
                &match self.transport {
                    Transport::Plain(_) => "Plain",
                    Transport::Tls(_) => "Tls",
                },
            )
            .field("tls_state", &self.tls_state.as_ref().map(|_| "<config>"))
            .finish()
    }
}

impl RpcClient {
    /// Connect to a server at `addr` (`host:port`) with plain TCP.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub async fn connect(addr: &str) -> Result<Self> {
        Self::connect_tls(addr, None).await
    }

    /// Connect with optional mTLS from a pre-built [`rustls::ClientConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP connection or TLS handshake fails.
    pub async fn connect_tls(addr: &str, tls_config: Option<Arc<rustls::ClientConfig>>) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (transport, tls_state) = match tls_config {
            Some(cfg) => {
                let connector = TlsConnector::from(cfg);
                let host = addr.split(':').next().unwrap_or("localhost");
                let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                    .map_err(|e| Error::Runtime(format!("invalid server name for TLS: {e}")))?;
                let tls_stream = connector
                    .connect(name.clone(), stream)
                    .await
                    .map_err(|e| Error::Runtime(format!("mTLS handshake failed: {e}")))?;
                (Transport::Tls(Box::new(TlsStream::Client(tls_stream))), Some((connector, name)))
            },
            None => (Transport::Plain(stream), None),
        };
        Ok(Self { transport, tls_state })
    }

    /// Send `request` and await the server's response.
    ///
    /// If the server responds with `NotLeader`, the client automatically
    /// reconnects to the leader address and retries the request once. This
    /// is transparent to callers.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails, the connection closes before a
    /// response, or a frame is malformed. Returns an error immediately when
    /// the `NotLeader` response carries no leader address.
    pub async fn call(&mut self, request: &Request) -> Result<Response> {
        // Write request.
        match self.transport {
            Transport::Plain(ref mut s) => write_frame(s, request).await?,
            Transport::Tls(ref mut s) => write_frame(s.as_mut(), request).await?,
        }
        // Read response.
        let resp = match self.transport {
            Transport::Plain(ref mut s) => read_frame(s).await,
            Transport::Tls(ref mut s) => read_frame(s.as_mut()).await,
        }
        .unwrap_or(None)
        .ok_or_else(|| Error::Runtime("rpc connection closed by server".to_owned()))?;

        // Auto-forward on NotLeader — reconnect to the leader and retry once.
        if let Response::NotLeader { ref leader_addr } = resp {
            let addr = match leader_addr {
                Some(a) => a.clone(),
                None => return Ok(resp), // no address to forward to; pass through
            };
            tracing::debug!("not-leader response, reconnecting to {addr}");
            if let Some((ref connector, ref name)) = self.tls_state {
                let stream = TcpStream::connect(addr.as_str()).await?;
                let tls_stream = connector
                    .connect(name.clone(), stream)
                    .await
                    .map_err(|e| Error::Runtime(format!("mTLS handshake with leader {addr} failed: {e}")))?;
                self.transport = Transport::Tls(Box::new(TlsStream::Client(tls_stream)));
            } else {
                self.transport = Transport::Plain(TcpStream::connect(addr.as_str()).await?);
            }
            // Retry once on the new connection.
            match self.transport {
                Transport::Plain(ref mut s) => write_frame(s, request).await?,
                Transport::Tls(ref mut s) => write_frame(s.as_mut(), request).await?,
            }
            return match self.transport {
                Transport::Plain(ref mut s) => read_frame(s).await,
                Transport::Tls(ref mut s) => read_frame(s.as_mut()).await,
            }
            .unwrap_or(None)
            .ok_or_else(|| Error::Runtime("rpc connection closed by server on retry".to_owned()));
        }

        Ok(resp)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn job_register_returns_eval_id() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q);
        let job = Job { name: "redis".to_owned(), ..Job::default() };
        let resp = ep.handle(Request::JobRegister(job)).unwrap();
        assert!(matches!(resp, Response::JobRegistered { .. }));
    }

    #[test]
    fn eval_dequeue_returns_eval_variant() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q);
        let req = Request::EvalDequeue { schedulers: vec!["service".to_owned()] };
        assert!(matches!(ep.handle(req).unwrap(), Response::Eval(_)));
    }

    #[test]
    fn job_register_enqueues_and_dequeue_returns_it() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q.clone());
        let job = Job { name: "web".to_owned(), ..Job::default() };
        let resp = ep.handle(Request::JobRegister(job)).unwrap();
        let Response::JobRegistered { eval_id } = resp else { panic!("expected JobRegistered") };
        assert!(!eval_id.is_empty());
        // The eval queue now has a pending eval; dequeue it.
        let dequeued = q.dequeue().unwrap().unwrap();
        assert_eq!(dequeued.id, eval_id);
        assert_eq!(dequeued.job_id, "web");
        assert_eq!(dequeued.status, EvalStatus::Pending);
    }

    #[test]
    fn dequeue_returns_highest_priority_first() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q.clone());
        // Register a low-priority job, then a high-priority job.
        let low = Job { name: "low".to_owned(), priority: 30, ..Job::default() };
        let high = Job { name: "high".to_owned(), priority: 80, ..Job::default() };
        ep.handle(Request::JobRegister(low)).unwrap();
        ep.handle(Request::JobRegister(high)).unwrap();
        // Dequeue should yield the high-priority eval first.
        let first = q.dequeue().unwrap().unwrap();
        assert_eq!(first.job_id, "high");
        let second = q.dequeue().unwrap().unwrap();
        assert_eq!(second.job_id, "low");
    }

    #[test]
    fn empty_dequeue_returns_none() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q);
        let req = Request::EvalDequeue { schedulers: vec!["service".to_owned()] };
        let resp = ep.handle(req).unwrap();
        assert!(matches!(resp, Response::Eval(None)));
    }

    #[test]
    fn job_register_persists_job_to_state() {
        let ep = RpcEndpoint::new(EvalQueue::new());
        ep.handle(Request::JobRegister(Job { name: "redis".to_owned(), ..Job::default() })).unwrap();
        assert!(ep.raft().lock().unwrap().state().get_job("redis").is_some());
    }

    fn node(id: &str) -> Node {
        use crate::node::{NodeStatus, SchedulingEligibility};
        Node {
            id: id.into(),
            name: id.to_owned(),
            datacenter: "dc1".to_owned(),
            node_class: String::new(),
            resources: crate::jobspec::Resources::default(),
            status: NodeStatus::Ready,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: std::collections::HashMap::new(),
            drivers: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn node_register_persists_node_to_state() {
        let ep = RpcEndpoint::new(EvalQueue::new());
        assert!(matches!(ep.handle(Request::NodeRegister(node("n1"))).unwrap(), Response::Ack));
        assert!(ep.raft().lock().unwrap().state().get_node("n1").is_some());
    }

    #[test]
    fn node_register_unblocks_blocked_evals() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q.clone());
        q.block(Evaluation {
            id: "blocked1".into(),
            job_id: "web".into(),
            priority: 50,
            trigger: EvalTrigger::JobRegister,
            status: EvalStatus::Pending,
        })
        .unwrap();
        assert_eq!(q.blocked_len(), 1);
        ep.handle(Request::NodeRegister(node("n1"))).unwrap();
        assert_eq!(q.blocked_len(), 0, "node join re-tried blocked evals");
        assert_eq!(q.len(), 1, "blocked eval moved to pending heap");
    }

    #[test]
    fn write_on_follower_returns_not_leader() {
        let raft = Arc::new(Mutex::new(RaftNode::new("f1")));
        let ep = RpcEndpoint::with_raft(EvalQueue::new(), raft);
        let resp = ep.handle(Request::JobRegister(Job { name: "x".to_owned(), ..Job::default() })).unwrap();
        assert!(matches!(resp, Response::NotLeader { .. }));
    }

    #[test]
    fn job_deregister_removes_job_and_enqueues_cleanup_eval() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::with_raft(q.clone(), Arc::new(Mutex::new(RaftNode::bootstrap("l1"))));
        ep.handle(Request::JobRegister(Job { name: "web".to_owned(), ..Job::default() })).unwrap();
        let _ = q.dequeue().unwrap(); // drain the register eval
        ep.handle(Request::JobDeregister("web".to_owned())).unwrap();
        assert!(ep.raft().lock().unwrap().state().get_job("web").is_none());
        let cleanup = q.dequeue().unwrap().expect("cleanup eval enqueued");
        assert_eq!(cleanup.job_id, "web");
        assert_eq!(cleanup.trigger, EvalTrigger::JobDeregister);
    }

    #[test]
    fn node_heartbeat_acknowledged() {
        let ep = RpcEndpoint::new(EvalQueue::new());
        // Register the node first.
        ep.handle(Request::NodeRegister(node("n1"))).unwrap();
        // Heartbeat should be acknowledged.
        let resp = ep.handle(Request::NodeHeartbeat { node_id: "n1".into() }).unwrap();
        assert!(matches!(resp, Response::Ack));
    }

    #[test]
    fn node_get_allocs_returns_empty_for_unknown_node() {
        let ep = RpcEndpoint::new(EvalQueue::new());
        let resp = ep.handle(Request::NodeGetAllocs { node_id: "n1".into() }).unwrap();
        assert!(matches!(resp, Response::NodeAllocs { ref allocs } if allocs.is_empty()));
    }

    // ---- wire transport --------------------------------------------------

    async fn spawn_server(endpoint: Arc<RpcEndpoint>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = RpcServer::new(endpoint);
        tokio::spawn(async move { drop(server.serve(listener).await) });
        addr
    }

    #[tokio::test]
    async fn frame_roundtrips_a_request() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let sent = Request::JobDeregister("x".to_owned());
        write_frame(&mut a, &sent).await.unwrap();
        let got: Request = read_frame(&mut b).await.unwrap().unwrap();
        assert!(matches!(got, Request::JobDeregister(n) if n == "x"));
    }

    #[tokio::test]
    async fn read_frame_returns_none_on_clean_close() {
        let (a, mut b) = tokio::io::duplex(64);
        drop(a); // peer closes without sending
        let got: Result<Option<Request>> = read_frame(&mut b).await;
        assert!(matches!(got, Ok(None)));
    }

    #[tokio::test]
    async fn register_job_over_wire_lands_in_fsm() {
        let endpoint = Arc::new(RpcEndpoint::new(EvalQueue::new()));
        let addr = spawn_server(Arc::clone(&endpoint)).await;
        let mut client = RpcClient::connect(&addr).await.unwrap();

        let resp =
            client.call(&Request::JobRegister(Job { name: "redis".to_owned(), ..Job::default() })).await.unwrap();
        assert!(matches!(resp, Response::JobRegistered { .. }));
        assert!(endpoint.raft().lock().unwrap().state().get_job("redis").is_some());
    }

    #[tokio::test]
    async fn multiple_requests_share_one_connection() {
        let endpoint = Arc::new(RpcEndpoint::new(EvalQueue::new()));
        let addr = spawn_server(Arc::clone(&endpoint)).await;
        let mut client = RpcClient::connect(&addr).await.unwrap();

        assert!(matches!(client.call(&Request::NodeRegister(node("n1"))).await.unwrap(), Response::Ack));
        assert!(matches!(
            client.call(&Request::JobRegister(Job { name: "web".to_owned(), ..Job::default() })).await.unwrap(),
            Response::JobRegistered { .. }
        ));
        // The register enqueued an eval; dequeue it over the same connection.
        let resp = client.call(&Request::EvalDequeue { schedulers: vec!["service".to_owned()] }).await.unwrap();
        assert!(matches!(resp, Response::Eval(Some(_))));
    }

    #[tokio::test]
    async fn write_on_follower_returns_not_leader_over_wire() {
        let endpoint = Arc::new(RpcEndpoint::with_raft(EvalQueue::new(), Arc::new(Mutex::new(RaftNode::new("f1")))));
        let addr = spawn_server(Arc::clone(&endpoint)).await;
        let mut client = RpcClient::connect(&addr).await.unwrap();

        let resp = client.call(&Request::JobRegister(Job { name: "x".to_owned(), ..Job::default() })).await.unwrap();
        assert!(matches!(resp, Response::NotLeader { .. }));
    }

    // ---- NotLeader auto-forward ------------------------------------------

    #[tokio::test]
    async fn not_leader_without_address_passes_through() {
        // When the leader_addr is None, the client should pass the NotLeader
        // response through to the caller rather than silently staying
        // connected to a non-leader.
        let endpoint = Arc::new(RpcEndpoint::with_raft(EvalQueue::new(), Arc::new(Mutex::new(RaftNode::new("f1")))));
        let addr = spawn_server(Arc::clone(&endpoint)).await;
        let mut client = RpcClient::connect(&addr).await.unwrap();

        let resp = client.call(&Request::NodeRegister(node("n1"))).await.unwrap();
        assert!(
            matches!(resp, Response::NotLeader { leader_addr: None }),
            "NotLeader passed through without addr: {resp:?}"
        );
    }

    #[tokio::test]
    async fn not_leader_auto_forwards_to_leader() {
        // Set up a two-node scenario: a leader (bootstrap) and a follower
        // that knows the leader's address. The follower's NotLeader response
        // triggers auto-forward.
        let leader_node = Arc::new(Mutex::new(RaftNode::bootstrap("leader")));
        let leader_ep = RpcEndpoint::with_raft(EvalQueue::new(), Arc::clone(&leader_node));
        let leader_addr = spawn_server(Arc::new(leader_ep)).await;

        // Create a follower whose commit returns NotLeader with the leader addr.
        let mut follower = RaftNode::new("f1");
        follower.set_leader_addr(Some(leader_addr.clone()));
        let follower_node = Arc::new(Mutex::new(follower));
        let follower_ep = RpcEndpoint::with_raft(EvalQueue::new(), Arc::clone(&follower_node));
        let follower_addr = spawn_server(Arc::new(follower_ep)).await;

        // Register a node on the leader first so the state has it.
        leader_node.lock().unwrap().state_mut().upsert_node(node("n1")).unwrap();

        // Connect to the follower. A write should auto-forward to the leader.
        let mut client = RpcClient::connect(&follower_addr).await.unwrap();
        let resp = client.call(&Request::NodeRegister(node("n1"))).await.expect("auto-forward should succeed");
        assert!(matches!(resp, Response::Ack));
    }
}
