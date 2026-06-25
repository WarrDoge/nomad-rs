// SPDX-License-Identifier: Apache-2.0

//! RPC — dependency-agnostic.
//!
//! Defines the request/response surface servers expose to nodes and each other,
//! processed by the in-tree [`RpcEndpoint`](crate::rpc::RpcEndpoint) (forwarding writes
//! to the leader). [`RpcServer`](crate::rpc::RpcServer)/[`RpcClient`](crate::rpc::RpcClient)
//! carry [`Request`](crate::rpc::Request)/[`Response`](crate::rpc::Response)
//! over a length-prefixed JSON frame on a tokio TCP stream; mTLS is layered by
//! wrapping the stream (see [`crate::tls`]) — slotted in once cert plumbing exists.

use crate::error::{Error, Result};
use crate::eval::{EvalStatus, EvalTrigger, Evaluation};
use crate::eval_queue::EvalQueue;
use crate::fsm::Command;
use crate::jobspec::Job;
use crate::node::Node;
use crate::raft::RaftNode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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
}

/// A response to a [`Request`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// A job was registered; an evaluation was created.
    JobRegistered {
        /// Id of the evaluation created for the registration.
        eval_id: String,
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
}

/// The in-tree RPC handler.
///
/// Writes are committed through the local [`RaftNode`] (so they land in the
/// FSM-backed state), then any follow-up eval is enqueued. On a follower, a
/// write returns [`Response::NotLeader`].
///
/// ponytail: no wire transport yet — `NotLeader` reports where to forward, but
/// the actual node↔server RPC socket (custom-over-mTLS / gRPC) is still TBD.
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
    pub fn with_raft(eval_queue: EvalQueue, raft: Arc<Mutex<RaftNode>>) -> Self {
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
        let mut raft = self.raft.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !raft.is_leader() {
            return Ok(Some(Response::NotLeader { leader_addr: raft.leader_addr() }));
        }
        raft.propose(command)?;
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
                    job_id: name,
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
                    job_id: name,
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
        }
    }
}

/// A non-deterministic eval id (nanosecond timestamp); tests must not assert
/// on its exact value.
fn eval_id_for(job_name: &str) -> String {
    format!(
        "eval-{}-{}",
        job_name,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
    )
}

/// Write `msg` as a length-prefixed JSON frame: a 4-byte big-endian length
/// followed by that many bytes of JSON.
///
/// # Errors
///
/// Returns an error if serialisation fails, the frame exceeds [`MAX_FRAME`], or
/// the underlying write fails.
async fn write_frame<W, T>(w: &mut W, msg: &T) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
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
    R: AsyncReadExt + Unpin,
    T: DeserializeOwned,
{
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

/// A TCP RPC server: accepts connections and dispatches framed [`Request`]s
/// through a shared [`RpcEndpoint`], writing back each [`Response`].
#[derive(Debug, Clone)]
pub struct RpcServer {
    /// The endpoint requests are dispatched through.
    endpoint: Arc<RpcEndpoint>,
}

impl RpcServer {
    /// Create a server over the given endpoint.
    #[must_use]
    pub fn new(endpoint: Arc<RpcEndpoint>) -> Self {
        Self { endpoint }
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
            tokio::spawn(serve_conn(stream, endpoint));
        }
    }
}

/// Serve a single connection: read requests until the peer closes, dispatching
/// each through `endpoint`. A handler error closes the connection.
///
/// ponytail: a handler error closes the conn rather than returning a typed error
/// frame — add a `Response::Error` variant if clients need the failure reason.
async fn serve_conn(mut stream: TcpStream, endpoint: Arc<RpcEndpoint>) {
    while let Ok(Some(req)) = read_frame::<_, Request>(&mut stream).await {
        let Ok(resp) = endpoint.handle(req) else { break };
        if write_frame(&mut stream, &resp).await.is_err() {
            break;
        }
    }
}

/// A TCP RPC client: one connection, one request/response at a time.
#[derive(Debug)]
pub struct RpcClient {
    /// The open connection to a server.
    stream: TcpStream,
}

impl RpcClient {
    /// Connect to a server at `addr` (`host:port`).
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub async fn connect(addr: &str) -> Result<Self> {
        Ok(Self { stream: TcpStream::connect(addr).await? })
    }

    /// Send `request` and await the server's response.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails, the connection closes before a
    /// response, or a frame is malformed.
    pub async fn call(&mut self, request: &Request) -> Result<Response> {
        write_frame(&mut self.stream, request).await?;
        read_frame(&mut self.stream).await?.ok_or_else(|| Error::Runtime("rpc connection closed by server".to_owned()))
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
            id: id.to_owned(),
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
}
