// SPDX-License-Identifier: Apache-2.0

//! Integration test helpers for spinning up an in-process Nomad cluster.
//!
//! The [`ClusterBuilder`] provides a builder pattern for assembling a
//! server, client, eval queue, and RPC endpoint, all running in the
//! same process.  Consumers can register jobs, dequeue evaluations, and
//! inspect internal state without needing external processes or network
//! listeners.
//!
//! # Example
//!
//! ```ignore
//! use nomad_rs::integration::ClusterBuilder;
//!
//! let mut cluster = ClusterBuilder::default().build().await.unwrap();
//!
//! // Register a job — an eval is created.
//! let eval_id = cluster.register_job("redis", 50).await.unwrap();
//! assert!(!eval_id.is_empty());
//!
//! // Dequeue the evaluation.
//! let eval = cluster.dequeue_eval().await.unwrap();
//! assert_eq!(eval.job_id, "redis");
//!
//! cluster.shutdown().await;
//! ```

use std::sync::Arc;

use crate::config::Config;
use crate::error::Result;
use crate::eval::Evaluation;
use crate::eval_queue::EvalQueue;
use crate::id::EvalId;
use crate::jobspec::Job;
use crate::rpc::{Request, Response, RpcEndpoint};

/// A running in-process Nomad cluster for integration testing.
///
/// Owns a server, client, eval queue, and RPC endpoint.  Dropping the
/// handle will **not** automatically shut down the server — call
/// [`shutdown`](Self::shutdown) explicitly.
#[derive(Debug)]
pub struct ClusterHandle {
    /// Server handle.
    pub server: crate::server::Server,
    /// Client handle.
    pub client: crate::client::Client,
    /// Shared eval queue.
    pub eval_queue: EvalQueue,
    /// The RPC endpoint wired to the eval queue.
    pub rpc_endpoint: Arc<RpcEndpoint>,
}

impl ClusterHandle {
    /// Register a job and return the created eval ID.
    ///
    /// # Errors
    ///
    /// Delegates to [`RpcEndpoint::handle`].
    pub fn register_job(&self, name: &str, priority: i32) -> Result<EvalId> {
        let job = Job { name: name.to_owned(), priority, ..Job::default() };
        match self.rpc_endpoint.handle(Request::JobRegister(job))? {
            Response::JobRegistered { eval_id } => Ok(eval_id),
            _ => Err(crate::error::Error::Runtime("unexpected response to JobRegister".to_owned())),
        }
    }

    /// Dequeue the highest-priority pending evaluation.
    ///
    /// Returns `None` if the queue is empty.
    #[must_use]
    pub fn dequeue_eval(&self) -> Option<Evaluation> {
        self.eval_queue.dequeue().ok()?
    }

    /// Shut down the cluster (stops the server).
    pub fn shutdown(&mut self) {
        self.server.stop();
    }
}

/// Builder for constructing an in-process Nomad cluster.
///
/// Default configuration creates a server and client with default
/// [`Config`] values.
#[derive(Debug, Default)]
pub struct ClusterBuilder {
    server_config: Config,
    client_config: Config,
    eval_queue: Option<EvalQueue>,
}

impl ClusterBuilder {
    /// Create a new builder with default config.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the server configuration.
    #[must_use]
    pub fn server_config(mut self, config: Config) -> Self {
        self.server_config = config;
        self
    }

    /// Override the client configuration.
    #[must_use]
    pub fn client_config(mut self, config: Config) -> Self {
        self.client_config = config;
        self
    }

    /// Use a pre-existing eval queue (default: creates a new one).
    #[must_use]
    pub fn eval_queue(mut self, queue: EvalQueue) -> Self {
        self.eval_queue = Some(queue);
        self
    }

    /// Build and start the cluster.
    ///
    /// Spawns the server and creates the client, eval queue, and RPC
    /// endpoint.  Returns a [`ClusterHandle`] for interaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to start.
    pub async fn build(self) -> Result<ClusterHandle> {
        // Validate configs early.
        self.server_config.validate()?;
        self.client_config.validate()?;

        // Start the server.
        let mut server = crate::server::Server::new(self.server_config);
        server.run().await?;

        // Create or use the eval queue.
        let eval_queue = self.eval_queue.unwrap_or_default();
        let rpc_endpoint = Arc::new(RpcEndpoint::new(eval_queue.clone()));

        let client = crate::client::Client::new(self.client_config);

        Ok(ClusterHandle { server, client, eval_queue, rpc_endpoint })
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_builder_default() {
        let mut cluster = ClusterBuilder::default().build().await.unwrap();
        assert!(cluster.server.is_running());
        assert!(cluster.eval_queue.is_empty());

        let eval_id = cluster.register_job("test-job", 50).unwrap();
        assert!(!eval_id.is_empty());
        assert_eq!(cluster.eval_queue.len(), 1);

        let eval = cluster.dequeue_eval().unwrap();
        assert_eq!(eval.job_id, "test-job");
        assert_eq!(eval.priority, 50);

        cluster.shutdown();
        assert!(!cluster.server.is_running());
    }

    #[tokio::test]
    async fn test_cluster_builder_custom_config() {
        let cfg = Config {
            node_name: "integration-node".to_owned(),
            bind_addr: "127.0.0.1:9999".to_owned(),
            ..Config::default()
        };

        let mut cluster = ClusterBuilder::new().server_config(cfg.clone()).client_config(cfg).build().await.unwrap();

        assert_eq!(cluster.server.config().node_name, "integration-node");
        assert_eq!(cluster.server.config().bind_addr, "127.0.0.1:9999");
        cluster.shutdown();
    }
}
