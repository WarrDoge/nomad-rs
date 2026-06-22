// SPDX-License-Identifier: Apache-2.0

//! HTTP API contract — dependency-agnostic.
//!
//! Defines the request/response shape and the handler that routes API calls.
//! The concrete web framework (axum/hyper/...) lives behind [`ApiHandler`].
//! [`HttpApi`] is the in-tree handler whose behaviour is specified by the tests
//! and is unimplemented.

use crate::error::Result;

/// HTTP method of an API request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `DELETE`.
    Delete,
}

/// A decoded API request.
#[derive(Debug, Clone)]
pub struct ApiRequest {
    /// Request method.
    pub method: Method,
    /// Request path, e.g. `"/v1/jobs"`.
    pub path: String,
    /// Optional JSON body.
    pub body: Option<String>,
}

/// An API response.
#[derive(Debug, Clone)]
pub struct ApiResponse {
    /// HTTP status code.
    pub status: u16,
    /// JSON response body.
    pub body: String,
}

/// Routes and handles API requests.
pub trait ApiHandler {
    /// Handle one request.
    ///
    /// # Errors
    ///
    /// Returns an error if routing fails or the underlying operation errors.
    fn handle(&self, request: ApiRequest) -> Result<ApiResponse>;
}

/// The in-tree HTTP API.
#[derive(Debug)]
pub struct HttpApi;

impl ApiHandler for HttpApi {
    #[allow(clippy::needless_pass_by_value, reason = "request is routed/consumed once implemented")]
    fn handle(&self, request: ApiRequest) -> Result<ApiResponse> {
        let _ = request;
        todo!("route the request to the matching endpoint and serialise the result")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn list_jobs_returns_200() {
        let req = ApiRequest { method: Method::Get, path: "/v1/jobs".to_owned(), body: None };
        assert_eq!(HttpApi.handle(req).unwrap().status, 200);
    }

    #[test]
    fn unknown_path_returns_404() {
        let req = ApiRequest { method: Method::Get, path: "/v1/nope".to_owned(), body: None };
        assert_eq!(HttpApi.handle(req).unwrap().status, 404);
    }
}
