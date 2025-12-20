//! # RPC Message Types
//!
//! This module defines the request/response types for inter-node RPC communication.
//! RPC messages are used for direct peer-to-peer communication, such as Raft
//! consensus messages and health queries.
//!
//! # Message Format
//!
//! All RPC messages are JSON-encoded with the following structure:
//!
//! **Request**:
//! ```json
//! {
//!   "method": "vote_request",
//!   "body": { "term": 1, "candidate_id": "..." },
//!   "leader_only": false
//! }
//! ```
//!
//! **Response**:
//! ```json
//! {
//!   "ok": true,
//!   "body": { "vote_granted": true }
//! }
//! ```

use std::sync::{Arc, LazyLock, Mutex};

use crate::discovery::ServiceRecord;

/// An RPC request from one node to another.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RPCRequest {
    /// Method name (e.g., "vote_request", "append_entries")
    pub method: String,

    /// Request payload (method-specific)
    #[serde(default)]
    pub body: serde_json::Map<String, serde_json::Value>,

    /// If true, only the Raft leader should handle this request
    #[serde(default)]
    pub leader_only: bool,
}

/// An RPC response to a request.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RPCResponse {
    /// Whether the request was successful
    pub ok: bool,

    /// Error message if not ok
    #[serde(default)]
    pub error: Option<String>,

    /// Response payload (method-specific)
    #[serde(default)]
    pub body: serde_json::Map<String, serde_json::Value>,
}

use futures::future::BoxFuture;

/// Type alias for RPC handler functions.
type Handler =
    Arc<dyn Fn(&ServiceRecord, RPCRequest) -> BoxFuture<'static, RPCResponse> + Send + Sync>;

/// Global RPC handler storage.
static HANDLER: LazyLock<Mutex<Option<Handler>>> = LazyLock::new(|| Mutex::new(None));

/// Registers the global RPC handler.
///
/// This should be called once during agent initialization.
pub fn register_stream_handler(handler: Handler) {
    *HANDLER.lock().expect("handler lock") = Some(handler);
}

/// Dispatches an RPC request to the registered handler.
///
/// Returns an error response if no handler is registered.
pub async fn handle_request(peer_id: &str, request: RPCRequest) -> RPCResponse {
    let handler = HANDLER.lock().expect("handler lock").clone();
    if let Some(h) = handler {
        let record = ServiceRecord {
            peer_id: peer_id.to_string(),
            ..Default::default()
        };
        h(&record, request).await
    } else {
        RPCResponse {
            ok: false,
            error: Some("no handler registered".to_string()),
            body: Default::default(),
        }
    }
}
