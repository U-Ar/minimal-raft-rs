#[derive(Debug)]
pub enum RPCError {
    Timeout(String),
    NodeNotFound(String),
    NotSupported(String),
    TemporarilyUnavailable(String),
    MalformedRequest(String),
    Crash(String),
    Abort(String),
    KeyDoesNotExist(String),
    KeyAlreadyExists(String),
    PreconditionFailed(String),
    TxnConflict(String),
}

impl RPCError {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            RPCError::Timeout(text) => {
                serde_json::json!({"type": "error", "code": 0, "text": text})
            }
            RPCError::NodeNotFound(text) => {
                serde_json::json!({"type": "error", "code": 1, "text": text})
            }
            RPCError::NotSupported(text) => {
                serde_json::json!({"type": "error", "code": 10, "text": text})
            }
            RPCError::TemporarilyUnavailable(text) => {
                serde_json::json!({"type": "error", "code": 11, "text": text})
            }
            RPCError::MalformedRequest(text) => {
                serde_json::json!({"type": "error", "code": 12, "text": text})
            }
            RPCError::Crash(text) => serde_json::json!({"type": "error", "code": 13, "text": text}),
            RPCError::Abort(text) => serde_json::json!({"type": "error", "code": 14, "text": text}),
            RPCError::KeyDoesNotExist(text) => {
                serde_json::json!({"type": "error", "code": 20, "text": text})
            }
            RPCError::KeyAlreadyExists(text) => {
                serde_json::json!({"type": "error", "code": 21, "text": text})
            }
            RPCError::PreconditionFailed(text) => {
                serde_json::json!({"type": "error", "code": 22, "text": text})
            }
            RPCError::TxnConflict(text) => {
                serde_json::json!({"type": "error", "code": 23, "text": text})
            }
        }
    }
}
