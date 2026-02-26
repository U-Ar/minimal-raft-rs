// tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/lin-kv --time-limit 10 --rate 10 --node-count 1 --concurrency 2n
// tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/lin-kv --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use log::{debug, warn};
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node},
    },
    raft::{
        ClientRequest, ClusterConfig, RPCMessage, Raft, RaftError, RaftLogEntry, RaftMessage,
        RaftRequest, RaftResponse,
    },
    state_machine::hash_map_state_machine::{
        HashMapStateMachine, HashMapStateMachineCommand, HashMapStateMachineResponse,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc, oneshot};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum LinKvRequest {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Read {
        msg_id: u64,
        key: u64,
    },
    Write {
        msg_id: u64,
        key: u64,
        value: u64,
    },
    Cas {
        msg_id: u64,
        key: u64,
        from: u64,
        to: u64,
    },
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteRes {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    },
    AppendEntriesRes {
        term: u64,
        success: bool,
        next_index: u64,
        conflict_index: Option<u64>,
    },
}

fn convert_raft_error(err: RaftError) -> RPCError {
    match err {
        RaftError::Timeout(message) => RPCError::Timeout(message),
        RaftError::NodeNotFound(message) => RPCError::NodeNotFound(message),
        RaftError::NotSupported(message) => RPCError::NotSupported(message),
        RaftError::NoLeader(message) => RPCError::TemporarilyUnavailable(message),
        RaftError::MalformedRequest(message) => RPCError::MalformedRequest(message),
        RaftError::Crash(message) => RPCError::Crash(message),
        RaftError::Abort(message) => RPCError::Abort(message),
        RaftError::KeyDoesNotExist(message) => RPCError::KeyDoesNotExist(message),
        RaftError::KeyAlreadyExists(message) => RPCError::KeyAlreadyExists(message),
        RaftError::PreconditionFailed(message) => RPCError::PreconditionFailed(message),
        RaftError::Redirect {
            leader_id: _leader_id,
        } => RPCError::TemporarilyUnavailable("not leader".to_string()),
    }
}

struct RaftHandler {
    raft_sender: OnceLock<mpsc::Sender<RaftRequest>>,
}

impl RaftHandler {
    pub fn new() -> Self {
        RaftHandler {
            raft_sender: OnceLock::new(),
        }
    }
}

impl RaftHandler {
    async fn process_raft_request(
        &self,
        src: String,
        request: RaftMessage,
    ) -> Result<RaftResponse, RaftError> {
        let (tx, rx) = oneshot::channel();
        if let Some(raft_sender) = self.raft_sender.get() {
            let _ = raft_sender
                .send(RaftRequest {
                    src,
                    request,
                    response_tx: Some(tx),
                })
                .await;
        } else {
            return Err(RaftError::Crash(
                "Raft instance not yet started".to_string(),
            ));
        }

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(RaftError::Crash(
                "Raft task not running (oneshot recv failed)".to_string(),
            )),
        }
    }

    async fn process_raft_request_with_no_response(
        &self,
        src: String,
        request: RaftMessage,
    ) -> Result<(), RaftError> {
        if let Some(raft_sender) = self.raft_sender.get() {
            let _ = raft_sender
                .send(RaftRequest {
                    src,
                    request,
                    response_tx: None,
                })
                .await;
            Ok(())
        } else {
            Err(RaftError::Crash(
                "Raft instance not yet started".to_string(),
            ))
        }
    }
}

#[async_trait]
impl Handler for RaftHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<LinKvRequest>(message.body.clone()).unwrap();

        match &body {
            LinKvRequest::Init { node_id, node_ids } => {
                node.init(message, node_id.clone(), node_ids.clone()).await;
                debug!("Initialized node. starting Raft instance...");
                self.raft_sender
                    .set(Raft::start(
                        ClusterConfig {
                            node_id: node_id.clone(),
                            node_ids: node_ids.clone(),
                        },
                        Arc::new(node.clone()),
                        Arc::new(Mutex::new(HashMapStateMachine::new())),
                    ))
                    .unwrap_or_default();
                debug!("Raft instance started.");
            }
            LinKvRequest::Read { msg_id, key } => {
                let res = self
                    .process_raft_request(
                        message.src.clone(),
                        RaftMessage::Client(ClientRequest::Command {
                            op: serde_json::to_vec(&HashMapStateMachineCommand::Read { key: *key })
                                .unwrap(),
                        }),
                    )
                    .await;
                match res {
                    Ok(raft_response) => {
                        if let RaftResponse::Client { result } = raft_response {
                            let value: HashMapStateMachineResponse =
                                serde_json::from_slice(&result).unwrap();
                            if let HashMapStateMachineResponse::ReadOk { value } = value {
                                node.reply(
                                    message,
                                    serde_json::json!({
                                        "type": "read_ok",
                                        "msg_id": msg_id,
                                        "value": value,
                                    }),
                                )
                                .await;
                            } else {
                                return Err(RPCError::Crash(
                                    "Unexpected state machine response".to_string(),
                                ));
                            }
                        } else {
                            return Err(RPCError::Crash(
                                "Unexpected response type from raft".to_string(),
                            ));
                        }
                    }
                    Err(err) => {
                        if let RaftError::Redirect { leader_id } = err {
                            let redirect_result =
                                node.rpc_sync(&leader_id, message.body.clone()).await;
                            match redirect_result {
                                Ok(redirect_response) => {
                                    node.reply(message, redirect_response.body).await;
                                }
                                Err(err) => {
                                    warn!("Error redirecting to leader {}: {:?}", leader_id, err);
                                    return Err(RPCError::Crash(
                                        "inter-node communication failed.".to_string(),
                                    ));
                                }
                            }
                        } else {
                            return Err(convert_raft_error(err));
                        }
                    }
                }
            }
            LinKvRequest::Write { msg_id, key, value } => {
                let res = self
                    .process_raft_request(
                        message.src.clone(),
                        RaftMessage::Client(ClientRequest::Command {
                            op: serde_json::to_vec(&HashMapStateMachineCommand::Write {
                                key: *key,
                                value: *value,
                            })
                            .unwrap(),
                        }),
                    )
                    .await;
                match res {
                    Ok(raft_response) => {
                        if let RaftResponse::Client { result } = raft_response {
                            let value: HashMapStateMachineResponse =
                                serde_json::from_slice(&result).unwrap();
                            if let HashMapStateMachineResponse::WriteOk = value {
                                node.reply(
                                    message,
                                    serde_json::json!({
                                        "type": "write_ok",
                                        "msg_id": msg_id,
                                    }),
                                )
                                .await;
                            } else {
                                return Err(RPCError::Crash(
                                    "Unexpected state machine response".to_string(),
                                ));
                            }
                        } else {
                            return Err(RPCError::Crash(
                                "Unexpected response type from raft".to_string(),
                            ));
                        }
                    }
                    Err(err) => {
                        if let RaftError::Redirect { leader_id } = err {
                            let redirect_result =
                                node.rpc_sync(&leader_id, message.body.clone()).await;
                            match redirect_result {
                                Ok(redirect_response) => {
                                    node.reply(message, redirect_response.body).await;
                                }
                                Err(err) => {
                                    warn!("Error redirecting to leader {}: {:?}", leader_id, err);
                                    return Err(RPCError::Crash(
                                        "inter-node communication failed.".to_string(),
                                    ));
                                }
                            }
                        } else {
                            return Err(convert_raft_error(err));
                        }
                    }
                }
            }
            LinKvRequest::Cas {
                msg_id,
                key,
                from,
                to,
            } => {
                let res = self
                    .process_raft_request(
                        message.src.clone(),
                        RaftMessage::Client(ClientRequest::Command {
                            op: serde_json::to_vec(&HashMapStateMachineCommand::Cas {
                                key: *key,
                                from: *from,
                                to: *to,
                            })
                            .unwrap(),
                        }),
                    )
                    .await;
                match res {
                    Ok(raft_response) => {
                        if let RaftResponse::Client { result } = raft_response {
                            let value: HashMapStateMachineResponse =
                                serde_json::from_slice(&result).unwrap();
                            if let HashMapStateMachineResponse::CasOk = value {
                                node.reply(
                                    message,
                                    serde_json::json!({
                                        "type": "cas_ok",
                                        "msg_id": msg_id,
                                    }),
                                )
                                .await;
                            } else {
                                return Err(RPCError::PreconditionFailed(
                                    "CAS precondition failed".to_string(),
                                ));
                            }
                        } else {
                            return Err(RPCError::Crash(
                                "Unexpected response type from raft".to_string(),
                            ));
                        }
                    }
                    Err(err) => {
                        if let RaftError::Redirect { leader_id } = err {
                            let redirect_result =
                                node.rpc_sync(&leader_id, message.body.clone()).await;
                            match redirect_result {
                                Ok(redirect_response) => {
                                    node.reply(message, redirect_response.body).await;
                                }
                                Err(err) => {
                                    warn!("Error redirecting to leader {}: {:?}", leader_id, err);
                                    return Err(RPCError::Crash(
                                        "inter-node communication failed.".to_string(),
                                    ));
                                }
                            }
                        } else {
                            return Err(convert_raft_error(err));
                        }
                    }
                }
            }
            LinKvRequest::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => match self
                .process_raft_request(
                    message.src.clone(),
                    RaftMessage::RPC(RPCMessage::RequestVote {
                        term: *term,
                        candidate_id: candidate_id.clone(),
                        last_log_index: *last_log_index as usize,
                        last_log_term: *last_log_term,
                    }),
                )
                .await
            {
                Ok(value) => {
                    node.reply(message, serde_json::to_value(&value).unwrap())
                        .await;
                }
                Err(err) => {
                    return Err(convert_raft_error(err));
                }
            },
            LinKvRequest::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => match self
                .process_raft_request(
                    message.src.clone(),
                    RaftMessage::RPC(RPCMessage::AppendEntries {
                        term: *term,
                        leader_id: leader_id.clone(),
                        prev_log_index: *prev_log_index as usize,
                        prev_log_term: *prev_log_term,
                        entries: entries.clone(),
                        leader_commit: *leader_commit as usize,
                    }),
                )
                .await
            {
                Ok(value) => {
                    node.reply(message, serde_json::to_value(&value).unwrap())
                        .await;
                }
                Err(err) => {
                    return Err(convert_raft_error(err));
                }
            },
            LinKvRequest::AppendEntriesRes {
                term,
                success,
                next_index,
                conflict_index,
            } => {
                match self
                    .process_raft_request_with_no_response(
                        message.src.clone(),
                        RaftMessage::RPC(RPCMessage::AppendEntriesRes {
                            term: *term,
                            success: *success,
                            next_index: *next_index as usize,
                            conflict_index: conflict_index.map(|idx| idx as usize),
                        }),
                    )
                    .await
                {
                    Ok(()) => {
                        // no response expected
                    }
                    Err(err) => {
                        return Err(convert_raft_error(err));
                    }
                };
            }
            LinKvRequest::RequestVoteRes { term, vote_granted } => {
                match self
                    .process_raft_request_with_no_response(
                        message.src.clone(),
                        RaftMessage::RPC(RPCMessage::RequestVoteRes {
                            term: *term,
                            vote_granted: *vote_granted,
                        }),
                    )
                    .await
                {
                    Ok(()) => {
                        // no response expected
                    }
                    Err(err) => {
                        return Err(convert_raft_error(err));
                    }
                };
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(RaftHandler::new()));
    node.serve().await;
}
