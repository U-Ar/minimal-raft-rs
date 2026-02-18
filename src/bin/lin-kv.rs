// tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/lin-kv --time-limit 10 --rate 10 --node-count 1 --concurrency 2n
// tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/lin-kv --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use log::debug;
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node},
    },
    raft::{ClusterConfig, LinKvRequest, Raft, RaftRequest},
};
use tokio::sync::{mpsc, oneshot};

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
        request: LinKvRequest,
    ) -> Result<serde_json::Value, RPCError> {
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
            return Err(RPCError::Crash("Raft instance not yet started".to_string()));
        }

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(RPCError::Crash(
                "Raft task not running (oneshot recv failed)".to_string(),
            )),
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
                    ))
                    .unwrap_or_default();
                debug!("Raft instance started.");
            }
            LinKvRequest::Read {
                origin,
                msg_id: _msg_id,
                key: _key,
            } => {
                if let Some(raft_sender) = self.raft_sender.get() {
                    let _ = raft_sender
                        .send(RaftRequest {
                            src: if origin.is_some() {
                                origin.clone().unwrap()
                            } else {
                                message.src.clone()
                            },
                            request: body,
                            response_tx: None,
                        })
                        .await;
                } else {
                    return Err(RPCError::Crash("Raft instance not yet started".to_string()));
                }
            }
            LinKvRequest::Write {
                origin,
                msg_id: _msg_id,
                key: _key,
                value: _value,
            } => {
                if let Some(raft_sender) = self.raft_sender.get() {
                    let _ = raft_sender
                        .send(RaftRequest {
                            src: if origin.is_some() {
                                origin.clone().unwrap()
                            } else {
                                message.src.clone()
                            },
                            request: body,
                            response_tx: None,
                        })
                        .await;
                } else {
                    return Err(RPCError::Crash("Raft instance not yet started".to_string()));
                }
            }
            LinKvRequest::Cas {
                origin,
                msg_id: _msg_id,
                key: _key,
                from: _from,
                to: _to,
            } => {
                if let Some(raft_sender) = self.raft_sender.get() {
                    let _ = raft_sender
                        .send(RaftRequest {
                            src: if origin.is_some() {
                                origin.clone().unwrap()
                            } else {
                                message.src.clone()
                            },
                            request: body,
                            response_tx: None,
                        })
                        .await;
                } else {
                    return Err(RPCError::Crash("Raft instance not yet started".to_string()));
                }
            }
            LinKvRequest::RequestVote {
                term: _term,
                candidate_id: _candidate_id,
                last_log_index: _last_log_index,
                last_log_term: _last_log_term,
            } => match self.process_raft_request(message.src.clone(), body).await {
                Ok(value) => {
                    node.reply(message, value).await;
                }
                Err(err) => {
                    return Err(err);
                }
            },
            LinKvRequest::AppendEntries {
                term: _term,
                leader_id: _leader_id,
                prev_log_index: _prev_log_index,
                prev_log_term: _prev_log_term,
                entries: _entries,
                leader_commit: _leader_commit,
            } => match self.process_raft_request(message.src.clone(), body).await {
                Ok(value) => {
                    node.reply(message, value).await;
                }
                Err(err) => {
                    return Err(err);
                }
            },
            _ => {
                return Err(RPCError::NotSupported(
                    "Operation not supported".to_string(),
                ));
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
