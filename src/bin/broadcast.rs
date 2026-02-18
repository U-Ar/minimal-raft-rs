// tests/maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --time-limit 20 --rate 100
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use log::debug;
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node},
    },
};
use tokio::sync::Mutex;

struct BroadcastHandler {
    inner: Arc<Mutex<BroadcastInner>>,
}

struct BroadcastInner {
    neighbors: Vec<String>,
    messages: HashSet<u64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum BroadcastRequest {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    Broadcast {
        message: u64,
    },
    Read {},
}

#[async_trait]
impl Handler for BroadcastHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<BroadcastRequest>(message.body.clone()).unwrap();

        match body {
            BroadcastRequest::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids).await;
            }
            BroadcastRequest::Topology { topology } => {
                let neighbors = topology
                    .get(node.get_node_id())
                    .cloned()
                    .unwrap_or_default();
                let mut inner = self.inner.lock().await;
                inner.neighbors = neighbors;
                debug!(
                    "Set neighbors for node {}: {:?}",
                    node.get_node_id(),
                    inner.neighbors
                );
                node.reply_ok(message).await;
            }
            BroadcastRequest::Broadcast { message: msg } => {
                if message.body.get("msg_id").is_some() {
                    node.reply_ok(message).await;
                }

                let mut inner = self.inner.lock().await;
                if !inner.messages.contains(&msg) {
                    inner.messages.insert(msg);

                    let mut neighbors = Vec::new();
                    for neighbor in &inner.neighbors {
                        neighbors.push(
                            node.rpc(
                                neighbor,
                                serde_json::json!({
                                    "type": "broadcast",
                                    "message": msg,
                                }),
                            )
                            .await,
                        );
                    }

                    // Await all RPCs to neighbors
                    for neighbor_rx in neighbors {
                        let reply = neighbor_rx.await;
                        if let Ok(reply) = reply {
                            debug!("Received reply from neighbor {}: {:?}", msg, reply.body);
                        }
                    }
                    debug!(
                        "Node {} broadcasted message {} to neighbors successfully",
                        node.get_node_id(),
                        msg
                    );
                }
            }
            BroadcastRequest::Read {} => {
                let inner = self.inner.lock().await;
                node.reply(
                    message,
                    serde_json::json!({
                        "type": "read_ok",
                        "messages": inner.messages,
                    }),
                )
                .await;
            }
        }
        Ok(())
    }
}

impl BroadcastHandler {
    fn new() -> Self {
        BroadcastHandler {
            inner: Arc::new(Mutex::new(BroadcastInner {
                neighbors: Vec::new(),
                messages: HashSet::new(),
            })),
        }
    }
}

#[tokio::main]
async fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(BroadcastHandler::new()));
    node.serve().await;
}
