use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use minimal_raft_rs::node::{Handler, Message, Node, RPCError, Request};
use tokio::sync::Mutex;

struct BroadcastHandler {
    inner: Arc<Mutex<BroadcastInner>>,
}

struct BroadcastInner {
    neighbors: Vec<String>,
    messages: HashSet<u64>,
}

#[async_trait]
impl Handler for BroadcastHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<Request>(message.body.clone()).unwrap();

        match body {
            Request::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids).await;
            }
            Request::Topology { topology } => {
                let neighbors = topology
                    .get(node.get_node_id())
                    .cloned()
                    .unwrap_or_default();
                let mut inner = self.inner.lock().await;
                inner.neighbors = neighbors;
                node.log(format!(
                    "Set neighbors for node {}: {:?}",
                    node.get_node_id(),
                    inner.neighbors
                ));
                node.reply_ok(message).await;
            }
            Request::Broadcast { message: msg } => {
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
                            node.log(format!(
                                "Received reply from neighbor {}: {:?}",
                                msg, reply.body
                            ));
                        }
                    }
                    node.log(format!(
                        "Node {} broadcasted message {} to neighbors successfully",
                        node.get_node_id(),
                        msg
                    ));
                }
            }
            Request::Read {} => {
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
            _ => {
                return Err(RPCError::NotSupported(
                    "Operation not supported".to_string(),
                ));
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

fn main() {
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(BroadcastHandler::new()));
    node.run();
}
