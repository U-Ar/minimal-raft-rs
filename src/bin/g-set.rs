// ./tests/maelstrom/maelstrom test -w g-set --bin target/debug/g-set --time-limit 20 --rate 10
use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use log::debug;
use minimal_raft_rs::node::{Handler, Message, Node, RPCError, Request, init_logger};
use tokio::sync::Mutex;

struct GSetHandler {
    inner: Arc<Mutex<GSetInner>>,
}

struct GSetInner {
    elements: HashSet<i64>,
}

#[async_trait]
impl Handler for GSetHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<Request>(message.body.clone()).unwrap();

        match body {
            Request::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids).await;
                let (n0, h0) = (node.clone(), self.inner.clone());
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        debug!("emit replication signal");
                        let inner = h0.lock().await;
                        for dest in n0.get_membership().node_ids.iter() {
                            if dest == n0.get_node_id() {
                                continue;
                            }
                            n0.rpc(
                                dest,
                                serde_json::json!({
                                    "type": "replicate",
                                    "value": inner.elements,
                                }),
                            )
                            .await;
                        }
                    }
                });
            }
            Request::Add {
                element,
                delta: _delta,
            } => {
                let mut inner = self.inner.lock().await;
                inner.elements.insert(element.unwrap());
                node.reply_ok(message).await;
            }
            Request::Read {} => {
                let inner = self.inner.lock().await;
                node.reply(
                    message,
                    serde_json::json!({
                        "type": "read_ok",
                        "value": inner.elements,
                    }),
                )
                .await;
            }
            Request::Replicate {
                value,
                inc: _inc,
                dec: _dec,
            } => {
                let mut inner = self.inner.lock().await;
                for element in value.unwrap() {
                    inner.elements.insert(element);
                }
                node.reply_ok(message).await;
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

impl GSetHandler {
    pub fn new() -> Self {
        GSetHandler {
            inner: Arc::new(Mutex::new(GSetInner {
                elements: HashSet::new(),
            })),
        }
    }
}

fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(GSetHandler::new()));
    node.run();
}
