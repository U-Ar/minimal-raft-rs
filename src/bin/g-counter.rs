// tests/maelstrom/maelstrom test -w pn-counter --bin target/debug/g-counter --time-limit 30 --rate 10 --nemesis partition
use std::{collections::HashMap, sync::Arc, time::Duration};

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

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum GCounterRequest {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Add {
        delta: i64,
    },
    Read {},
    Replicate {
        inc: HashMap<String, i64>,
        dec: HashMap<String, i64>,
    },
}

struct GCounterHandler {
    inner: Arc<Mutex<GCounterInner>>,
}

pub struct GCounterInner {
    pub inc: HashMap<String, i64>,
    pub dec: HashMap<String, i64>,
}

impl Default for GCounterInner {
    fn default() -> Self {
        Self::new()
    }
}

impl GCounterInner {
    pub fn new() -> Self {
        GCounterInner {
            inc: HashMap::new(),
            dec: HashMap::new(),
        }
    }

    pub fn sum(&self) -> i64 {
        let inc_sum: i64 = self.inc.values().sum();
        let dec_sum: i64 = self.dec.values().sum();
        inc_sum - dec_sum
    }

    pub fn init(&mut self, nodes: &Vec<String>) {
        for node in nodes {
            self.inc.insert(node.clone(), 0);
            self.dec.insert(node.clone(), 0);
        }
    }
}

#[async_trait]
impl Handler for GCounterHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<GCounterRequest>(message.body.clone()).unwrap();

        match body {
            GCounterRequest::Init { node_id, node_ids } => {
                self.inner.lock().await.init(&node_ids);
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
                                    "inc": inner.inc,
                                    "dec": inner.dec,
                                }),
                            )
                            .await;
                        }
                    }
                });
            }
            GCounterRequest::Add { delta } => {
                let mut inner = self.inner.lock().await;
                if delta >= 0 {
                    let counter = inner
                        .inc
                        .entry(node.get_node_id().parse().unwrap())
                        .or_insert(0);
                    *counter += delta;
                } else {
                    let counter = inner
                        .dec
                        .entry(node.get_node_id().parse().unwrap())
                        .or_insert(0);
                    *counter += -delta;
                }
                node.reply_ok(message).await;
            }
            GCounterRequest::Read {} => {
                let inner = self.inner.lock().await;
                node.reply(
                    message,
                    serde_json::json!({
                        "type": "read_ok",
                        "value": inner.sum(),
                    }),
                )
                .await;
            }
            GCounterRequest::Replicate { inc, dec } => {
                let mut inner = self.inner.lock().await;
                for (k, v) in inc {
                    if !inner.inc.contains_key(&k) || v > inner.inc.get(&k).cloned().unwrap_or(0) {
                        inner.inc.insert(k, v);
                    }
                }
                for (k, v) in dec {
                    if !inner.dec.contains_key(&k) || v > inner.dec.get(&k).cloned().unwrap_or(0) {
                        inner.dec.insert(k, v);
                    }
                }
                node.reply_ok(message).await;
            }
        }
        Ok(())
    }
}

impl GCounterHandler {
    pub fn new() -> Self {
        GCounterHandler {
            inner: Arc::new(Mutex::new(GCounterInner::new())),
        }
    }
}

fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(GCounterHandler::new()));
    node.run();
}
