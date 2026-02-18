// tests/maelstrom/maelstrom test -w echo --bin target/debug/echo --nodes n1 --time-limit 10
use std::sync::Arc;

use async_trait::async_trait;
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node},
    },
};

struct EchoHandler {}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum EchoRequest {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        echo: String,
    },
}

#[async_trait]
impl Handler for EchoHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<EchoRequest>(message.body.clone()).unwrap();

        match body {
            EchoRequest::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids).await;
            }
            EchoRequest::Echo { echo } => {
                node.reply(
                    message,
                    serde_json::json!({
                        "type": "echo_ok",
                        "echo": echo,
                    }),
                )
                .await
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(EchoHandler {}));
    node.serve().await;
}
