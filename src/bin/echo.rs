// ./tests/maelstrom/maelstrom test -w echo --bin target/debug/echo --nodes n1 --time-limit 10
use std::sync::Arc;

use async_trait::async_trait;
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node, Request},
    },
};

struct EchoHandler {}

#[async_trait]
impl Handler for EchoHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<Request>(message.body.clone()).unwrap();

        match body {
            Request::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids).await;
            }
            Request::Echo { echo } => {
                node.reply(
                    message,
                    serde_json::json!({
                        "type": "echo_ok",
                        "echo": echo,
                    }),
                )
                .await
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

fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(EchoHandler {}));
    node.run();
}
