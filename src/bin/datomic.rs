// ./tests/maelstrom/maelstrom test -w txn-list-append --bin target/debug/datomic --time-limit 10 --node-count 2 --rate 100
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use async_trait::async_trait;
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node, Request},
    },
};
use tokio::sync::Mutex;

const LIN_KV_ROOT_KEY: &str = "root";
const LIN_KV_NODE_ID: &str = "lin-kv";

struct DatomicHandler {
    thunk_id: AtomicU64,
    thunk_cache: Mutex<HashMap<String, serde_json::Value>>,
}

impl DatomicHandler {
    pub fn new() -> Self {
        DatomicHandler {
            thunk_id: AtomicU64::new(0),
            thunk_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn gen_id(&self, node: &Node) -> String {
        let node_id = node.get_node_id();
        let thunk_id = self
            .thunk_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{}-{}", node_id, thunk_id)
    }

    pub async fn read_value_from_lin_kv(
        &self,
        node: &Node,
        key: &str,
    ) -> Option<serde_json::Value> {
        if key != LIN_KV_ROOT_KEY {
            let cache = self.thunk_cache.lock().await;
            if let Some(val) = cache.get(key) {
                return Some(val.clone());
            }
        }
        let val = node
            .rpc_sync(
                LIN_KV_NODE_ID,
                serde_json::json!(
                    {
                        "type": "read",
                        "key": key,
                    }
                ),
            )
            .await
            .expect("rpc failed")
            .body
            .get("value")
            .cloned();
        if key != LIN_KV_ROOT_KEY {
            let mut cache = self.thunk_cache.lock().await;
            if let Some(ref val) = val {
                cache.insert(key.to_string(), val.clone());
            }
        }
        val
    }

    pub async fn write_value_to_lin_kv(&self, node: &Node, key: &str, value: serde_json::Value) {
        let _res = node
            .rpc_sync(
                LIN_KV_NODE_ID,
                serde_json::json!(
                    {
                        "type": "write",
                        "key": key,
                        "value": value,
                    }
                ),
            )
            .await
            .expect("rpc failed");
    }

    pub async fn cas_value_to_lin_kv(
        &self,
        node: &Node,
        key: &str,
        from: serde_json::Value,
        to: serde_json::Value,
    ) -> bool {
        let res = node
            .rpc_sync(
                LIN_KV_NODE_ID,
                serde_json::json!(
                    {
                        "type": "cas",
                        "key": key,
                        "from": from,
                        "to": to,
                        "create_if_not_exists": true,
                    }
                ),
            )
            .await
            .expect("rpc failed");
        res.body.get("type").unwrap().as_str().unwrap() == "cas_ok"
    }
}

#[async_trait]
impl Handler for DatomicHandler {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError> {
        let body = serde_json::from_value::<Request>(message.body.clone()).unwrap();

        match body {
            Request::Init { node_id, node_ids } => {
                node.init(message, node_id, node_ids).await;
            }
            Request::Txn { txn } => {
                let mut result = Vec::new();

                // 元のroot thunkのID
                let root_id1 = match self.read_value_from_lin_kv(&node, LIN_KV_ROOT_KEY).await {
                    Some(val) => val.as_str().unwrap().to_string(),
                    None => self.gen_id(&node),
                };
                let root_id2 = self.gen_id(&node);

                // 現在のデータのマップ
                let json_map1 = match self.read_value_from_lin_kv(&node, &root_id1).await {
                    Some(val) => val,
                    None => serde_json::Value::Object(serde_json::Map::new()),
                };
                let map1 = match json_map1 {
                    serde_json::Value::Object(obj) => obj,
                    _ => serde_json::Map::new(),
                };
                let mut map2 = map1.clone();

                for op in txn {
                    match op.as_slice() {
                        [serde_json::Value::String(op_type), rest @ ..] => match op_type.as_str() {
                            "r" => {
                                let key = rest[0].as_u64().unwrap();
                                let list_id = match map2.get(&key.to_string()) {
                                    Some(val) => val.as_str().unwrap().to_string(),
                                    None => "".to_string(),
                                };
                                let list = if list_id.is_empty() {
                                    serde_json::Value::Array(vec![])
                                } else {
                                    match self.read_value_from_lin_kv(&node, &list_id).await {
                                        Some(val) => val,
                                        None => serde_json::Value::Array(vec![]),
                                    }
                                };
                                result.push(serde_json::json!(["r", key, list]));
                            }
                            "append" => {
                                let key = rest[0].as_u64().unwrap();
                                let value = rest[1].as_u64().unwrap();

                                result.push(serde_json::json!([
                                    op_type,
                                    key.clone(),
                                    value.clone()
                                ]));

                                let list_id1 = match map2.get(&key.to_string()) {
                                    Some(val) => val.as_str().unwrap().to_string(),
                                    None => "".to_string(),
                                };
                                let list1 = if list_id1.is_empty() {
                                    serde_json::Value::Array(vec![])
                                } else {
                                    match self.read_value_from_lin_kv(&node, &list_id1).await {
                                        Some(val) => val,
                                        None => serde_json::Value::Array(vec![]),
                                    }
                                };
                                let mut new_list = list1.as_array().unwrap_or(&vec![]).clone();
                                new_list.push(serde_json::Value::Number(value.into()));

                                let list_id2 = self.gen_id(&node);

                                self.write_value_to_lin_kv(
                                    &node,
                                    &list_id2,
                                    serde_json::Value::Array(new_list),
                                )
                                .await;

                                map2.insert(key.to_string(), serde_json::Value::String(list_id2));
                            }
                            _ => {
                                return Err(RPCError::NotSupported(
                                    "Operation not supported".to_string(),
                                ));
                            }
                        },
                        _ => {
                            return Err(RPCError::NotSupported(
                                "Operation not supported".to_string(),
                            ));
                        }
                    }
                }

                self.write_value_to_lin_kv(&node, &root_id2, serde_json::Value::Object(map2))
                    .await;

                let cas_ok = self
                    .cas_value_to_lin_kv(
                        &node,
                        LIN_KV_ROOT_KEY,
                        serde_json::Value::String(root_id1),
                        serde_json::Value::String(root_id2),
                    )
                    .await;

                if cas_ok {
                    node.reply(
                        message,
                        serde_json::json!({
                            "type": "txn_ok",
                            "txn": result,
                        }),
                    )
                    .await;
                } else {
                    return Err(RPCError::TxnConflict("CAS failed!".to_string()));
                }
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
    let handler = Arc::new(DatomicHandler::new());
    node.set_handler(handler);
    node.run();
}
