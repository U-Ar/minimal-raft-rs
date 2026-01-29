use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use async_trait::async_trait;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncBufReadExt,
    sync::{Mutex, OnceCell, mpsc, oneshot},
};

use crate::maelstrom_node::error::RPCError;

#[derive(Deserialize, Serialize, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: serde_json::Value,
}

pub enum CallbackSender<T> {
    OneShot(oneshot::Sender<T>),
    Mpsc(mpsc::Sender<T>),
}

impl<T> CallbackSender<T> {
    pub async fn send(self, value: T) {
        match self {
            CallbackSender::OneShot(tx) => {
                let _ = tx.send(value);
            }
            CallbackSender::Mpsc(tx) => {
                let _ = tx.send(value).await;
            }
        }
    }
}

#[derive(Clone)]
pub struct Node {
    pub inner: Arc<NodeInner>,
}

pub struct NodeInner {
    pub msg_id: AtomicU64,
    pub membership: OnceCell<Membership>,
    pub handler: OnceCell<Arc<dyn Handler>>,
    pub callbacks: Mutex<HashMap<u64, CallbackSender<Message>>>,
    pub print_sender: OnceCell<mpsc::Sender<serde_json::Value>>,
}

#[allow(dead_code)]
pub struct Membership {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

impl NodeInner {
    pub fn set_membership(&self, node_id: String, node_ids: Vec<String>) {
        let membership = Membership { node_id, node_ids };
        self.membership.set(membership).unwrap_or_default();
    }

    pub fn get_membership(&self) -> &Membership {
        self.membership.get().unwrap()
    }

    pub fn set_handler(&self, handler: Arc<dyn Handler>) {
        self.handler.set(handler).unwrap_or_default();
    }
}

#[async_trait]
pub trait Handler: Sync + Send {
    async fn handle(&self, node: Node, message: &Message) -> Result<(), RPCError>;
}

impl Node {
    pub fn new() -> Self {
        Node {
            inner: Arc::new(NodeInner {
                msg_id: AtomicU64::new(1),
                membership: OnceCell::new(),
                handler: OnceCell::new(),
                callbacks: Mutex::new(HashMap::new()),
                print_sender: OnceCell::new(),
            }),
        }
    }

    pub fn get_node_id(&self) -> &str {
        &self.inner.get_membership().node_id
    }

    pub fn get_membership(&self) -> &Membership {
        self.inner.get_membership()
    }

    pub async fn init(&self, message: &Message, node_id: String, node_ids: Vec<String>) {
        info!("Initialized node {}", node_id);
        self.inner.set_membership(node_id, node_ids);

        self.reply_ok(message).await;
    }

    pub fn set_handler(&self, handler: Arc<dyn Handler>) {
        self.inner.set_handler(handler);
    }

    pub async fn handle(&self, message: &Message) {
        if let Some(in_reply_to) = message.body.get("in_reply_to") {
            debug!("Handling reply to msg_id {}", in_reply_to.as_u64().unwrap());
            let msg_id = in_reply_to.as_u64().unwrap();
            let mut callbacks = self.inner.callbacks.lock().await;
            if let Some(tx) = callbacks.remove(&msg_id) {
                let _ = tx.send(message.clone()).await;
                debug!("Delivered reply to handler for msg_id {}", msg_id);
                return;
            }
        }

        let msg_type = message.body.get("type").unwrap().as_str().unwrap();
        if let Some(handler) = self.inner.handler.get() {
            if let Err(e) = handler.handle(self.clone(), message).await {
                debug!("Error handling message: {:?}", e);
                self.reply(message, e.to_json()).await;
            }
        } else {
            debug!("No handler for message type: {:?}", msg_type);
            self.reply(
                message,
                RPCError::NotSupported("Operation not supported".to_string()).to_json(),
            )
            .await;
        }
    }

    pub async fn rpc(&self, dest: &str, body: serde_json::Value) -> oneshot::Receiver<Message> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let body = match body {
            serde_json::Value::Object(mut map) => {
                let msg_id = self
                    .inner
                    .msg_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                map.insert(
                    "msg_id".to_string(),
                    serde_json::Value::Number(msg_id.into()),
                );
                serde_json::Value::Object(map)
            }
            _ => {
                panic!("Body must be a JSON object");
            }
        };
        let msg_id = body.get("msg_id").unwrap().as_u64().unwrap();
        {
            let mut callbacks = self.inner.callbacks.lock().await;
            callbacks.insert(msg_id, CallbackSender::OneShot(tx));
        }
        self.send(dest, body).await;
        rx
    }

    pub async fn rpc_sync(
        &self,
        dest: &str,
        body: serde_json::Value,
    ) -> Result<Message, oneshot::error::RecvError> {
        let rx = self.rpc(dest, body).await;
        rx.await
    }

    // レスポンスが来たらcallback Senderから送信する
    pub async fn rpc_with_callback(
        &self,
        dest: &str,
        body: serde_json::Value,
        callback: mpsc::Sender<Message>,
    ) {
        let msg_id = self
            .inner
            .msg_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match body {
            serde_json::Value::Object(mut map) => {
                map.insert(
                    "msg_id".to_string(),
                    serde_json::Value::Number(msg_id.into()),
                );
                let send_body = serde_json::Value::Object(map);
                {
                    let mut callbacks = self.inner.callbacks.lock().await;
                    callbacks.insert(msg_id, CallbackSender::Mpsc(callback));
                }
                self.send(dest, send_body).await;
            }
            _ => {
                debug!("Message body to {} must be a JSON object: {:?}", dest, body);
            }
        };
    }

    pub async fn brpc_with_callback(
        &self,
        body: serde_json::Value,
        callback: mpsc::Sender<Message>,
    ) {
        for dest in &self.get_membership().node_ids {
            if dest == self.get_node_id() {
                continue;
            }
            self.rpc_with_callback(dest, body.clone(), callback.clone())
                .await;
        }
    }

    pub async fn send(&self, dest: &str, body: serde_json::Value) {
        let out_message = Message {
            src: self.inner.get_membership().node_id.clone(),
            dest: dest.to_string(),
            body,
        };
        if let Some(print_sender) = self.inner.print_sender.get() {
            let out_json = serde_json::to_value(&out_message).unwrap();
            if let Err(err) = print_sender.send(out_json).await {
                warn!("Failed to send message to print channel: {}", err);
            }
        } else {
            warn!("Print sender not initialized");
        }
    }

    pub async fn reply(&self, message: &Message, mut body: serde_json::Value) {
        let reply_dest = message.src.as_str();
        let in_reply_to = message.body.get("msg_id").unwrap().as_u64().unwrap();

        match body {
            serde_json::Value::Object(ref mut map) => {
                map.insert(
                    "in_reply_to".to_string(),
                    serde_json::Value::Number(in_reply_to.into()),
                );
                let send_body = serde_json::Value::Object(map.clone());
                self.send(reply_dest, send_body).await;
            }
            _ => {
                warn!(
                    "Reply body to {} must be a JSON object: {:?}",
                    reply_dest, body
                );
            }
        }
    }

    pub async fn reply_ok(&self, message: &Message) {
        if let Some(msg_type) = message.body.get("type") {
            self.reply(
                message,
                serde_json::json!({
                    "type": format!("{}_ok", msg_type.as_str().unwrap()),
                }),
            )
            .await;
        } else {
            warn!("Cannot reply_ok: message has no type field");
        }
    }

    pub async fn serve(self: Arc<Self>) {
        let stdin = tokio::io::stdin();
        let reader = tokio::io::BufReader::new(stdin);
        let mut lines = reader.lines();

        let (print_sender, mut print_receiver) = mpsc::channel::<serde_json::Value>(100);
        self.inner.print_sender.set(print_sender).unwrap();
        tokio::spawn(async move {
            while let Some(string) = print_receiver.recv().await {
                println!("{}", string);
            }
        });

        while let Ok(Some(line)) = lines.next_line().await {
            let ptr = self.clone();
            tokio::spawn(async move {
                debug!("Received: \"{}\"", line.escape_default());

                match serde_json::from_str(&line) as Result<Message, _> {
                    Ok(message) => {
                        ptr.handle(&message).await;
                    }
                    Err(e) => {
                        warn!("Error parsing JSON: {}", e);
                    }
                };
            });
        }
    }

    pub fn run(self: Arc<Self>) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(self.serve());
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}
