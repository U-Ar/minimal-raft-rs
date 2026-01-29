// ./tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/raft --time-limit 10 --rate 10 --node-count 1 --concurrency 2n
// ./tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/raft --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10 --test-count 10

use std::{
    collections::{HashMap, HashSet},
    ops::{Index, IndexMut},
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use log::{debug, info};
use minimal_raft_rs::{
    logger::init_logger,
    maelstrom_node::{
        error::RPCError,
        node::{Handler, Message, Node},
    },
};
use rand::{self};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type RaftValue = serde_json::Value;

fn number_or_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::String(s) => Ok(s),
        _ => Err(serde::de::Error::custom("expected number or string")),
    }
}

fn majority(count: usize) -> usize {
    (count / 2) + 1
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum LinKvRequest {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Read {
        origin: Option<String>,
        msg_id: u64,
        #[serde(deserialize_with = "number_or_string")]
        key: String,
    },
    Write {
        origin: Option<String>,
        msg_id: u64,
        #[serde(deserialize_with = "number_or_string")]
        key: String,
        value: RaftValue,
    },
    Cas {
        origin: Option<String>,
        msg_id: u64,
        #[serde(deserialize_with = "number_or_string")]
        key: String,
        from: RaftValue,
        to: RaftValue,
    },
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    StartElection {},
    RequestVoteRes {
        term: u64,
        vote_granted: bool,
    },
    CheckStepDown {},
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
    },
    ReplicateLog {},
}

struct RaftRequest {
    pub src: String,
    pub request: LinKvRequest,
    pub response_tx: Option<oneshot::Sender<Result<serde_json::Value, RPCError>>>,
}

struct Raft {
    // ハートビート・タイムアウト
    election_timeout: Duration,         // 選挙タイムアウト
    heartbeat_interval: Duration,       // ハートビート間隔
    min_replication_interval: Duration, // 最小複製間隔

    election_deadline: Instant,  // 選挙開始タイミング
    step_down_deadline: Instant, // リーダー降格タイミング
    last_replication: Instant,   // 最後のログ複製時刻

    // 入出力
    node: Node,
    raft_sender: mpsc::Sender<RaftRequest>,

    // 選挙状態
    state: State,
    term: u64,
    voted_for: Option<String>,
    votes: HashSet<String>,
    cancellation_token: CancellationToken,

    // リーダー状態
    leader: Option<String>,
    commit_index: usize,
    next_index: HashMap<String, usize>,
    match_index: HashMap<String, usize>,

    state_machine: HashMap<String, RaftValue>,
    last_applied: usize,

    log: RaftLog,
}

enum State {
    Follower,
    Candidate,
    Leader,
}

struct RaftHandler {
    raft_sender: OnceLock<mpsc::Sender<RaftRequest>>,
}

impl Raft {
    pub fn start(node: Node) -> mpsc::Sender<RaftRequest> {
        let (tx0, mut rx0) = mpsc::channel::<RaftRequest>(100);
        let tx1 = tx0.clone();
        info!("Raft instance handling starting...");
        tokio::spawn(async move {
            let mut raft = Raft::new(node, tx0);
            loop {
                debug!("Waiting for Raft request...");
                if let Some(message) = rx0.recv().await {
                    debug!("Received Raft request");
                    let _ = raft.handle_request(message).await;
                }
            }
        });
        tx1
    }

    pub fn new(node: Node, raft_sender: mpsc::Sender<RaftRequest>) -> Self {
        Raft {
            election_timeout: Duration::from_secs(2),
            heartbeat_interval: Duration::from_secs(1),
            min_replication_interval: Duration::from_millis(50),
            election_deadline: Instant::now(),
            step_down_deadline: Instant::now(),
            last_replication: Instant::now(),
            node,
            raft_sender,
            state: State::Follower,
            term: 0,
            voted_for: None,
            votes: HashSet::new(),
            cancellation_token: CancellationToken::new(),
            leader: None,
            commit_index: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            state_machine: HashMap::new(),
            last_applied: 1,
            log: RaftLog::new(),
        }
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }

    pub fn is_voted(&self) -> bool {
        self.voted_for.is_some()
    }

    pub fn get_voted_for(&self) -> String {
        self.voted_for.clone().unwrap()
    }

    pub fn set_voted_for(&mut self, candidate_id: String) {
        self.voted_for = Some(candidate_id);
    }

    pub fn passed_election_deadline(&self) -> bool {
        Instant::now() >= self.election_deadline
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader)
    }

    pub fn is_candidate(&self) -> bool {
        matches!(self.state, State::Candidate)
    }

    fn get_commit_threshold(&self) -> usize {
        let mut indexes: Vec<usize> = self.match_index.values().cloned().collect();
        indexes.push(self.last_index());

        indexes.sort_unstable();

        indexes[indexes.len() / 2]
    }

    fn cancel_election(&self) {
        self.cancellation_token.cancel();
    }

    fn last_index(&self) -> usize {
        self.log.len()
    }

    fn last_term(&self) -> u64 {
        self.log.last().map(|entry| entry.term).unwrap_or(0)
    }

    fn is_candidate_log_up_to_date(
        &self,
        candidate_last_log_index: usize,
        candidate_last_log_term: u64,
    ) -> bool {
        let last_log_term = self.last_term();
        if candidate_last_log_term > last_log_term {
            true
        } else if candidate_last_log_term == last_log_term {
            candidate_last_log_index >= self.last_index()
        } else {
            false
        }
    }

    pub async fn become_candidate(&mut self) {
        self.state = State::Candidate;
        self.leader = None;
        self.reset_election_deadline();
        self.reset_step_down_deadline();

        self.advance_term(self.term + 1);
        debug!("Becoming candidate for term {}", self.term);
        self.request_votes().await;
    }

    pub fn become_follower(&mut self) {
        self.state = State::Follower;
        self.leader = None;
        self.next_index.clear();
        self.match_index.clear();

        self.reset_election_deadline();
        self.cancel_election();
        debug!("Became follower for term {}", self.term);
    }

    pub fn become_leader(&mut self) {
        self.state = State::Leader;
        self.leader = Some(self.node.get_node_id().to_string());
        self.last_replication = Instant::now() - Duration::from_secs(1);

        self.next_index.clear();
        self.match_index.clear();
        for node_id in &self.node.get_membership().node_ids {
            if node_id != self.node.get_node_id() {
                self.next_index.insert(node_id.clone(), self.last_index());
                self.match_index.insert(node_id.clone(), 0);
            }
        }

        self.reset_step_down_deadline();
        debug!("Became leader for term {}", self.term);
        self.cancel_election();
    }

    pub fn reset_election_deadline(&mut self) {
        self.election_deadline =
            Instant::now() + rand::random_range(self.election_timeout..self.election_timeout * 2);
    }

    pub fn reset_step_down_deadline(&mut self) {
        self.step_down_deadline = Instant::now() + self.election_timeout;
    }

    pub fn advance_term(&mut self, new_term: u64) {
        if new_term > self.term {
            self.term = new_term;
            self.voted_for = None;
            self.votes.clear();
        }
    }

    pub async fn advance_commit_index(&mut self) {
        if self.is_leader() {
            let n = self.get_commit_threshold();
            if self.commit_index < n && self.log[n - 1].term == self.term {
                self.commit_index = n;
            }
        }
        self.advance_state_machine().await;
    }

    pub async fn advance_state_machine(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.log[self.last_applied - 1];
            match &entry.op {
                LinKvRequest::Read {
                    origin: _origin,
                    msg_id,
                    key,
                } => {
                    if self.is_leader() {
                        match self.state_machine.get(key) {
                            Some(value) => {
                                self.node
                                    .send(
                                        entry.src.as_str(),
                                        serde_json::json!({
                                            "type": "read_ok",
                                            "in_reply_to": msg_id,
                                            "value": value,
                                        }),
                                    )
                                    .await;
                            }
                            None => {
                                self.node
                                    .send(
                                        entry.src.as_str(),
                                        serde_json::json!({
                                            "type": "error",
                                            "in_reply_to": msg_id,
                                            "code": 20,
                                            "text": "Key not found.",
                                        }),
                                    )
                                    .await;
                            }
                        }
                    }
                }
                LinKvRequest::Write {
                    origin: _origin,
                    msg_id,
                    key,
                    value,
                } => {
                    self.state_machine.insert(key.clone(), value.clone());
                    if self.is_leader() {
                        self.node
                            .send(
                                entry.src.as_str(),
                                serde_json::json!({
                                    "type": "write_ok",
                                    "in_reply_to": msg_id,
                                }),
                            )
                            .await;
                    }
                }
                LinKvRequest::Cas {
                    origin: _origin,
                    msg_id,
                    key,
                    from,
                    to,
                } => {
                    let cas_ok = if let Some(current_value) = self.state_machine.get(key)
                        && current_value == from
                    {
                        self.state_machine.insert(key.clone(), to.clone());
                        true
                    } else {
                        false
                    };
                    if self.is_leader() {
                        if cas_ok {
                            self.node
                                .send(
                                    entry.src.as_str(),
                                    serde_json::json!({
                                        "type": "cas_ok",
                                        "in_reply_to": msg_id,
                                    }),
                                )
                                .await;
                        } else {
                            self.node
                                .send(
                                    entry.src.as_str(),
                                    serde_json::json!({
                                        "type": "error",
                                        "in_reply_to": msg_id,
                                        "code": 22,
                                        "text": "CAS failed.",
                                    }),
                                )
                                .await;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub fn maybe_step_down(&mut self, remote_term: u64) {
        if remote_term > self.term {
            debug!(
                "Stepping down: remote term {} is higher than current term {}",
                remote_term, self.term
            );
            self.advance_term(remote_term);
            self.become_follower();
        }
    }

    fn refresh_cancellation_token(&mut self) {
        self.cancellation_token.cancel();
        self.cancellation_token = CancellationToken::new();
    }

    fn refresh_election_state(&mut self) {
        self.refresh_cancellation_token();
        self.voted_for = Some(self.node.get_node_id().to_string());
        self.votes.clear();
        self.votes.insert(self.node.get_node_id().to_string());
    }

    pub async fn request_votes(&mut self) {
        let last_log_index = self.last_index();
        let last_log_term = self.last_term();

        let raft_sender = self.raft_sender.clone();
        let peers = self.node.get_membership().node_ids.len() - 1;
        let (tx, mut rx) = mpsc::channel::<Message>(peers);

        debug!(
            "Requesting votes from peers, term: {}, last_log_index: {}, last_log_term: {}",
            self.term, last_log_index, last_log_term
        );

        self.refresh_election_state();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            let mut received = 0;
            while received < peers {
                #[cfg(debug_assertions)]
                eprintln!("Waiting for vote responses...");
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                    message = rx.recv() => {
                        match message {
                            Some(message) => {
                                #[cfg(debug_assertions)]
                                eprintln!("Received vote response from {}", message.src);
                                received += 1;
                                let res = serde_json::from_value::<LinKvRequest>(message.body);
                                if let Ok(res) = res {
                                    let _ = raft_sender.send(RaftRequest {
                                        src: message.src,
                                        request: res,
                                        response_tx: None,
                                    }).await;
                                }
                            }
                            None => {
                                #[cfg(debug_assertions)]
                                eprintln!("Vote response channel closed");
                                break;
                            }
                        }
                    }
                }
            }
            #[cfg(debug_assertions)]
            eprintln!("Finished processing vote responses.");
        });

        self.node
            .brpc_with_callback(
                serde_json::json!(
                    {
                        "type": "request_vote",
                        "term": self.term,
                        "candidate_id": self.node.get_node_id(),
                        "last_log_index": last_log_index,
                        "last_log_term": last_log_term,
                    }
                ),
                tx.clone(),
            )
            .await;
    }

    async fn replicate_log(&mut self, force: bool) {
        let elapsed_time = Instant::now().duration_since(self.last_replication);
        let mut replicated = false;

        if self.is_leader() && (elapsed_time >= self.min_replication_interval || force) {
            debug!("Replication interval reached, replicating logs to followers");
            for node_id in self.node.get_membership().node_ids.iter() {
                if node_id == self.node.get_node_id() {
                    continue;
                }

                let next_index = *self.next_index.get(node_id).unwrap();
                if next_index > self.log.len() {
                    continue;
                }
                if next_index <= self.log.entries.len() || elapsed_time >= self.heartbeat_interval {
                    replicated = true;
                    debug!("Replicating log #{}+ to {}", next_index, node_id);
                    let rx = self
                        .node
                        .rpc(
                            node_id,
                            serde_json::json!({
                                "type": "append_entries",
                                "term": self.term,
                                "leader_id": self.node.get_node_id(),
                                "prev_log_index": next_index - 1 ,
                                "prev_log_term": self.log[next_index - 1].term,
                                "entries": self.log.entries[next_index - 1..], // entries,
                                "leader_commit": self.commit_index,
                            }),
                        )
                        .await;
                    let raft_sender = self.raft_sender.clone();
                    tokio::spawn(async move {
                        match rx.await {
                            Ok(message) => {
                                let res = serde_json::from_value::<LinKvRequest>(message.body);
                                if let Ok(res) = res {
                                    let _ = raft_sender
                                        .send(RaftRequest {
                                            src: message.src,
                                            request: res,
                                            response_tx: None,
                                        })
                                        .await;
                                }
                            }
                            Err(_) => {
                                debug!("Vote response channel closed");
                            }
                        }
                    });
                }
            }
        }
        if replicated {
            self.last_replication = Instant::now();
        }
    }

    pub async fn handle_request(&mut self, request: RaftRequest) {
        let response_sender = request.response_tx;
        let mut force_replicate = false;
        match &request.request {
            LinKvRequest::Init {
                node_id: _node_id,
                node_ids: _node_ids,
            } => {
                let raft_sender = self.raft_sender.clone();
                let src = request.src.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(rand::random_range(100..=200)))
                            .await;
                        let _ = raft_sender
                            .send(RaftRequest {
                                src: src.clone(),
                                request: LinKvRequest::StartElection {},
                                response_tx: None,
                            })
                            .await;
                    }
                });
                let raft_sender = self.raft_sender.clone();
                let src = request.src.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        let _ = raft_sender
                            .send(RaftRequest {
                                src: src.clone(),
                                request: LinKvRequest::CheckStepDown {},
                                response_tx: None,
                            })
                            .await;
                    }
                });
                let raft_sender = self.raft_sender.clone();
                let src = request.src.clone();
                let min_replication_interval = self.min_replication_interval;
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(min_replication_interval).await;
                        let _ = raft_sender
                            .send(RaftRequest {
                                src: src.clone(),
                                request: LinKvRequest::ReplicateLog {},
                                response_tx: None,
                            })
                            .await;
                    }
                });
            }
            LinKvRequest::Read {
                origin: _origin,
                msg_id,
                key,
            } => {
                debug!("Received read request from {} for key {}", request.src, key);
                if self.is_leader() {
                    self.log.append(RaftLogEntry {
                        term: self.get_term(),
                        src: request.src.clone(),
                        op: request.request.clone(),
                    });
                    force_replicate = true;
                } else if let Some(leader) = &self.leader {
                    self.node
                        .send(
                            leader,
                            serde_json::json!({
                                "type": "read",
                                "msg_id": msg_id,
                                "key": key,
                                "origin": request.src.clone(),
                            }),
                        )
                        .await;
                } else {
                    self.node
                        .send(
                            request.src.as_str(),
                            serde_json::json!({
                                "type": "error",
                                "in_reply_to": msg_id,
                                "code": 11,
                                "text": "No leader elected.",
                            }),
                        )
                        .await;
                }
            }
            LinKvRequest::Write {
                origin: _origin,
                msg_id,
                key,
                value,
            } => {
                if self.is_leader() {
                    force_replicate = true;
                    debug!(
                        "Received write request from {} for key {}, value {}",
                        request.src, key, value
                    );
                    self.log.append(RaftLogEntry {
                        term: self.get_term(),
                        src: request.src.clone(),
                        op: request.request.clone(),
                    });
                } else if let Some(leader) = &self.leader {
                    self.node
                        .send(
                            leader,
                            serde_json::json!({
                                "type": "write",
                                "key": key,
                                "value": value,
                                "msg_id": msg_id,
                                "origin": request.src.clone(),
                            }),
                        )
                        .await;
                } else {
                    self.node
                        .send(
                            request.src.as_str(),
                            serde_json::json!({
                                "type": "error",
                                "in_reply_to": msg_id,
                                "code": 11,
                                "text": "No leader elected.",
                            }),
                        )
                        .await;
                }
            }
            LinKvRequest::Cas {
                origin: _origin,
                msg_id,
                key,
                from,
                to,
            } => {
                if self.is_leader() {
                    force_replicate = true;
                    debug!(
                        "Received cas request from {} for key {}, from {}, to {}",
                        request.src, key, from, to
                    );
                    self.log.append(RaftLogEntry {
                        term: self.get_term(),
                        src: request.src.clone(),
                        op: request.request.clone(),
                    });
                } else if let Some(leader) = &self.leader {
                    self.node
                        .send(
                            leader,
                            serde_json::json!({
                                "type": "cas",
                                "key": key,
                                "from": from,
                                "to": to,
                                "msg_id": msg_id,
                                "origin": request.src.clone(),
                            }),
                        )
                        .await;
                } else {
                    self.node
                        .send(
                            request.src.as_str(),
                            serde_json::json!({
                                "type": "error",
                                "in_reply_to": msg_id,
                                "code": 11,
                                "text": "No leader elected.",
                            }),
                        )
                        .await;
                }
            }
            LinKvRequest::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                debug!(
                    "Received vote request from {} for term {}",
                    candidate_id, term
                );
                self.maybe_step_down(*term);

                let mut vote_granted = false;
                if *term < self.get_term() {
                    debug!(
                        "Candidate term {} is less than current term {}",
                        term,
                        self.get_term()
                    );
                } else if self.is_voted() && self.get_voted_for() != *candidate_id {
                    debug!("Already voted for {}", self.get_voted_for());
                } else if !self
                    .is_candidate_log_up_to_date(*last_log_index as usize, *last_log_term)
                {
                    debug!(
                        "Our logs are both at term {}, but candidate's log is not up-to-date",
                        last_log_term
                    );
                } else {
                    debug!("Voted for candidate {}", candidate_id);
                    vote_granted = true;
                    self.set_voted_for(candidate_id.clone());
                    self.reset_election_deadline();
                }

                if let Some(sender) = response_sender {
                    sender
                        .send(Ok(serde_json::json!({
                            "type": "request_vote_res",
                            "term": self.get_term(),
                            "vote_granted": vote_granted,
                        })))
                        .unwrap();
                }
            }
            LinKvRequest::StartElection {} => {
                debug!("Received periodic election check");
                if self.passed_election_deadline() {
                    debug!("Election deadline passed, starting election");
                    if self.is_leader() {
                        self.reset_election_deadline();
                    } else {
                        self.become_candidate().await;
                    }
                }
            }
            LinKvRequest::RequestVoteRes { term, vote_granted } => {
                debug!("Received vote response for term {}: {}", term, vote_granted);
                self.reset_step_down_deadline();
                self.maybe_step_down(*term);
                if self.is_candidate() && self.term == *term && *vote_granted {
                    debug!("Received vote from {}", request.src);
                    self.votes.insert(request.src);
                    if self.votes.len() >= majority(self.node.get_membership().node_ids.len()) {
                        debug!("Becoming leader for term {}", self.term);
                        self.become_leader();
                    }
                }
            }
            LinKvRequest::CheckStepDown {} => {
                if self.is_leader() && Instant::now() >= self.step_down_deadline {
                    debug!("Stepping down: haven't received acks from followers recently");
                    self.become_follower();
                }
            }
            LinKvRequest::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                debug!(
                    "Received append entries from leader {} for term {}, prev_log_index {}, prev_log_term {}, leader_commit {}",
                    leader_id, term, prev_log_index, prev_log_term, leader_commit
                );
                debug!(
                    "Current term {}, log length {}",
                    self.get_term(),
                    self.log.len()
                );

                self.maybe_step_down(*term);

                if *term < self.get_term() {
                    if let Some(response_sender) = response_sender {
                        response_sender
                            .send(Ok(serde_json::json!({
                                "type": "append_entries_res",
                                "term": self.get_term(),
                                "success": false,
                                "next_index": 0,
                            })))
                            .unwrap();
                    }
                    return;
                }

                self.leader = Some(leader_id.clone());
                self.reset_election_deadline();

                if self.log.len() <= *prev_log_index as usize
                    || (*prev_log_index > 0
                        && self.log[*prev_log_index as usize - 1].term != *prev_log_term)
                {
                    if let Some(response_sender) = response_sender {
                        response_sender
                            .send(Ok(serde_json::json!({
                                "type": "append_entries_res",
                                "term": self.get_term(),
                                "success": false,
                                "next_index": 1,
                            })))
                            .unwrap();
                    }
                    return;
                }

                let mut log_insert_index = *prev_log_index as usize;
                for entry in entries {
                    if log_insert_index >= self.log.len() {
                        self.log.append(entry.clone());
                    } else if self.log[log_insert_index].term != entry.term {
                        self.log.truncate(log_insert_index);
                        self.log.append(entry.clone());
                    }
                    log_insert_index += 1;
                }

                if *leader_commit as usize > self.commit_index {
                    self.commit_index = std::cmp::min(*leader_commit as usize, self.log.len());
                    self.advance_state_machine().await;
                }

                if let Some(response_sender) = response_sender {
                    response_sender
                        .send(Ok(serde_json::json!({
                                "type": "append_entries_res",
                                "term": self.get_term(),
                                "success": true,
                                "next_index": self.log.len() + 1,
                        })))
                        .unwrap();
                }
            }
            LinKvRequest::AppendEntriesRes {
                term,
                success,
                next_index,
            } => {
                debug!(
                    "Received append entries response for term {}: success={}, next_index {}",
                    term, success, next_index
                );
                self.maybe_step_down(*term);
                if self.is_leader() && self.term == *term {
                    self.reset_step_down_deadline();
                    if *success {
                        self.next_index
                            .entry(request.src.clone())
                            .and_modify(|index| {
                                *index = *next_index as usize;
                            });
                        self.match_index.entry(request.src).and_modify(|index| {
                            *index = *next_index as usize - 1;
                        });
                        self.advance_commit_index().await;
                    } else {
                        self.next_index.entry(request.src).and_modify(|index| {
                            if *index > 0 {
                                *index -= 1;
                            }
                        });
                    }
                }
            }
            LinKvRequest::ReplicateLog {} => {
                self.replicate_log(false).await;
                return;
            }
        }
        self.replicate_log(force_replicate).await;
    }
}

pub struct RaftLog {
    entries: Vec<RaftLogEntry>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct RaftLogEntry {
    term: u64,
    src: String,
    op: LinKvRequest,
}

impl RaftLog {
    pub fn new() -> Self {
        RaftLog {
            entries: vec![RaftLogEntry {
                term: 0,
                src: "".to_string(),
                op: LinKvRequest::Init {
                    node_id: "".to_string(),
                    node_ids: vec![],
                },
            }],
        }
    }

    pub fn append(&mut self, entry: RaftLogEntry) {
        self.entries.push(entry);
    }

    pub fn append_entries(&mut self, entries: Vec<RaftLogEntry>) {
        self.entries.extend(entries);
    }

    pub fn last(&self) -> Option<&RaftLogEntry> {
        self.entries.last()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn truncate(&mut self, index: usize) {
        self.entries.truncate(index);
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Index<usize> for RaftLog {
    type Output = RaftLogEntry;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl IndexMut<usize> for RaftLog {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
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
                    .set(Raft::start(node.clone()))
                    .unwrap_or_default();
                debug!("Raft instance started.");
                let _ = self
                    .raft_sender
                    .get()
                    .unwrap()
                    .send(RaftRequest {
                        src: message.src.clone(),
                        request: body,
                        response_tx: None,
                    })
                    .await;
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

fn main() {
    init_logger();
    let node = Arc::new(Node::new());
    node.set_handler(Arc::new(RaftHandler::new()));
    node.run();
}
