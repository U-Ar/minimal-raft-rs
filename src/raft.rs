use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::{Index, IndexMut},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::Instant,
};

#[async_trait]
pub trait StateMachine: Sync + Send {
    async fn apply(&mut self, command: Vec<u8>) -> Result<Vec<u8>, RaftError>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransportError {
    pub message: String,
}

#[async_trait]
pub trait Transport: Sync + Send {
    async fn send(&self, target: &str, message: RaftMessage) -> Result<(), TransportError>;
    async fn broadcast(
        &self,
        targets: &[String],
        message: RaftMessage,
    ) -> Result<(), TransportError>;
}

fn majority(count: usize) -> usize {
    (count / 2) + 1
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RaftError {
    Timeout(String),
    NodeNotFound(String),
    NotSupported(String),
    NoLeader(String),
    MalformedRequest(String),
    Crash(String),
    Abort(String),
    KeyDoesNotExist(String),
    KeyAlreadyExists(String),
    PreconditionFailed(String),
    Redirect { leader_id: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum RaftMessage {
    Internal(InternalMessage),
    RPC(RPCMessage),
    Client(ClientRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum InternalMessage {
    StartElection,
    CheckStepDown,
    ReplicateLog,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RPCMessage {
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: usize,
        last_log_term: u64,
    },
    RequestVoteRes {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: usize,
    },
    AppendEntriesRes {
        term: u64,
        success: bool,
        next_index: usize,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ClientRequest {
    Command { op: Vec<u8> },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum RaftResponse {
    RPC(RPCMessage),
    Client { result: Vec<u8> },
}

pub struct RaftRequest {
    pub src: String,
    pub request: RaftMessage,
    pub response_tx: Option<oneshot::Sender<Result<RaftResponse, RaftError>>>,
}

struct PendingRequest {
    log_index: usize,
    response_tx: oneshot::Sender<Result<RaftResponse, RaftError>>,
}

pub struct ClusterConfig {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub struct Raft {
    config: ClusterConfig,

    // ハートビート・タイムアウト
    election_timeout: Duration,         // 選挙タイムアウト
    heartbeat_interval: Duration,       // ハートビート間隔
    min_replication_interval: Duration, // 最小複製間隔

    election_deadline: Instant,  // 選挙開始タイミング
    step_down_deadline: Instant, // リーダー降格タイミング
    last_replication: Instant,   // 最後のログ複製時刻

    // 入出力
    transport: Arc<dyn Transport + Send + Sync>,
    raft_sender: mpsc::Sender<RaftRequest>,

    // 選挙状態
    state: State,
    term: u64,
    voted_for: Option<String>,
    votes: HashSet<String>,

    // リーダー状態
    leader: Option<String>,
    commit_index: usize,
    next_index: HashMap<String, usize>,
    match_index: HashMap<String, usize>,

    state_machine: Arc<Mutex<dyn StateMachine + Send + Sync>>,
    pending_requests: VecDeque<PendingRequest>,
    last_applied: usize,
    log: RaftLog,
}

enum State {
    Follower,
    Candidate,
    Leader,
}

impl Raft {
    pub fn start(
        config: ClusterConfig,
        transport: Arc<dyn Transport + Send + Sync>,
        state_machine: Arc<Mutex<dyn StateMachine + Send + Sync>>,
    ) -> mpsc::Sender<RaftRequest> {
        let (tx0, mut rx0) = mpsc::channel::<RaftRequest>(100);
        let tx1 = tx0.clone();
        info!("Raft instance handling starting...");
        tokio::spawn(async move {
            let mut raft = Raft::new(config, transport, state_machine, tx0);
            raft.start_routines().await;
            loop {
                debug!("Waiting for Raft request...");
                if let Some(message) = rx0.recv().await {
                    debug!("Received Raft request");
                    let mut requests = vec![message];
                    while let Ok(message) = rx0.try_recv() {
                        requests.push(message);
                    }
                    let _ = raft.handle_request(requests).await;
                }
            }
        });
        tx1
    }

    pub fn new(
        config: ClusterConfig,
        transport: Arc<dyn Transport + Send + Sync>,
        state_machine: Arc<Mutex<dyn StateMachine + Send + Sync>>,
        raft_sender: mpsc::Sender<RaftRequest>,
    ) -> Self {
        Raft {
            config,
            election_timeout: Duration::from_secs(2),
            heartbeat_interval: Duration::from_secs(1),
            min_replication_interval: Duration::from_millis(50),
            election_deadline: Instant::now(),
            step_down_deadline: Instant::now(),
            last_replication: Instant::now(),
            transport,
            raft_sender,
            state: State::Follower,
            term: 0,
            voted_for: None,
            votes: HashSet::new(),
            leader: None,
            commit_index: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            state_machine,
            pending_requests: VecDeque::new(),
            last_applied: 1,
            log: RaftLog::new(),
        }
    }

    async fn start_routines(&mut self) {
        let raft_sender = self.raft_sender.clone();
        let node_id = self.config.node_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(rand::random_range(100..=200))).await;
                let _ = raft_sender
                    .send(RaftRequest {
                        src: node_id.clone(),
                        request: RaftMessage::Internal(InternalMessage::StartElection {}),
                        response_tx: None,
                    })
                    .await;
            }
        });
        let raft_sender = self.raft_sender.clone();
        let node_id = self.config.node_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = raft_sender
                    .send(RaftRequest {
                        src: node_id.clone(),
                        request: RaftMessage::Internal(InternalMessage::CheckStepDown {}),
                        response_tx: None,
                    })
                    .await;
            }
        });
        let raft_sender = self.raft_sender.clone();
        let node_id = self.config.node_id.clone();
        let min_replication_interval = self.min_replication_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(min_replication_interval).await;
                let _ = raft_sender
                    .send(RaftRequest {
                        src: node_id.clone(),
                        request: RaftMessage::Internal(InternalMessage::ReplicateLog {}),
                        response_tx: None,
                    })
                    .await;
            }
        });
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
        self.pending_requests.clear();

        self.reset_election_deadline();

        debug!("Became follower for term {}", self.term);
    }

    pub fn become_leader(&mut self) {
        self.state = State::Leader;
        self.leader = Some(self.config.node_id.clone());
        self.last_replication = Instant::now() - Duration::from_secs(1);

        self.next_index.clear();
        self.match_index.clear();
        for node_id in &self.config.node_ids {
            if node_id != &self.config.node_id {
                self.next_index.insert(node_id.clone(), self.last_index());
                self.match_index.insert(node_id.clone(), 0);
            }
        }

        self.reset_step_down_deadline();
        debug!("Became leader for term {}", self.term);
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
            let command = entry.op.clone();
            let state_machine_result = self.state_machine.lock().await.apply(command).await;
            if self.is_leader() {
                while let Some(pending_request) = self.pending_requests.front() {
                    if pending_request.log_index >= self.last_applied {
                        break;
                    } else {
                        self.pending_requests.pop_front();
                    }
                }
                if let Some(pending_request) = self.pending_requests.front()
                    && pending_request.log_index == self.last_applied
                {
                    let pending_request = self.pending_requests.pop_front().unwrap();
                    match state_machine_result {
                        Ok(result) => {
                            let _ = pending_request
                                .response_tx
                                .send(Ok(RaftResponse::Client { result }));
                        }
                        Err(e) => {
                            let _ = pending_request.response_tx.send(Err(e));
                        }
                    };
                }
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

    fn refresh_election_state(&mut self) {
        self.voted_for = Some(self.config.node_id.clone());
        self.votes.clear();
        self.votes.insert(self.config.node_id.clone());
    }

    pub async fn request_votes(&mut self) {
        let last_log_index = self.last_index();
        let last_log_term = self.last_term();

        debug!(
            "Requesting votes from peers, term: {}, last_log_index: {}, last_log_term: {}",
            self.term, last_log_index, last_log_term
        );

        self.refresh_election_state();

        let result = self
            .transport
            .broadcast(
                &self
                    .config
                    .node_ids
                    .iter()
                    .filter(|id| *id != &self.config.node_id)
                    .cloned()
                    .collect::<Vec<String>>(),
                RaftMessage::RPC(RPCMessage::RequestVote {
                    term: self.term,
                    candidate_id: self.config.node_id.clone(),
                    last_log_index,
                    last_log_term,
                }),
            )
            .await;
        if let Err(e) = result {
            warn!("Failed to broadcast RequestVote RPC: {}", e.message);
        }
    }

    async fn replicate_log(&mut self, force: bool) {
        let elapsed_time = Instant::now().duration_since(self.last_replication);
        let mut replicated = false;

        if self.is_leader() && (elapsed_time >= self.min_replication_interval || force) {
            debug!("Replication interval reached, replicating logs to followers");
            for node_id in self.config.node_ids.iter() {
                if node_id == &self.config.node_id {
                    continue;
                }

                let next_index = *self.next_index.get(node_id).unwrap();
                if next_index > self.log.len() {
                    continue;
                }
                if next_index <= self.log.entries.len()
                    || elapsed_time >= self.heartbeat_interval
                    || force
                {
                    replicated = true;
                    debug!("Replicating log #{}+ to {}", next_index, node_id);
                    let result = self
                        .transport
                        .send(
                            node_id,
                            RaftMessage::RPC(RPCMessage::AppendEntries {
                                term: self.get_term(),
                                leader_id: self.config.node_id.clone(),
                                prev_log_index: next_index - 1,
                                prev_log_term: self.log[next_index - 1].term,
                                entries: self.log.entries[next_index - 1..].to_vec(),
                                leader_commit: self.commit_index,
                            }),
                        )
                        .await;
                    if let Err(e) = result {
                        warn!("Failed to send AppendEntries to {}: {}", node_id, e.message);
                    }
                }
            }
        }
        if replicated {
            self.last_replication = Instant::now();
        }
    }

    pub async fn handle_request(&mut self, requests: Vec<RaftRequest>) {
        let mut force_replicate = false;
        for request in requests {
            let response_sender = request.response_tx;
            match &request.request {
                RaftMessage::Internal(internal) => match internal {
                    InternalMessage::StartElection => {
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
                    InternalMessage::CheckStepDown => {
                        if self.is_leader() && Instant::now() >= self.step_down_deadline {
                            debug!("Stepping down: haven't received acks from followers recently");
                            self.become_follower();
                        }
                    }
                    InternalMessage::ReplicateLog => {
                        self.replicate_log(false).await;
                        continue;
                    }
                },
                RaftMessage::Client(client_request) => match client_request {
                    ClientRequest::Command { op } => {
                        if self.is_leader() {
                            self.log.append(RaftLogEntry {
                                term: self.get_term(),
                                op: op.clone(),
                            });
                            if let Some(sender) = response_sender {
                                self.pending_requests.push_back(PendingRequest {
                                    log_index: self.last_index(),
                                    response_tx: sender,
                                });
                            }
                            force_replicate = true;
                        } else if let Some(leader) = &self.leader {
                            if let Some(response_sender) = response_sender {
                                let _ = response_sender.send(Err(RaftError::Redirect {
                                    leader_id: leader.clone(),
                                }));
                            }
                        } else if let Some(response_sender) = response_sender {
                            let _ = response_sender
                                .send(Err(RaftError::NoLeader("No leader elected".to_string())));
                        }
                    }
                },
                RaftMessage::RPC(rpc_request) => match rpc_request {
                    RPCMessage::RequestVote {
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
                        } else if !self.is_candidate_log_up_to_date(*last_log_index, *last_log_term)
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
                                .send(Ok(RaftResponse::RPC(RPCMessage::RequestVoteRes {
                                    term: self.get_term(),
                                    vote_granted,
                                })))
                                .unwrap();
                        }
                    }
                    RPCMessage::RequestVoteRes { term, vote_granted } => {
                        debug!("Received vote response for term {}: {}", term, vote_granted);
                        self.reset_step_down_deadline();
                        self.maybe_step_down(*term);
                        if self.is_candidate() && self.term == *term && *vote_granted {
                            debug!("Received vote from {}", request.src);
                            self.votes.insert(request.src);
                            if self.votes.len() >= majority(self.config.node_ids.len()) {
                                debug!("Becoming leader for term {}", self.term);
                                self.become_leader();
                            }
                        }
                    }
                    RPCMessage::AppendEntries {
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
                                    .send(Ok(RaftResponse::RPC(RPCMessage::AppendEntriesRes {
                                        term: self.get_term(),
                                        success: false,
                                        next_index: 0,
                                    })))
                                    .unwrap();
                            }
                            continue;
                        }

                        self.leader = Some(leader_id.clone());
                        self.reset_election_deadline();

                        if self.log.len() < *prev_log_index
                            || (*prev_log_index > 0
                                && self.log[*prev_log_index - 1].term != *prev_log_term)
                        {
                            if let Some(response_sender) = response_sender {
                                response_sender
                                    .send(Ok(RaftResponse::RPC(RPCMessage::AppendEntriesRes {
                                        term: self.get_term(),
                                        success: false,
                                        next_index: 1,
                                    })))
                                    .unwrap();
                            }
                            continue;
                        }

                        let mut log_insert_index = *prev_log_index;
                        for entry in entries {
                            if log_insert_index >= self.log.len() {
                                self.log.append(entry.clone());
                            } else if self.log[log_insert_index].term != entry.term {
                                self.log.truncate(log_insert_index);
                                self.log.append(entry.clone());
                            }
                            log_insert_index += 1;
                        }

                        if *leader_commit > self.commit_index {
                            self.commit_index = std::cmp::min(*leader_commit, self.log.len());
                            self.advance_state_machine().await;
                        }

                        if let Some(response_sender) = response_sender {
                            response_sender
                                .send(Ok(RaftResponse::RPC(RPCMessage::AppendEntriesRes {
                                    term: self.get_term(),
                                    success: true,
                                    next_index: self.log.len() + 1,
                                })))
                                .unwrap();
                        }
                    }
                    RPCMessage::AppendEntriesRes {
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
                                        *index = *next_index;
                                    });
                                self.match_index.entry(request.src).and_modify(|index| {
                                    *index = *next_index - 1;
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
                },
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
    op: Vec<u8>,
}

impl RaftLog {
    pub fn new() -> Self {
        RaftLog {
            entries: vec![RaftLogEntry {
                term: 0,
                op: vec![],
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
