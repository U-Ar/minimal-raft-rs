use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::raft::{RaftError, StateMachine};

pub struct HashMapStateMachine {
    map: HashMap<u64, u64>,
}

impl HashMapStateMachine {
    pub fn new() -> Self {
        HashMapStateMachine {
            map: HashMap::new(),
        }
    }
}

impl Default for HashMapStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum HashMapStateMachineCommand {
    Read { key: u64 },
    Write { key: u64, value: u64 },
    Cas { key: u64, from: u64, to: u64 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum HashMapStateMachineResponse {
    ReadOk { value: u64 },
    WriteOk,
    CasOk,
}

#[async_trait]
impl StateMachine for HashMapStateMachine {
    async fn apply(&mut self, command: Vec<u8>) -> Result<Vec<u8>, RaftError> {
        let command: HashMapStateMachineCommand = match serde_json::from_slice(&command) {
            Ok(cmd) => cmd,
            Err(e) => {
                return Err(RaftError::MalformedRequest(format!(
                    "Failed to deserialize command: {}",
                    e
                )));
            }
        };

        match command {
            HashMapStateMachineCommand::Read { key } => {
                if let Some(value) = self.map.get(&key) {
                    Ok(
                        serde_json::to_vec(&HashMapStateMachineResponse::ReadOk { value: *value })
                            .unwrap(),
                    )
                } else {
                    Err(RaftError::KeyDoesNotExist(format!(
                        "Key {} does not exist",
                        key
                    )))
                }
            }
            HashMapStateMachineCommand::Write { key, value } => {
                self.map.insert(key, value);
                let response = HashMapStateMachineResponse::WriteOk;
                Ok(serde_json::to_vec(&response).unwrap())
            }
            HashMapStateMachineCommand::Cas { key, from, to } => {
                if let Some(current_value) = self.map.get(&key) {
                    if *current_value == from {
                        self.map.insert(key, to);
                        let response = HashMapStateMachineResponse::CasOk;
                        Ok(serde_json::to_vec(&response).unwrap())
                    } else {
                        Err(RaftError::PreconditionFailed(format!(
                            "CAS failed: expected {}, found {}",
                            from, current_value
                        )))
                    }
                } else {
                    Err(RaftError::KeyDoesNotExist(format!(
                        "Key {} does not exist",
                        key
                    )))
                }
            }
        }
    }
}
