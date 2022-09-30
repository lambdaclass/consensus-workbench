use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum PingMessage {
    Ping,
    Pong,
    Other(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyValueCommand {
    Set { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
}
