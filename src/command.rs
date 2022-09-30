use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PingMessage {
    Ping,
    Pong,
    Other(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyValueCommand {
    Set { key: String, value: String },
    Get { key: String },
}
