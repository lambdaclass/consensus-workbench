use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum PingMessage {
    Ping,
    Pong,
    Other(String)
}
