use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PingMessage {
    Ping,
    Pong,
    Other(String),
}
