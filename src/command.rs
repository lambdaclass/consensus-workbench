use crate::network::ReliableSender;
use anyhow::{anyhow, Result};
use std::fmt;

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Serialize, Deserialize, Parser, Clone)]
#[clap()]
pub enum Command {
    // node-generated commands (TODO: this could be part of a different enum)
    SyncSet { key: String, value: String },
    // user-generated commands
    Set { key: String, value: String },
    Get { key: String },
}

impl Command {
    /// Send this command over to a server at the given address and return the response.
    pub async fn send_to(&self, address: SocketAddr) -> Result<Option<String>> {
        let mut sender = ReliableSender::new();

        let reply_handler = sender.send(address, self).await;

        let response = reply_handler.await?;
        let response: Result<Option<String>, String> = bincode::deserialize(&response)?;
        response.map_err(|e| anyhow!(e))
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
