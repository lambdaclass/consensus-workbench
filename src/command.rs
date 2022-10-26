use crate::network::ReliableSender;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::fmt;

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub type CommandResult = Result<Option<String>, String>;

#[derive(Debug, Serialize, Deserialize, Parser, Clone, PartialEq, Eq)]
#[clap()]
pub enum ClientCommand {
    // user-generated commands
    Set { key: String, value: String },
    Get { key: String },
}

impl ClientCommand {
    /// Send this command over to a server at the given address and return the response.
    pub async fn send_to(self, address: SocketAddr) -> Result<Option<String>> {
        let mut sender = ReliableSender::new();

        let message: Bytes = bincode::serialize(&(self))?.into();
        let reply_handler = sender.send(address, message).await;

        let response = reply_handler.await?;
        let response: CommandResult = bincode::deserialize(&response)?;
        response.map_err(|e| anyhow!(e))
    }
}

impl fmt::Display for ClientCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
