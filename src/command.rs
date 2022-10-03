use crate::network::ReliableSender;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Serialize, Deserialize, Parser)]
#[clap()]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
}

pub async fn execute(command: Command, address: SocketAddr) -> Result<Option<String>> {
    let mut sender = ReliableSender::new();

    let message: Bytes = bincode::serialize(&command)?.into();
    let reply_handler = sender.send(address, message).await;

    let response = reply_handler.await?;
    let response: Result<Option<String>, String> = bincode::deserialize(&response)?;
    response.map_err(|e| anyhow!(e))
}
