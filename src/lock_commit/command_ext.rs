use bytes::Bytes;
use core::fmt;
use std::net::SocketAddr;

use lib::{command::ClientCommand, network::ReliableSender};
use serde::{Deserialize, Serialize};

use anyhow::{anyhow, Result};

/// The Command enum represents any command that can be sent to the lock_commit node
/// It can either be a ClientCommand (from the shared lib), or a NetworkCommand
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Command {
    Client(ClientCommand),
    Network(NetworkCommand),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NetworkCommand {
    Propose {
        command_view: CommandView,
    },
    Lock {
        socket_addr: SocketAddr,
        command_view: CommandView,
    },
    Commit {
        command_view: CommandView,
    },

    // view change
    Blame {
        socket_addr: SocketAddr,
        view: u128,
        timer_expired: bool,
    },
    ViewChange {
        socket_addr: SocketAddr,
        new_view: u128,
        highest_lock: CommandView,
    },
}

impl Command {
    /// Send this command over to a server at the given address and return the response.
    pub async fn send_to(self, address: SocketAddr) -> Result<Option<String>> {
        let mut sender = ReliableSender::new();

        let message: Bytes = bincode::serialize(&self)?.into();
        let reply_handler = sender.send(address, message).await;

        let response = reply_handler.await?;
        let response: Result<Option<String>, String> = bincode::deserialize(&response)?;
        response.map_err(|e| anyhow!(e))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CommandView {
    pub command: ClientCommand, // lock_value
    pub view: u128,             // lock_view
}

impl CommandView {
    pub fn new() -> CommandView {
        CommandView {
            command: ClientCommand::Get {
                key: "-".to_string(),
            },
            view: 0,
        }
    }
}
