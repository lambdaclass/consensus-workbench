use core::fmt;
use std::{net::SocketAddr};
use bytes::Bytes;

use lib::{network::ReliableSender, command::ClientCommand};
use serde::{Serialize, Deserialize};

use anyhow::{anyhow, Result};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Command {
    Client(ClientCommand),
    Network(NetworkCommand),
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NetworkCommand {
    Propose {
        command_view: CommandView,
    }, // todo: the address should maybe be taken from writer's sink fields? or maybe generalized in dispatch()
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
        timer_expired: bool
    },
    ViewChange {
        socket_addr: SocketAddr,
        new_view: u128,
        highest_lock: CommandView
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


impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
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
