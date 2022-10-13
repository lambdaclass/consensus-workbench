use crate::network::ReliableSender;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::info;
use std::fmt;

use clap::Parser;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
<<<<<<< HEAD
use clap::Parser;

<<<<<<< HEAD
#[derive(Debug, Serialize, Deserialize, Parser, Clone, PartialEq, Eq)]
#[clap()]
=======
=======
>>>>>>> 8cce6d6 (quorum commits work, have to rework tests)

#[derive(Debug, Serialize, Deserialize, Clone)]
>>>>>>> 8a1ec05 (replicate)
pub enum Command {
    Client(ClientCommand),
    Network(NetworkCommand),
}

#[derive(Debug, Serialize, Deserialize, Clone, Parser, PartialEq)]
#[clap()]
pub enum ClientCommand {
    // user-generated commands
    Set { key: String, value: String },
    Get { key: String },
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
        view: u128,
    },
    ViewChange {
        new_view: u128,
    },
}

impl ClientCommand {
    /// Send this command over to a server at the given address and return the response.
    pub async fn send_to(self, address: SocketAddr) -> Result<Option<String>> {
        let mut sender = ReliableSender::new();

        let message: Bytes = bincode::serialize(&Command::Client(self))?.into();
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
