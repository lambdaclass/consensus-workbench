use std::fmt;
use std::net::SocketAddr;

use lib::command::ClientCommand;
use serde::{Deserialize, Serialize};

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
    Forward {
        command: ClientCommand,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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

impl fmt::Display for NetworkCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
