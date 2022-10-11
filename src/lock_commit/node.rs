/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, ReliableSender, Writer},
    store::Store, command::{CommandView, Command, ClientCommand},
};
use log::info;
use tokio::time::error;
use std::{net::SocketAddr, collections::HashSet};

use lib::command::NetworkCommand;


#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub socket_address: SocketAddr,
    pub state: State,
    pub store: Store,
    pub peers: Vec<SocketAddr>,
    pub sender: ReliableSender,

    pub current_primary_index: usize,

    pub current_view: u128,
    command_view_lock: CommandView,

    // the amount of peers which responded with "Lock"
    // note: if we were to create a QC to store in the blockchain, 
    // we would need to store signatures from peers here
    pub lock_responses: HashSet<SocketAddr>
}

/// The state of a node viewed as a state-machine.
#[derive(Clone, Copy)]
pub enum State {
    Primary,
    Backup,
}

use ClientCommand::*;
use Message::*;
use State::*;

impl Node {
    pub fn new(peers: Vec<SocketAddr>, db_path: &str, address: SocketAddr) -> Self {
        Self {
            state: if address == *peers.get(0).unwrap() { Primary } else { Backup },
            store: Store::new(db_path).unwrap(),
            peers: peers,
            sender: ReliableSender::new(),
            current_view: 0,
            command_view_lock: CommandView::new(),
            lock_responses: HashSet::new(),
            current_primary_index: 0,
            socket_address: address,
        }
    }

    async fn handle_client_command(&self, command: ClientCommand) -> Result<Option<Vec<u8>>, anyhow::Error> {
        match command {
            ClientCommand::Set { key, value } => self.store.write(key.into(), value.into()).await,
            ClientCommand::Get { key } => self.store.read(key.clone().into()).await,
        }
    }
}

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = Message::deserialize(bytes)?;
        info!("Received request {:?}", request);

        let result = match (self.state, request) {
            (Primary, Command::Client(client_comand)) => {
                let command = NetworkCommand::Propose { command_view: CommandView {
                    command: client_comand,
                    view: self.current_view + 1
                    }
                };
                info!("test");
                self.broadcast(&command); 
                Ok(())
            },
            (Primary, Command::Network(NetworkCommand::Lock {socket_addr, command_view })) => {
                self.lock_responses.insert(socket_addr);
                // broadcast commit, then try commit 
                // TODO: this really is a function of the amount of omission faults we want the network to tolerate, f<n/2
                if self.lock_responses.len() >= ((self.peers.len())/2) + 1 {
                    self.broadcast(&NetworkCommand::Commit { command_view: self.command_view_lock.clone()  }).await;
                }
                Ok(())
            },
            (Backup, Command::Network(NetworkCommand::Propose { command_view })) => {

                self.lock_command_view(&command_view);
                self.send_to_primary(&command_view.command).await;
                Ok(())

            },
            (Backup, Command::Client(client_command)) => {
                self.send_to_primary(&client_command).await;
                Ok(())
            },
            (_, Command::Network(NetworkCommand::Commit { command_view })) => {
                if let Ok(result) = self.try_commit(command_view).await {
                    info!("Committed command, response was {:?}", result.unwrap_or_default());
                    return Ok(());
                }
                Err(anyhow!("Error committing command"))
            },
            _ => Err(anyhow!("Unhandled command")),
        };

        // convert the error into something serializable
        let result = result.map_err(|e| e.to_string());

        info!("Sending response {:?}", result);
        let reply = bincode::serialize(&result)?;
        Ok(writer.send(reply.into()).await?)
    }
}

impl Node {
    async fn broadcast(&mut self, network_command: &NetworkCommand) {
        let message: Bytes = bincode::serialize(network_command).unwrap().into();

        // forward the command to all replicas and wait for them to respond
        let handlers = self
            .sender
            .broadcast(&self.peers, message)
            .await;

        futures::future::join_all(handlers).await;
    }

    async fn send_to_primary(&mut self, cmd: &ClientCommand) {
        let message: Bytes = bincode::serialize(cmd).unwrap().into();

        // forward the command to all replicas and wait for them to respond
        self
            .sender
            .send(*(self.peers.get(self.current_primary_index).expect("Error getting primary")), message)
            .await;
    }

    async fn try_commit(&mut self, command_view: CommandView) -> Result<Option<Vec<u8>>> {
        // handle command, remove command lock
        if self.command_view_lock != command_view {
            // we are trying to commit something that has not been locked correctly, 
            // so there must have been some fault
            return Err(anyhow!("Trying to commit a command-view that had not been previously locked"));
        }

        // handle command
        self.lock_responses.clear();
        self.clear_cmd_view_lock();

        self.handle_client_command(command_view.command).await
    }

    fn lock_command_view(&mut self, command_view: &CommandView) {
        if command_view.view != 0 {
            self.command_view_lock = command_view.clone();
        }
    }

    fn clear_cmd_view_lock(&mut self) {
        self.command_view_lock = CommandView::new();
    }
}