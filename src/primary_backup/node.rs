/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use bytes::Bytes;
use core::fmt;
use lib::command::{ClientCommand, CommandResult};
use lib::{network::SimpleSender, store::Store};
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The types of messages supported by this implementation's state machine.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    /// A command sent by a a client to this node.
    Command(ClientCommand),

    /// A request from a peer to replicate a client write command
    Replicate(ClientCommand, SocketAddr),

    /// A backup replica request to subcribe to a primary
    Subscribe { address: SocketAddr },

    /// A primary node's heartbeat including the currently known peers this is just for illustration purposes not being used yet
    Heartbeat,

    /// A request for the actual primary, used when a new replica wants to join but doesn't know who is the current primary
    PrimaryAddress,

    /// A Message emitted from primary to all replicas informing that a new node was added with the current peers and the view
    NewReplica(Vec<SocketAddr>, usize),
}

const HEARTBEAT_CYCLE: usize = 2;
const PRIMARY_TIMEOUT: usize = 10;
const CIYLE_LENGTH: u64 = 100;

/// Safe serialization helper. Logs on error.
fn serialize<T: Serialize + fmt::Debug>(message: &T) -> Option<Bytes> {
    match bincode::serialize(message) {
        Ok(data) => Some(data.into()),
        Err(err) => {
            error!("failed to serialize message {:?}, error: {}", message, err);
            None
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub state: State,
    pub store: Store,

    /// peers is a vector with the addresses of the nodes that integrate the system ordered by the time that they integrate it.
    pub peers: Vec<SocketAddr>,

    /// cycle represents:
    ///     Primary: Number of seconds since the last time the node send a heartbeat to the replicas.
    ///     Backup: Number of seconds since the last received heartbeat.
    pub cycle: usize,

    /// view number refers to the current primary on peers
    pub view: usize,

    pub sender: SimpleSender,
    address: SocketAddr,

    /// address of the primary node, used by backups nodes to subscribe at start up
    primary_address: SocketAddr,
}

/// The state of a node viewed as a state-machine.
#[derive(Clone, Copy, PartialEq)]
pub enum State {
    Primary,
    Backup,
}

use ClientCommand::*;
use Message::*;
use State::*;

impl Node {
    pub fn primary(db_path: &str, address: SocketAddr, primary_address: SocketAddr) -> Self {
        Self {
            address,
            state: Primary,
            store: Store::new(db_path).unwrap(),
            view: 0,
            cycle: 0,
            peers: Vec::new(),
            sender: SimpleSender::new(),
            primary_address,
        }
    }

    pub fn backup(db_path: &str, address: SocketAddr, primary_address: SocketAddr) -> Self {
        Self {
            address,
            state: Backup,
            store: Store::new(db_path).unwrap(),
            peers: Vec::new(),
            cycle: 0,
            view: 0,
            sender: SimpleSender::new(),
            primary_address,
        }
    }
}

impl Node {
    async fn broadcast_to_others(&mut self, command: Message) {
        let message: Bytes = bincode::serialize(&command).unwrap().into();
        let peers = &self.peers[self.view + 1..];
        let other_peers: Vec<SocketAddr> = peers
            .iter()
            .copied()
            .filter(|x| *x != self.address)
            .collect();

        // forward the command to all replicas and wait for them to respond
        self.sender.broadcast(other_peers, message).await;
    }

    async fn send_primary(&mut self, command: Message) {
        let message: Bytes = bincode::serialize(&command).unwrap().into();
        self.sender.send(self.get_primary(), message).await;
    }
    /// Runs the node to process network messages incoming in the given receiver
    pub async fn run(
        &mut self,
        mut network_receiver: Receiver<(Message, oneshot::Sender<String>)>,
        mut client_receiver: Receiver<(ClientCommand, oneshot::Sender<CommandResult>)>,
    ) -> JoinHandle<()> {
        if self.state == Backup {
            let msg = Message::Subscribe {
                address: self.address,
            };
            let message: Bytes = bincode::serialize(&msg).unwrap().into();
            self.sender.send(self.primary_address, message).await;
        } else {
            self.peers = vec![self.primary_address];
        }

        loop {
            tokio::select! {
                Some((command, reply_sender)) = client_receiver.recv() => {
                    info!("Received client message {}", command);

                    let message = Command(command);

                    let result = self.handle_msg(message.clone()).await.map_err(|e|e.to_string());

                    if let Err(error) = reply_sender.send(result) {
                        error!("failed to send message {:?} response {:?}", message, error);
                    };
                }
                Some((message, reply_sender)) = network_receiver.recv() => {
                    info!("[{}] Received network message {}",self.address, message);

                    // PrimaryAddress is a command that get who is at that momment the primary node, so should return a value and not an ack.
                    if let msg @ PrimaryAddress = &message {
                        if let Ok(Some(result)) = self.handle_msg(msg.clone()).await.map_err(|e|e.to_string()){
                            if let Err(error) = reply_sender.send(result) {
                                error!("failed to send message {:?} response {:?}", msg, error);
                            };
                        } else {
                            error!("failed to handle message {:?}", msg);
                        };
                    } else {
                        reply_sender.send("ACK".to_string()).unwrap();
                        self.handle_msg(message.clone()).await.unwrap();
                    }
                }
                _ = self.check_timer() => ()
            }
        }
    }

    // Checks sync betwen primary and replicas.
    async fn check_timer(&mut self) {
        match self.state {
            // Primary waits HEARTBEAT_CYCLE * CYCLE_LENGTH miliseconds to send a new heartbeat to replicas
            State::Primary => {
                if self.cycle >= HEARTBEAT_CYCLE {
                    self.broadcast_to_others(Heartbeat).await;
                    self.cycle = 0;
                } else {
                    self.cycle += 1;
                }
            }
            // Backup waits at least PRIMARY TIMEOUT * cycle milliseconds to change view
            // If Backup is next in line (peers[view + 1]) and the view change then it becomes the new primary
            State::Backup => {
                if self.cycle >= PRIMARY_TIMEOUT {
                    if self.peers[self.view + 1] == self.address {
                        self.state = State::Primary;
                    }
                    self.view += 1;
                    self.cycle = 0
                } else {
                    self.cycle += 1;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(CIYLE_LENGTH)).await;
    }
    /// Process each messages coming from clients and foward events to the replicas
    pub async fn handle_msg(&mut self, message: Message) -> Result<Option<String>> {
        match (self.state, message) {
            (Primary, Command(Set { key, value })) => {
                self.broadcast_to_others(Replicate(
                    Set {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    self.address,
                ))
                .await;
                self.cycle = 0;
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await?;

                Ok(Some(value))
            }
            (Backup, Replicate(Set { key, value }, reply_to)) => {
                self.cycle = 0;
                self.store.write(key.into(), value.clone().into()).await?;

                if let Some(data) = serialize(&value) {
                    self.sender.send(reply_to, data).await;
                }
                Ok(None)
            }
            (Backup, message @ Command(Set { key: _, value: _ })) => {
                self.send_primary(message).await;
                Ok(None)
            }
            (Backup, Heartbeat) => {
                self.cycle = 0;
                Ok(None)
            }
            (Primary, Subscribe { address }) => {
                self.peers.push(address);
                info!("Peers: {:?}", self.peers);

                self.broadcast_to_others(Message::NewReplica(self.peers.clone(), self.view))
                    .await;

                Ok(None)
            }
            (Backup, NewReplica(peers, view)) => {
                self.view = view;
                self.peers = peers;
                Ok(None)
            }
            (_, Command(Get { key })) => {
                if let Ok(Some(val)) = self.store.read(key.clone().into()).await {
                    let value = String::from_utf8(val)?;
                    return Ok(Some(value));
                }

                Ok(None)
            }
            (_, PrimaryAddress) => Ok(Some(self.get_primary().to_string())),
            _ => Err(anyhow!("Unhandled command")),
        }
    }

    fn get_primary(&self) -> SocketAddr {
        self.peers[self.view]
    }
}
