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

    /// A primary node's heartbeat including the currently known peers
    // this is just for illustration purposes not being used yet
    Heartbeat,

    /// A request for the actual primary
    PrimaryAddress,
}

const HEARTBEAT_CICLE: usize = 2;
const PRIMARY_TIMEOUT: usize = 10;
const CICLE_TIME: u64 = 100;

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
    pub cicle: usize,
    pub view: usize,
    pub peers: Vec<SocketAddr>,
    pub sender: SimpleSender,
    address: SocketAddr,
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
    pub fn primary(db_path: &str, address: SocketAddr, peers: Vec<SocketAddr>) -> Self {
        Self {
            address,
            state: Primary,
            store: Store::new(db_path).unwrap(),
            view: 0,
            cicle: 0,
            peers,
            sender: SimpleSender::new(),
        }
    }

    pub fn backup(db_path: &str, address: SocketAddr, peers: Vec<SocketAddr>) -> Self {
        Self {
            address,
            state: Backup,
            store: Store::new(db_path).unwrap(),
            peers,
            cicle: 0,
            view: 0,
            sender: SimpleSender::new(),
        }
    }
}

impl Node {
    async fn broadcast_to_others(&mut self, command: Message) {
        let message: Bytes = bincode::serialize(&command).unwrap().into();
        let peers = &self.peers[self.view+1..];
        let other_peers: Vec<SocketAddr> = peers
            .iter()
            .copied()
            .filter(|x| *x != self.address)
            .collect();

        // forward the command to all replicas and wait for them to respond
        self.sender.broadcast(other_peers, message).await;
    }

    /// Runs the node to process network messages incoming in the given receiver
    pub async fn run(
        &mut self,
        mut network_receiver: Receiver<(Message, oneshot::Sender<String>)>,
        mut client_receiver: Receiver<(ClientCommand, oneshot::Sender<CommandResult>)>,
    ) -> JoinHandle<()> {
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

    pub async fn check_timer(&mut self) {
        match self.state {
            State::Primary => {
                if self.cicle >= HEARTBEAT_CICLE {
                    self.broadcast_to_others(Heartbeat).await;
                    self.cicle = 0;
                } else {
                    self.cicle += 1;
                }
            }
            State::Backup => {
                if self.cicle >= PRIMARY_TIMEOUT {
                    if self.peers[self.view+1] == self.address{
                        self.state = State::Primary;
                    }
                    self.view += 1;
                    self.cicle = 0
                } else {
                    self.cicle += 1;
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(CICLE_TIME)).await;
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
                self.cicle = 0;
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await?;

                Ok(Some(value))
            }
            (Backup, Replicate(Set { key, value }, reply_to)) => {
                self.broadcast_to_others(Replicate(
                    Set {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    self.address,
                ))
                .await;

                self.cicle = 0;
                self.store.write(key.into(), value.clone().into()).await?;

                if let Some(data) = serialize(&value) {
                    self.sender.send(reply_to, data).await;
                }
                Ok(None)
            }
            (Backup, Heartbeat) => {
                self.cicle = 0;
                Ok(None)
            }
            (_, Subscribe { address }) => {
                self.peers.push(address);
                info!("Peers: {:?}", self.peers);
                Ok(None)
            }
            (_, Command(Get { key })) => {
                if let Ok(Some(val)) = self.store.read(key.clone().into()).await {
                    let value = String::from_utf8(val)?;
                    return Ok(Some(value));
                }

                Ok(None)
            }
            (_, PrimaryAddress) => Ok(Some(self.peers[self.view].to_string())),
            _ => Err(anyhow!("Unhandled command")),
        }
    }
}
