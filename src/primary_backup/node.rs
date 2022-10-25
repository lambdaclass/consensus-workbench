/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::command::ClientCommand;
use lib::{
    network::{MessageHandler, ReliableSender, Writer},
    store::Store,
};
use log::info;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

/// The types of messages supported by this implementation's state machine.
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// A command sent by a a client to this node.
    Command(ClientCommand),

    /// A request from a peer to replicate a client write command
    Replicate(ClientCommand),

    /// A backup replica request to subcribe to a primary
    Subscribe { address: SocketAddr },

    /// A primary node's heartbeat including the currently known peers
    // this is just for illustration purposes not being used yet
    Heartbeat { peers: Vec<SocketAddr> },
}

impl Message {
    /// Incoming requests can be either messages sent by other nodes or client commands
    /// (which are server implementation agnostic)
    /// in which case they are wrapped into Message::Command to treat them uniformly by
    /// the state machine
    pub fn deserialize(data: Bytes) -> Result<Self> {
        // this allows handling both client and peer messages from the same tcp listener
        // alternatively the network code could be refactored to have different tcp connections
        // and feeding both types of incoming messages into the same async handler task
        if let Ok(c) = bincode::deserialize::<ClientCommand>(&data) {
            Ok(Self::Command(c))
        } else {
            bincode::deserialize(&data).map_err(|e| anyhow!(e))
        }
    }
}

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub state: State,
    pub store: Store,
    pub peers: Vec<SocketAddr>,
    pub sender: ReliableSender,
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
    pub fn primary(db_path: &str) -> Self {
        Self {
            state: Primary,
            store: Store::new(db_path).unwrap(),
            peers: vec![],
            sender: ReliableSender::new(),
        }
    }

    pub fn backup(db_path: &str) -> Self {
        Self {
            state: Backup,
            store: Store::new(db_path).unwrap(),
            peers: vec![],
            sender: ReliableSender::new(),
        }
    }
}

#[derive(Clone)]
pub struct NodeReceiverHandler {
    /// Used to forward incoming TCP messages to the node
    pub network_sender: Sender<(Message, oneshot::Sender<Result<Option<Vec<u8>>>>)>,
}

#[async_trait]
impl MessageHandler for NodeReceiverHandler {
    /// When a TCP message is received, interpret it as a node::Message and forward it to the node task.
    /// Send the node's response back through the TCP connection.
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = Message::deserialize(bytes)?;
        log::info!("Received request {:?}", request);

        let (reply_sender, reply_receiver) = oneshot::channel();
        self.network_sender.send((request, reply_sender)).await?;
        let reply = reply_receiver.await?.map_err(|e| e.to_string());

        let reply = bincode::serialize(&reply)?;
        log::info!("Sending response {:?}", reply);

        Ok(writer.send(reply.into()).await?)
    }
}

impl Node {
    async fn forward_to_replicas(&mut self, command: ClientCommand) {
        let sync_message: Bytes = bincode::serialize(&command).unwrap().into();

        // forward the command to all replicas and wait for them to respond
        info!("Forwarding set to {:?}", self.peers);
        let handlers = self.sender.broadcast(&self.peers, sync_message).await;
        futures::future::join_all(handlers).await;
    }

    pub async fn run(
        &mut self,
        mut network_receiver: Receiver<(Message, oneshot::Sender<Result<Option<Vec<u8>>>>)>,
    ) -> () {
        while let Some((message, reply_sender)) = network_receiver.recv().await {
            self.handle_msg(message, reply_sender).await;
        }
    }

    pub async fn handle_msg(
        &mut self,
        message: Message,
        reply_sender: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) -> () {
        let result = match (self.state, message) {
            (Primary, Command(Set { key, value })) => {
                self.forward_to_replicas(Set {
                    key: key.clone(),
                    value: value.clone(),
                })
                .await;
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
            }
            (Backup, Replicate(Set { key, value })) => {
                self.forward_to_replicas(Set {
                    key: key.clone(),
                    value: value.clone(),
                })
                .await;
                self.store.write(key.into(), value.into()).await
            }
            (_, Subscribe { address }) => {
                self.peers.push(address);
                info!("Peers: {:?}", self.peers);
                Ok(None)
            }
            (_, Command(Get { key })) => self.store.read(key.clone().into()).await,
            _ => Err(anyhow!("Unhandled command")),
        };
        let _ = reply_sender.send(result);
    }
}
