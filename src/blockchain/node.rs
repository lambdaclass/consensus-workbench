/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, ReliableSender, Writer},
    store::Store,
};
use log::info;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use lib::command::Command as ClientCommand;

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
    pub peers: Arc<Mutex<Vec<SocketAddr>>>,
    pub sender: ReliableSender,
}

use ClientCommand::*;
use Message::*;

impl Node {
    pub fn new(seed: Option<SocketAddr>) -> Self {
        let peers = seed.map(|s| vec![s]).unwrap_or_default();
        Self {
            peers: Arc::new(Mutex::new(peers)),
            sender: ReliableSender::new(),
        }
    }
}

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = Message::deserialize(bytes)?;
        info!("Received request {:?}", request);

        let result = match request {
            Command(Set { key, value }) => {
                // TODO
                Ok(())
            }
            Replicate(Set { key, value }) => {
                // TODO
                Ok(())
            }
            Subscribe { address } => {
                // TODO
                Ok(())
            }
            Command(Get { key }) => Ok(()),
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
    async fn forward_to_replicas(&mut self, command: ClientCommand) {
        let sync_message: Bytes = bincode::serialize(&command).unwrap().into();

        // Need to lock the shared self.peers variable, but it needs to be done in
        // its own scope to release the lock before the .await
        // See https://tokio.rs/tokio/tutorial/shared-state in section
        // "Holding a MutexGuard across an .await" for more info
        let peers_clone;
        {
            let peers_lock = self.peers.lock().unwrap();
            peers_clone = peers_lock.clone();
        }

        // forward the command to all replicas and wait for them to respond
        info!("Forwarding set to {:?}", peers_clone.to_vec());
        let handlers = self
            .sender
            .broadcast(peers_clone.to_vec(), sync_message)
            .await;
        futures::future::join_all(handlers).await;
    }
}
