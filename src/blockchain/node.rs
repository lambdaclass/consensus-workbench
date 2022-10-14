/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use bytes::Bytes;
use lib::network::ReliableSender;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use lib::command::Command as ClientCommand;

/// The types of messages supported by this implementation's state machine.
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// A client transaction either received directly from the client or forwarded by a peer.
    Command(String, ClientCommand),

    /// A request from a node to its seed to get it's current ledger.
    GetState { reply_to: SocketAddr },

    State {
        from: SocketAddr,
        peers: HashSet<SocketAddr>,
        ledger: Ledger,
    },
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
            // for now generating the uuid here, should we let the client do it?
            let txid = uuid::Uuid::new_v4().to_string();
            Ok(Self::Command(txid, c))
        } else {
            bincode::deserialize(&data).map_err(|e| anyhow!(e))
        }
    }
}

/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub address: SocketAddr,
    pub peers: HashSet<SocketAddr>,
    pub sender: ReliableSender,
    pub mempool: HashMap<String, ClientCommand>,
    pub ledger: Ledger,
    pub miner_task: JoinHandle<()>,
    pub miner_receiver: Receiver<Block>,
    pub miner_sender: Sender<Block>,
    pub network_receiver: Receiver<(Message, oneshot::Sender<Result<Option<String>>>)>,
}

use ClientCommand::*;
use Message::*;

use crate::ledger::{Block, Ledger};

impl Node {
    pub async fn spawn(
        address: SocketAddr,
        seed: Option<SocketAddr>,
        network_receiver: Receiver<(Message, oneshot::Sender<Result<Option<String>>>)>,
    ) -> JoinHandle<()> {
        let mut peers = HashSet::new();
        if let Some(seed) = seed {
            peers.insert(seed);
        }

        let ledger = Ledger::new();
        let (miner_sender, miner_receiver) = channel(2);
        let miner_task = ledger.spawn_miner(vec![], miner_sender.clone()).await;

        let mut node = Self {
            address,
            peers,
            sender: ReliableSender::new(),
            mempool: HashMap::new(),
            ledger,
            miner_task,
            miner_sender,
            miner_receiver,
            network_receiver,
        };

        tokio::spawn(async move {
            // ask the seeds for their current state to catch up with the ledger and learn about peers
            let startup_message = GetState {
                reply_to: node.address,
            };
            node.broadcast(startup_message).await;

            loop {
                // TODO move pattern up
                tokio::select! {
                    block = node.miner_receiver.recv() => {
                        info!("Received block: {:?}", block);
                        if let Some(block) = block {
                            let new_ledger = node.ledger.extend(block).unwrap();
                            node.update_ledger(new_ledger).await;
                        }
                    }
                    message = node.network_receiver.recv() => {
                        if let Some((message, reply_sender)) = message {
                            let result = node.handle_message(message).await;
                            // FIXME don't unwrap
                            reply_sender.send(result).unwrap();
                        }
                    }
                }
            }
        })
    }
}

impl Node {
    async fn handle_message(&mut self, message: Message) -> Result<Option<String>> {
        info!("Received request {:?}", message);

        match message {
            Command(_, Get { key }) => Ok(self.ledger.get(&key)),
            Command(txid, Set { value, key }) => {
                if self.mempool.contains_key(&txid) || self.ledger.contains(&txid) {
                    Ok(None)
                } else {
                    let cmd = Set {
                        key: key.clone(),
                        value: value.clone(),
                    };
                    self.mempool.insert(txid.clone(), cmd.clone());

                    let message = Command(txid, cmd);
                    self.broadcast(message).await;

                    // just for consistency return the value, although it's not committed
                    Ok(Some(value))
                }
            }
            GetState { reply_to } => {
                // save the peer if later user
                self.peers.insert(reply_to);

                let response = State {
                    from: self.address,
                    ledger: self.ledger.clone(),
                    peers: self.peers.clone(),
                };

                // FIXME log error and continue on error instead of unwrapping
                let response = bincode::serialize(&response).unwrap().into();
                self.sender.send(reply_to, response).await;
                Ok(None)
            }
            State {
                from,
                ledger,
                peers,
            } => {
                // learn about new peers
                self.peers.insert(from);
                self.peers.extend(&peers);

                // if the received chain is longer, prefer it and broadcast it
                // otherwise ignore
                if ledger.is_valid() && ledger.length() > self.ledger.length() {
                    self.update_ledger(ledger).await;
                }
                Ok(None)
            }
        }
    }

    /// TODO
    // FIXME this is a weird function, only added it to avoid duplication but there should be a better separation
    async fn update_ledger(&mut self, ledger: Ledger) {
        self.ledger = ledger;

        // since the ledger changed, the current miner task extending the old one is invalid
        // so we abort it and restart mining based on the latest ledger
        self.miner_task.abort();
        let mempool = self.mempool.clone().into_iter().collect();
        self.miner_task = self
            .ledger
            .spawn_miner(mempool, self.miner_sender.clone())
            .await;

        let message = State {
            from: self.address,
            ledger: self.ledger.clone(),
            peers: self.peers.clone(),
        };
        self.broadcast(message).await;
    }

    /// TODO
    async fn broadcast(&mut self, message: Message) {
        let message: Bytes = bincode::serialize(&message).unwrap().into();
        let peers_vec = self.peers.clone().into_iter().collect();

        // forward the command to all replicas and wait for them to respond
        info!("Forwarding set to {:?}", peers_vec);
        let handlers = self.sender.broadcast(peers_vec, message).await;
        futures::future::join_all(handlers).await;
    }
}
