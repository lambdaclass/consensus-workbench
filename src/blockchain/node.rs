/// TODO
use anyhow::{anyhow, Result};
use bytes::Bytes;
use core::fmt;
use lib::network::SimpleSender;
use log::{error, info};
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

    /// TODO
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

/// FIXME
pub struct Node {
    // FIXME add docs to each field
    pub address: SocketAddr,
    pub peers: HashSet<SocketAddr>,
    pub sender: SimpleSender,
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
    /// TODO
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

        let mut node = Self {
            address,
            peers,
            sender: SimpleSender::new(),
            mempool: HashMap::new(),
            ledger,
            miner_task: tokio::spawn(async {}), // temporary noop
            miner_sender,
            miner_receiver,
            network_receiver,
        };

        node.restart_miner();

        tokio::spawn(async move {
            // ask the seeds for their current state to catch up with the ledger and learn about peers
            let startup_message = GetState {
                reply_to: node.address,
            };
            node.broadcast(startup_message).await;

            loop {
                tokio::select! {
                    Some(block) = node.miner_receiver.recv() => {
                        info!("Received block: {:?}", block);
                        // even if we explicitly reset the miner when the ledger is updated, it could happen that
                        // a message is waiting in the channel from a now obsolete block
                        if let Ok(new_ledger) = node.ledger.extend(block) {
                            node.update_ledger(new_ledger).await;
                        };
                    }
                    Some((message, reply_sender)) = node.network_receiver.recv() => {
                        let result = node.handle_message(message).await;
                        // FIXME don't unwrap
                        reply_sender.send(result).unwrap();
                    }
                    else => {
                        error!("node channels are closed");
                    }
                }
            }
        })
    }

    /// TODO
    async fn handle_message(&mut self, message: Message) -> Result<Option<String>> {
        info!("Received network message {}", message);

        match message {
            Command(_, Get { key }) => Ok(self.ledger.get(&key)),
            Command(txid, Set { value, key }) => {
                if self.mempool.contains_key(&txid) || self.ledger.contains(&txid) {
                    // TODO consider this case in tests
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
                self.peers.remove(&self.address);

                // if the received chain is longer, prefer it and broadcast it
                // otherwise ignore
                if ledger.is_valid() && ledger.blocks.len() > self.ledger.blocks.len() {
                    info!(
                        "Received a longer ledger from {}, replacing the local one",
                        from
                    );
                    self.update_ledger(ledger).await;
                }
                Ok(None)
            }
        }
    }

    /// Replaces the node's local ledger with the given one and applies side-effects of this update:
    ///   - clear committed transactions from the mempool
    ///   - restarts the miner to consider the new latest block and list of transactions
    ///   - broadcast the new ledger state to propagate the changes (and increasing the chances of
    ///     this version of the chain to become accepted by the network).
    async fn update_ledger(&mut self, ledger: Ledger) {
        self.ledger = ledger;

        // TODO this should be tested
        // remove committed transactions from the mempool
        self.mempool.retain(|k, _| !self.ledger.contains(k));

        // since the ledger and the mempool changed, the current miner task extending the old one is invalid
        // so we abort it and restart mining based on the latest ledger
        self.restart_miner();

        let message = State {
            from: self.address,
            ledger: self.ledger.clone(),
            peers: self.peers.clone(),
        };
        self.broadcast(message).await;
    }

    /// TODO
    fn restart_miner(&mut self) {
        let previous_block = self.ledger.blocks.last().unwrap().clone();
        let transactions = self.mempool.clone().into_iter().collect();
        let sender = self.miner_sender.clone();
        self.miner_task.abort();
        self.miner_task = tokio::spawn(async move {
            let new_block = Ledger::mine_block(previous_block, transactions);
            sender.send(new_block).await.unwrap();
        });
    }

    /// TODO
    async fn broadcast(&mut self, message: Message) {
        let message: Bytes = bincode::serialize(&message).unwrap().into();
        let peers_vec = self.peers.clone().into_iter().collect();

        // forward the command to all replicas and wait for them to respond
        info!("Broadcasting to {:?}", peers_vec);
        self.sender.broadcast(peers_vec, message).await;
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State {
                from,
                peers,
                ledger,
            } => write!(
                f,
                "State {{ from: {:?}, peers: {:?}, ledger: {} }}",
                from, peers, ledger
            ),
            other => write!(f, "{:?}", other),
        }
    }
}
