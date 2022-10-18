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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    /// A client transaction either received directly from the client or forwarded by a peer.
    Command(String, ClientCommand),

    /// A request from a node to its seed to get it's current ledger.
    GetState { reply_to: SocketAddr },

    /// TODO
    // FIXME should this include mempool?
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
    sender: SimpleSender,
    mempool: HashMap<String, ClientCommand>,
    ledger: Ledger,
    miner_task: JoinHandle<()>,
    miner_sender: Sender<Block>,
    miner_receiver: Receiver<Block>,
}

use ClientCommand::*;
use Message::*;

use crate::ledger::{Block, Ledger};

impl Node {
    /// Initialize the node attributes. It doesn't run it nor starts mining.
    pub fn new(address: SocketAddr, seed: Option<SocketAddr>) -> Self {
        let mut peers = HashSet::new();
        if let Some(seed) = seed {
            peers.insert(seed);
        }

        let (miner_sender, miner_receiver) = channel(2);

        Self {
            address,
            peers,
            sender: SimpleSender::new(),
            mempool: HashMap::new(),
            ledger: Ledger::new(),
            miner_task: tokio::spawn(async {}), // noop default
            miner_sender,
            miner_receiver,
        }
    }

    /// Runs the node to process network messages incoming in the given receiver and starts a mining
    /// task to produce blocks to extend the node's ledger.
    pub async fn run(
        &mut self,
        mut network_receiver: Receiver<(Message, oneshot::Sender<Result<Option<String>>>)>,
    ) -> JoinHandle<()> {
        self.restart_miner();

        // ask the seeds for their current state to catch up with the ledger and learn about peers
        let startup_message = GetState {
            reply_to: self.address,
        };
        self.broadcast(startup_message).await;

        loop {
            tokio::select! {
                Some(block) = self.miner_receiver.recv() => {
                    info!("Received block: {:?}", block);
                    // even if we explicitly reset the miner when the ledger is updated, it could happen that
                    // a message is x``waiting in the channel from a now obsolete block
                    if let Ok(new_ledger) = self.ledger.extend(block) {
                        self.update_ledger(new_ledger).await;
                    };
                }
                Some((message, reply_sender)) = network_receiver.recv() => {
                    let result = self.handle_message(message.clone()).await;
                    if let Err(error) = reply_sender.send(result) {
                        error!("failed to send message {:?} response {:?}", message, error);
                    };
                }
                else => {
                    error!("node channels are closed");
                }
            }
        }
    }

    /// This function is the core of the node's behavior. It process messages coming both from clients
    /// and peers, updates the local state and broadcasts updates.
    async fn handle_message(&mut self, message: Message) -> Result<Option<String>> {
        info!("Received network message {}", message);

        match message {
            // When a client read request is received, just read the local ledger and send a response
            Command(_, Get { key }) => Ok(self.ledger.get(&key)),

            // When a client write request is received, it needs to be added to the local mempool (so it's included
            // in future blocks mined in this node) and broadcast to the network (so all the nodes eventually know about
            // the transaction and any winning chain includes it).
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

            // When a peer requests for this node state, respond directly to it
            GetState { reply_to } => {
                // save the peer if later user
                self.peers.insert(reply_to);

                let response = State {
                    from: self.address,
                    ledger: self.ledger.clone(),
                    peers: self.peers.clone(),
                };

                if let Some(data) = serialize(&response) {
                    self.sender.send(reply_to, data).await;
                }
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
            if let Err(err) = sender.send(new_block).await {
                error!("error sending mined block {}", err);
            }
        });
    }

    /// TODO
    async fn broadcast(&mut self, message: Message) {
        if let Some(data) = serialize(&message) {
            let peers_vec = self.peers.clone().into_iter().collect();

            // forward the command to all replicas and wait for them to respond
            info!("Broadcasting to {:?}", peers_vec);
            self.sender.broadcast(peers_vec, data).await;
        }
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

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn set_command() {
        // if already in ledger ignore
        // if already in mempool ignore
        // else add to mempool
    }

    #[tokio::test]
    async fn state() {
        // if ledger is shorter ignore
        // if ledger is valid and longer replace current one
        // transactions are removed from mempool
        // if ledger is invalid ignore
    }
}
