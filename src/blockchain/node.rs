/// This module contains the definition of a node in a blockchain p2p network, where each node maintains
/// a ledger of key/value store transactions, as well of the network messages supported between nodes.
use anyhow::Result;
use bytes::Bytes;
use core::fmt;
use lib::network::SimpleSender;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use lib::command::{ClientCommand, CommandResult};

/// The types of messages supported by this implementation's state machine.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Message {
    /// A client transaction either received directly from the client or forwarded by a peer.
    Command(TransactionId, ClientCommand),

    /// A request from a node to its seed to get it's current ledger.
    GetState { reply_to: SocketAddr },

    /// This is the only message to share state across nodes. It includes the list of the sender's
    /// known peers (so it will be used to discover the network) and the sender's ledger (which will be
    /// used both for new nodes to sync and to broadcast newly mined blocks).
    /// Note that this is a naive implementation in that the entire ledger is passed around every time,
    /// but that could be improved while maintaining a single message type by passing ledger segments
    /// instead.
    State {
        from: SocketAddr,
        peers: HashSet<SocketAddr>,
        ledger: Ledger,
    },
}

/// A node in the blockchain network.
pub struct Node {
    /// The ip+port this node is currently listening on for peer messages.
    address: SocketAddr,

    /// The list of known peers, to which this node will broadcast message to.
    peers: HashSet<SocketAddr>,

    /// Network sender to communicate with peers, for example for broadcasting messages.
    sender: SimpleSender,

    /// The pool of pending transactions. The miner task will draw from this pool to include in blocks.
    mempool: HashMap<TransactionId, ClientCommand>,

    /// The blockchain of committed transactions.
    ledger: Ledger,

    /// A handler to the local task that's mining blocks. Necessary to reset the mining whenever another
    /// node's ledger is found to be preferred than the local one which the mining was based on.
    miner_task: JoinHandle<()>,

    /// The channel handler the node listens to for newly mined blocks.
    miner_receiver: Receiver<Block>,

    /// A copy of the sender end of the mining channel, held to pass to each new miner task.
    miner_sender: Sender<Block>,
}

use ClientCommand::*;
use Message::*;

use crate::ledger::{Block, Ledger, TransactionId};

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
        mut network_receiver: Receiver<(Message, oneshot::Sender<()>)>,
        mut client_receiver: Receiver<(ClientCommand, oneshot::Sender<CommandResult>)>,
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
                Some((command, reply_sender)) = client_receiver.recv() => {
                    info!("Received client message {}", command);

                    // for now generating the uuid here, should we let the client do it?
                    let txid = uuid::Uuid::new_v4().to_string();
                    let message = Command(txid, command);
                    let result = self.handle_message(message.clone()).await.map_err(|e|e.to_string());

                    // in the case of clients sending a tcp request, we want to write a response directly
                    // to their connection
                    if let Err(error) = reply_sender.send(result) {
                        error!("failed to send message {:?} response {:?}", message, error);
                    };
                }
                Some((message, _)) = network_receiver.recv() => {
                    info!("Received network message {}", message);

                    // FIXME network messages shouldn't fail
                    self.handle_message(message.clone()).await.unwrap();
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
        match message {
            // When a client read request is received, just read the local ledger and send a response
            Command(_, Get { key }) => Ok(self.ledger.get(&key)),

            // When a client write request is received, it needs to be added to the local mempool (so it's included
            // in future blocks mined in this node) and broadcast to the network (so all the nodes eventually know about
            // the transaction and any winning chain includes it).
            Command(txid, Set { value, key }) => {
                if self.mempool.contains_key(&txid) || self.ledger.contains(&txid) {
                    debug!("skipping already seen transaction {}", txid);
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

            // When receiving a peers state, check if it contains a valid, longer chain that should
            // be preferred to the local one and broadcast.
            State {
                from,
                ledger,
                peers,
            } => {
                // learn about new peers
                self.peers.insert(from);
                self.peers.extend(&peers);
                self.peers.remove(&self.address);

                // check if the peer's ledger should be preferred
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

    /// Abort the currently running miner task and start a new one based on the latest ledger and mempool.
    fn restart_miner(&mut self) {
        debug!("Restarting miner...");
        let previous_block = self.ledger.blocks.last().unwrap().clone();
        let transactions = self.mempool.clone().into_iter().collect();
        let sender = self.miner_sender.clone();
        let miner_id = self.address.to_string();
        self.miner_task.abort();
        self.miner_task = tokio::spawn(async move {
            let new_block = Ledger::mine_block(&miner_id, previous_block, transactions).await;
            if let Err(err) = sender.send(new_block).await {
                error!("error sending mined block {}", err);
            }
        });
    }

    /// Send the given message to all known peers. Doesn't wait for acknowledge.
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
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn transactions() {
        let address: SocketAddr = "127.0.0.1:6279".parse().unwrap();
        let mut node = Node::new(address, None);

        // send a new transaction to the ledger -> adds it to the mempool
        let tx1 = Command(
            "tx1".to_string(),
            ClientCommand::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            },
        );

        node.handle_message(tx1.clone()).await.unwrap();
        assert_eq!(1, node.mempool.len());
        assert!(node.mempool.contains_key("tx1"));

        // if already in mempool ignore
        node.handle_message(tx1.clone()).await.unwrap();
        assert_eq!(1, node.mempool.len());
        assert!(node.mempool.contains_key("tx1"));

        // same operation with different transaction id is considered different
        let tx2 = Command(
            "tx2".to_string(),
            ClientCommand::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            },
        );
        node.handle_message(tx2.clone()).await.unwrap();
        assert_eq!(2, node.mempool.len());
        assert!(node.mempool.contains_key("tx2"));

        // don't include a transaction in the mempool if it's already in the ledger
        node.restart_miner();
        let block = node.miner_receiver.recv().await.unwrap();
        let new_ledger = node.ledger.extend(block).unwrap();
        node.update_ledger(new_ledger).await;
        assert_eq!(0, node.mempool.len());
        assert!(node.ledger.contains("tx2"));
        node.handle_message(tx2.clone()).await.unwrap();
        assert_eq!(0, node.mempool.len());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ledger_update() {
        let address1: SocketAddr = "127.0.0.1:6279".parse().unwrap();
        let mut node1 = Node::new(address1, None);

        let address2: SocketAddr = "127.0.0.1:6280".parse().unwrap();
        let mut node2 = Node::new(address2, None);

        // if an invalid ledger is received ignore
        assert_eq!(1, node1.ledger.blocks.len());
        let mut invalid_ledger = node1.ledger.clone();
        invalid_ledger.blocks.push(Block::genesis());
        invalid_ledger.blocks.push(Block::genesis());
        assert_eq!(3, invalid_ledger.blocks.len());

        let invalid_message = Message::State {
            peers: HashSet::new(),
            from: address2,
            ledger: invalid_ledger,
        };
        node1.handle_message(invalid_message).await.unwrap();
        // learns the new peer
        assert!(node1.peers.contains(&address2));
        // ignores the ledger
        assert_eq!(1, node1.ledger.blocks.len());

        // mine a block in one of the nodes
        node1.restart_miner();
        let block = node1.miner_receiver.recv().await.unwrap();
        let new_ledger = node1.ledger.extend(block).unwrap();
        node1.update_ledger(new_ledger.clone()).await;
        assert_eq!(2, node1.ledger.blocks.len());

        // send to the other node. accepted because it's valid and longer
        let valid_message = Message::State {
            peers: HashSet::new(),
            from: address1,
            ledger: new_ledger,
        };
        node2.handle_message(valid_message).await.unwrap();
        assert!(node2.peers.contains(&address1));
        assert_eq!(2, node1.ledger.blocks.len());

        // mine a new block in both
        let block = node1.miner_receiver.recv().await.unwrap();
        let new_ledger = node1.ledger.extend(block).unwrap();
        node1.update_ledger(new_ledger.clone()).await;
        assert_eq!(3, node1.ledger.blocks.len());
        assert_eq!(
            address1.to_string(),
            node1.ledger.blocks.last().unwrap().miner_id
        );

        let block = node2.miner_receiver.recv().await.unwrap();
        let new_ledger2 = node2.ledger.extend(block).unwrap();
        node2.update_ledger(new_ledger2.clone()).await;
        assert_eq!(3, node2.ledger.blocks.len());
        assert_eq!(
            address2.to_string(),
            node2.ledger.blocks.last().unwrap().miner_id
        );

        // send one to the other. ignored because the chain isn't longer
        let valid_message = Message::State {
            peers: HashSet::new(),
            from: address2,
            ledger: new_ledger2,
        };
        node1.handle_message(valid_message).await.unwrap();
        assert_eq!(3, node1.ledger.blocks.len());
        assert_eq!(
            address1.to_string(),
            node1.ledger.blocks.last().unwrap().miner_id
        );
    }
}
