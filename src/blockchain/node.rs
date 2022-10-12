/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::network::{MessageHandler, ReliableSender, Writer};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, net::SocketAddr};

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

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub address: SocketAddr,
    // FIXME this arc mutex shouldn't be necessary
    pub peers: Arc<Mutex<HashSet<SocketAddr>>>,
    pub sender: ReliableSender,
    pub mempool: HashMap<String, ClientCommand>,
    pub ledger: Ledger,
}

use ClientCommand::*;
use Message::*;

use crate::ledger::Ledger;

impl Node {
    pub fn new(address: SocketAddr, seed: Option<SocketAddr>) -> Self {
        let mut peers = HashSet::new();
        if let Some(seed) = seed {
            peers.insert(seed);
        }

        Self {
            address,
            peers: Arc::new(Mutex::new(peers)),
            sender: ReliableSender::new(),
            mempool: HashMap::new(),
            ledger: Ledger::new(),
        }
    }
}

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = Message::deserialize(bytes)?;
        info!("Received request {:?}", request);

        // FIXME there are a couple of events not yet handled in the code below because they require extra channel setup
        // 1. On node startup -> broadcast GetLedger to current peers (which will either be [] or [seed])
        // 2. block mining done -> add new block to ledger and broadacast Ledger message

        let result: Result<Option<String>, Error> = match request {
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
                let response;
                {
                    // save the peer if later user
                    // FIXME remove locking
                    let mut locked_peers = self.peers.lock().unwrap();
                    locked_peers.insert(reply_to);

                    response = State {
                        from: self.address,
                        ledger: self.ledger.clone(),
                        peers: locked_peers.clone(),
                    };
                }
                let response = bincode::serialize(&response)?.into();
                self.sender.send(reply_to, response).await;
                Ok(None)
            }
            State {
                from,
                ledger,
                peers,
            } => {
                {
                    // learn about new peers
                    // FIXME remove locking
                    let mut locked_peers = self.peers.lock().unwrap();
                    locked_peers.insert(from);
                    locked_peers.extend(&peers);
                }

                // if the received chain is longer, prefer it and broadcast it
                // otherwise ignore
                if ledger.is_valid() && ledger.length() > self.ledger.length() {
                    self.ledger = ledger;

                    // FIXME restart mining with new latest block
                    let message = State {
                        from,
                        ledger: self.ledger.clone(),
                        peers,
                    };
                    self.broadcast(message).await;
                }
                Ok(None)
            }
        };

        // convert the error into something serializable
        let result = result.map_err(|e| e.to_string());

        info!("Sending response {:?}", result);
        let reply = bincode::serialize(&result)?;
        Ok(writer.send(reply.into()).await?)
    }
}

impl Node {
    async fn broadcast(&mut self, message: Message) {
        let message: Bytes = bincode::serialize(&message).unwrap().into();

        // FIXME this won't be necessary when we refactor
        // Need to lock the shared self.peers variable, but it needs to be done in
        // its own scope to release the lock before the .await
        // See https://tokio.rs/tokio/tutorial/shared-state in section
        // "Holding a MutexGuard across an .await" for more info
        let peers_vec;
        {
            let peers_lock = self.peers.lock().unwrap();
            peers_vec = peers_lock.clone().into_iter().collect();
        }

        // forward the command to all replicas and wait for them to respond
        info!("Forwarding set to {:?}", peers_vec);
        let handlers = self.sender.broadcast(peers_vec, message).await;
        futures::future::join_all(handlers).await;
    }
}
