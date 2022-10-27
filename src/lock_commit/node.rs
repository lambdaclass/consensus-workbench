use crate::command_ext::{Command, CommandView, NetworkCommand};
/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::{
    command::{self, ClientCommand},
    network::{MessageHandler, SimpleSender, Writer},
    store::Store,
};
use log::info;
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub socket_address: SocketAddr,
    pub store: Store,
    pub peers: Vec<SocketAddr>,
    pub sender: SimpleSender,

    // fixme: shared state is wrapped in Arc<RwLock<>>s because this is cloned for every received request
    // in the future, we want to implement a channel-based solution like in some of the other PoCs
    pub current_view: Arc<RwLock<u128>>,
    pub timer_start: Arc<RwLock<Instant>>,
    pub command_view_lock: Arc<RwLock<CommandView>>,

    // the amount of peers which responded with "Lock"
    // note: if we were to create a QC to store in the blockchain,
    // we would need to store signatures from peers here
    pub lock_responses: Arc<Mutex<HashSet<SocketAddr>>>,
    pub blame_messages: Arc<Mutex<HashSet<SocketAddr>>>,
}

/// The state of a node viewed as a state-machine.
#[derive(Clone, Copy)]
pub enum State {
    Primary,
    Backup,
}

use State::*;

#[derive(Clone)]
pub struct NodeReceiverHandler {
    /// Used to forward incoming TCP messages to the node
    pub network_sender: Sender<(Command, oneshot::Sender<Result<Option<Vec<u8>>>>)>,
}

#[async_trait]
impl MessageHandler for NodeReceiverHandler {
    /// When a TCP message is received, interpret it as a node::Message and forward it to the node task.
    /// Send the node's response back through the TCP connection.
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request: Command = bincode::deserialize(&bytes)?;
        log::info!("Received request {:?}", request);

        let (reply_sender, reply_receiver) = oneshot::channel();
        self.network_sender.send((request, reply_sender)).await?;
        let reply = reply_receiver.await?.map_err(|e| e.to_string());

        let reply = bincode::serialize(&reply)?;
        log::info!("Sending response {:?}", reply);

        let _ = writer.send(reply.into()).await?;

        Ok(())
    }
}

impl Node {
    pub fn new(
        peers: Vec<SocketAddr>,
        db_path: &str,
        address: SocketAddr,
        timer_start: Arc<RwLock<Instant>>,
    ) -> Self {
        Self {
            store: Store::new(db_path).unwrap(),
            peers: peers,
            sender: SimpleSender::new(),
            current_view: Arc::new(RwLock::new(0)),
            command_view_lock: Arc::new(RwLock::new(CommandView::new())),
            lock_responses: Arc::new(Mutex::new(HashSet::new())),
            blame_messages: Arc::new(Mutex::new(HashSet::new())),
            socket_address: address,
            timer_start,
        }
    }

    /// Runs the node to process network messages incoming in the given receiver
    pub async fn run(
        &mut self,
        mut network_receiver: Receiver<(Command, oneshot::Sender<Result<Option<Vec<u8>>>>)>,
    ) -> () {
        while let Some((message, reply_sender)) = network_receiver.recv().await {
            self.handle_msg(message, reply_sender).await;
        }
    }

    /// Process each messages coming from clients and foward events to the replicas
    pub async fn handle_msg(
        &mut self,
        message: Command,
        reply_sender: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) -> () {
        let state = self.get_state();

        let result = match (state, message) {
            // as a 'hack': to make it simpler, we can just forward the Get command to handle_client_message
            // if you comment this match code block, Get requests will also require quorum but it will still work
            (_, Command::Client(cmd @ ClientCommand::Get { key: _ })) => {
                self.handle_client_command(cmd).await
            }

            // if we receive a client command that is not a get and we are primary, prepare to propose
            (Primary, Command::Client(client_comand)) => {
                // we advance the view according to the primary and propose it
                let command_view = CommandView {
                    command: client_comand,
                    view: *self.current_view.read().unwrap() + 1,
                };

                // since we are primary, we lock the command view

                self.lock_command_view(&command_view);
                let _ = self
                    .handle_lock_message(self.socket_address, command_view.clone())
                    .await;

                let command = NetworkCommand::Propose {
                    command_view: command_view.clone(),
                };

                *self.timer_start.write().unwrap() = Instant::now(); // for blame/view-change

                info!("Received command, broadcasting Propose");
                self.broadcast_to_others(command).await;

                Ok(None)
            }

            // Once we proposed and we receive lock requests as a Primary, we can start counting the responses
            (
                Primary,
                Command::Network(NetworkCommand::Lock {
                    socket_addr,
                    command_view,
                }),
            ) => self.handle_lock_message(socket_addr, command_view).await,

            // a command has been proposed and we can lock it before sending a Lock message
            // for now this happens in the Primary as well, but that functionality could be piggy-backed in the section where we receive the command
            (_, Command::Network(NetworkCommand::Propose { command_view })) => {
                // TODO: You should only lock if view number is expected?
                self.lock_command_view(&command_view);
                info!(
                    "{}: View-command locked, sending out Lock message",
                    self.socket_address
                );

                let lock_command = Command::Network(NetworkCommand::Lock {
                    socket_addr: self.socket_address,
                    command_view: command_view,
                });

                self.send_to_primary(lock_command).await;
                Ok(None)
            }

            (Backup, Command::Client(client_command)) => {
                info!("Received client command, forwarding to primary");

                self.send_to_primary(Command::Client(client_command)).await;
                Ok(None)
            }

            // for the primary, the command is committed as we reach quorum, so we can return Ok
            (Primary, Command::Network(NetworkCommand::Commit { .. })) => Ok(None),

            // the backup gets a Commit message after we reach quorum, so we can go ahead and commit
            (Backup, Command::Network(NetworkCommand::Commit { command_view })) => {
                info!("about to try commit as a response to Commit message");

                let result = match self.try_commit(command_view).await {
                    Ok(result) => {
                        info!(
                            "{}: Committed command, response was {:?}",
                            self.socket_address,
                            result.unwrap()
                        );
                        Ok(None)
                    }
                    _ => Err(anyhow!("Error committing command")),
                };
                result
            }
            // View change moves the view/primary after enough blames were emitted
            (
                _,
                Command::Network(NetworkCommand::ViewChange {
                    socket_addr: _,
                    new_view,
                    highest_lock: _,
                }),
            ) => {
                if new_view > *self.current_view.read().unwrap() {
                    info!(
                        "{}: View-change performed, primary is {}",
                        self.socket_address,
                        self.get_primary(new_view)
                    );
                    *self.timer_start.write().unwrap() = Instant::now();
                    self.trigger_view_change(new_view);
                }
                Ok(None)
            }
            // blames are emitted after timer expires or if 'f' nodes sent us a blame command
            // this differentiation is so that we can make the protocol partially synchronous instead of synchronous
            (
                _,
                Command::Network(NetworkCommand::Blame {
                    socket_addr,
                    view,
                    timer_expired,
                }),
            ) => self.handle_blame(view, socket_addr, timer_expired).await,
            _ => {
                info!("{} :unhandled command", self.socket_address);
                Err(anyhow!("Unhandled command"))
            }
        };

        let _ = reply_sender.send(result);
    }

    async fn handle_lock_message(
        &mut self,
        socket_addr: SocketAddr,
        command_view: CommandView,
    ) -> Result<Option<Vec<u8>>, anyhow::Error> {
        if command_view.view <= *self.current_view.read().unwrap() {
            info!("Received command with an old, previously committed view, discarding");
            Ok(None)
        } else {
            let _ = self.lock_responses.lock().unwrap().insert(socket_addr);
            let response_count = self.lock_responses.lock().unwrap().len();

            // the literature defines quorum as a function of the adversarial threshold we want to support
            // n > f, n > 2f, or n > 3f are the alternatives; this uses n > 2f model
            let quorum_count = ((self.peers.len()) / 2) + 1;

            info!(
                "Received lock, did we get quorum? {} responses so far vs expected quorum of {} ",
                response_count, quorum_count
            );
            *self.timer_start.write().unwrap() = Instant::now();
            // broadcast commit, then try commit
            if response_count >= quorum_count {
                info!("Quorum achieved, commiting first and sending out Commit message!");
                self.try_commit(command_view.clone())
                    .await
                    .expect("Error committing command as primary");

                self.broadcast_to_others(NetworkCommand::Commit {
                    command_view: command_view,
                })
                .await;
            }
            Ok(None)
        }
    }

    async fn handle_client_command(
        &self,
        command: ClientCommand,
    ) -> Result<Option<Vec<u8>>, anyhow::Error> {
        match command {
            ClientCommand::Set { key, value } => self.store.write(key.into(), value.into()).await,
            ClientCommand::Get { key } => self.store.read(key.clone().into()).await,
        }
    }

    async fn broadcast(&mut self, network_command: NetworkCommand) {
        let message: Bytes = bincode::serialize(&Command::Network(network_command))
            .unwrap()
            .into();

        // forward the command to all replicas and wait for them to respond
        self.sender.broadcast(self.peers.clone(), message).await;
    }

    async fn broadcast_to_others(&mut self, network_command: NetworkCommand) {
        let message: Bytes = bincode::serialize(&Command::Network(network_command))
            .unwrap()
            .into();

        let other_peers: Vec<SocketAddr> = self
            .peers
            .iter()
            .copied()
            .filter(|x| *x != self.socket_address)
            .collect();

        // forward the command to all replicas and wait for them to respond
        self.sender.broadcast(other_peers, message).await;
    }

    async fn send_to_primary(&mut self, cmd: Command) {
        let message: Bytes = bincode::serialize(&cmd).unwrap().into();
        let primary_address = *(self.get_primary(*self.current_view.read().unwrap()));

        // forward the command to all replicas and wait for them to respond
        let _ = self.sender.send(primary_address, message).await;
    }

    async fn try_commit(&mut self, command_view: CommandView) -> Result<Option<Vec<u8>>> {
        *self.timer_start.write().unwrap() = Instant::now();

        // handle command, remove command lock (Primray already commits when quorum is achieved)
        if *self.command_view_lock.read().unwrap() != command_view {
            info!(
                "{}: trying to commit {:?} but we had locked {:?}",
                self.socket_address,
                command_view,
                *self.command_view_lock.read().unwrap()
            );
            // we are trying to commit something that has not been locked correctly,
            // so there must have been some fault
            return Err(anyhow!(
                "Trying to commit a command-view that had not been previously locked"
            ));
        }

        {
            let mut lock = self.current_view.write().unwrap(); // update last valid view number
            *lock = command_view.view;
        }

        self.command_view_lock.write().unwrap().view += 1;
        // handle command
        self.lock_responses.lock().unwrap().clear();
        self.clear_cmd_view_lock();

        self.handle_client_command(command_view.command).await
    }

    fn lock_command_view(&mut self, command_view: &CommandView) {
        if command_view.view != 0 {
            let mut lock = self.command_view_lock.write().unwrap();
            *lock = command_view.clone();
        }
        info!(
            "{}: Locked command view {:?}",
            self.socket_address,
            *self.command_view_lock.read().unwrap()
        );
    }

    pub fn get_state(&self) -> State {
        if self.get_primary(*self.current_view.read().unwrap()) == &self.socket_address {
            return State::Primary;
        }
        State::Backup
    }

    fn trigger_view_change(&mut self, new_view: u128) {
        let mut lock = self.current_view.write().unwrap(); // update last valid view number
        *lock = new_view;

        self.lock_responses.lock().unwrap().clear();
        self.blame_messages.lock().unwrap().clear();
        *self.command_view_lock.write().unwrap() = CommandView::new();
    }

    fn get_primary(&self, view: u128) -> &SocketAddr {
        self.peers.get(view as usize % self.peers.len()).unwrap()
    }

    fn clear_cmd_view_lock(&mut self) {
        *self.command_view_lock.write().unwrap() = CommandView::new();
    }

    async fn handle_blame(
        &mut self,
        view: u128,
        socket_addr: SocketAddr,
        timer_expired: bool,
    ) -> Result<Option<Vec<u8>>> {
        if view != *self.current_view.read().unwrap() && !timer_expired {
            return Ok(None);
        }

        let _ = self.blame_messages.lock().unwrap().insert(socket_addr);
        let blame_count = self.blame_messages.lock().unwrap().len();

        let highest_view_lock = (self.command_view_lock.read().unwrap()).clone();

        // from the docs, f is the amount of omission failures we want to tolerate
        // again, f is defined from the adversarial threshold of the system
        let f = self.peers.len() / 2;
        let quorum_count = f + 1;

        info!(
            "Received blame, did we get quorum? {} responses so far vs expected quorum of {} ",
            blame_count, quorum_count
        );
        let current_view = *self.current_view.read().unwrap();

        // if we receive enough blames, even if the timer did not go off yet,
        // we send out the blame message
        if blame_count == f || timer_expired {
            // same as if the timer expired on the node
            self.broadcast_to_others(NetworkCommand::Blame {
                socket_addr,
                view: current_view,
                timer_expired: false,
            })
            .await;
        }
        // broadcast commit, then try commit
        if blame_count >= quorum_count {
            info!("Enough nodes emmitted a blame response, starting view-change");
            // same as if the timer expired on the node

            self.broadcast(NetworkCommand::ViewChange {
                socket_addr,
                new_view: current_view + 1,
                highest_lock: highest_view_lock,
            })
            .await;
        }
        Ok(None)
    }
}
