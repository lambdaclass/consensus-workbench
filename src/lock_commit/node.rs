/// This module contains an implementation nodes that can run in primary or backup mode.
/// Every Set command to a primary node will be broadcasted reliably for the backup nodes to replicate it.
/// We plan to add backup promotion in case of primary failure.
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{sink::SinkExt as _};
use lib::{
    command::{ClientCommand, Command, CommandView, self},
    network::{MessageHandler, ReliableSender, Writer},
    store::Store,
};
use log::info;
use std::{collections::HashSet, net::SocketAddr, sync::{Arc, Mutex, RwLock}, error::Error};

use lib::command::NetworkCommand;

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct Node {
    pub socket_address: SocketAddr,
    pub state: State,
    pub store: Store,
    pub peers: Vec<SocketAddr>,
    pub sender: ReliableSender,

    pub current_primary_index: usize,

    pub current_view: Arc<RwLock<u128>>,
    command_view_lock: Arc<RwLock<CommandView>>,

    // the amount of peers which responded with "Lock"
    // note: if we were to create a QC to store in the blockchain,
    // we would need to store signatures from peers here
    pub lock_responses: Arc<Mutex<HashSet<SocketAddr>>>,
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

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = bincode::deserialize(&bytes)?;
        info!("{}: Received request {:?}", self.socket_address, request);

        let result = match (self.state, request) {
            (_, Command::Client(cmd @ ClientCommand::Get { key: _ })) => {
                // as a 'hack': to make it simpler, we can just forward the Get command to handle_client_message
                // if you comment this match code block, Get requests will also require quorum
                self.handle_client_command(cmd).await
            }
            (Primary, Command::Client(client_comand)) => {
                // we advance the view according to the primary and propose it
                let command_view = CommandView {
                    command: client_comand,
                    view: *self.current_view.read().unwrap() + 1,
                };

                let command = NetworkCommand::Propose {
                    command_view: command_view.clone(),
                };

                // since we are primary, we lock the command view and add it to the quorum set
                self.lock_command_view(&command_view);
                //self.lock_responses.clone().lock().unwrap().insert(self.socket_address);

                info!("Received command, broadcasting Propose");
                self.broadcast(command).await;
                Ok(None)
            }
            (
                Primary,
                Command::Network(NetworkCommand::Lock {
                    socket_addr,
                    command_view,
                }),
            ) => {
                if command_view.view <= *self.current_view.read().unwrap() {
                    info!("Received command with an old, previously committed view, discarding");
                    return Ok(());
                }

                let _ = self.lock_responses.lock().unwrap().insert(socket_addr);
                let response_count = self.lock_responses.lock().unwrap().len();
                // TODO: in reality this is a function of the amount of omission faults we want the network to tolerate, f<n/2
                let quorum_count = ((self.peers.len()) / 2) + 1;

                info!("Received lock, did we get quorum? {} responses so far vs expected quorum of {} ", response_count, quorum_count);

                // broadcast commit, then try commit
                if response_count >=  quorum_count{
                    info!("Quorum achieved, sending out Commit message!");
                    self.try_commit(command_view.clone()).await.expect("Error committing command as primary");
                    self.broadcast(NetworkCommand::Commit {
                        command_view: command_view,
                    })
                    .await;
                }
                Ok(None)
            }
            (_, Command::Network(NetworkCommand::Propose { command_view })) => {
                // TODO: You should only lock if view number is expected
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
            (Primary, Command::Network(NetworkCommand::Commit { .. })) => Ok(None),
            (Backup, Command::Network(NetworkCommand::Commit { command_view })) => {
                let last_view = command_view.view;
                if let Ok(result) = self.try_commit(command_view).await {
                    info!(
                        "{}: Committed command, response was {:?}",
                        self.socket_address,
                        result.unwrap()
                    );
                    let mut lock = self.current_view.write().unwrap(); // update last valid view number
                    *lock = last_view;
                    return Ok(());
                }
                Err(anyhow!("Error committing command"))
            }
            _ => {
                info!("{} :unhandled command", self.socket_address);
                Err(anyhow!("Unhandled command"))
            }
        };

        info!("{}: Sending response {:?}", self.socket_address, result);

        std::thread::sleep(std::time::Duration::from_millis(100));
        // let ack: Result<Option<Vec<u8>>, String> = Ok(Some("ACK".into()));
        // convert the error into something serializable
        let result = result.map_err(|e| e.to_string());

        let reply = bincode::serialize(&result)?;
        let _ = writer.send(reply.into()).await?;
        Ok(())
    }
}

impl Node {
    pub fn new(peers: Vec<SocketAddr>, db_path: &str, address: SocketAddr) -> Self {
        Self {
            state: if address == *peers.get(0).unwrap() {
                Primary
            } else {
                Backup
            },
            store: Store::new(db_path).unwrap(),
            peers: peers,
            sender: ReliableSender::new(),
            current_view: Arc::new(RwLock::new(0)),
            command_view_lock: Arc::new(RwLock::new(CommandView::new())),
            lock_responses: Arc::new(Mutex::new(HashSet::new())),
            current_primary_index: 0,
            socket_address: address,
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

     /*    let other_peers = self
            .peers
            .iter()
            .copied()
            .filter(|x| *x != self.socket_address)
            .collect();
 */
        // forward the command to all replicas and wait for them to respond
        let handlers = self.sender.broadcast(&self.peers, message).await;

        futures::future::join_all(handlers).await;
    }

    async fn send_to_primary(&mut self, cmd: Command) {
        let message: Bytes = bincode::serialize(&cmd)
            .unwrap()
            .into();
        
        let primary_address = *(self
            .peers
            .get(self.current_primary_index)
            .expect("Error getting primary"));

        // forward the command to all replicas and wait for them to respond
        let _ = self.sender.send(primary_address, message).await.await;
    }

    async fn try_commit(&mut self, command_view: CommandView) -> Result<Option<Vec<u8>>> {
        // handle command, remove command lock (Primray already commits when quorum is achieved)
        if *self.command_view_lock.read().unwrap() != command_view {
            info!("{}: trying to commit {:?} but we had locked {:?}", self.socket_address,command_view ,*self.command_view_lock.read().unwrap());
            // we are trying to commit something that has not been locked correctly,
            // so there must have been some fault
            return Err(anyhow!(
                "Trying to commit a command-view that had not been previously locked"
            ));
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
        info!("{}: Locked command view {:?}", self.socket_address,*self.command_view_lock.read().unwrap());
    }

    fn clear_cmd_view_lock(&mut self) {
        *self.command_view_lock.write().unwrap() = CommandView::new();
    }
}
