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
use std::net::SocketAddr;

use lib::command::Command;

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

use State::*;

impl Node {
    pub fn primary(peers: Vec<SocketAddr>, db_path: &str) -> Self {
        Self {
            state: Primary,
            store: Store::new(db_path).unwrap(),
            peers,
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

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let result = match (self.state, request) {
            (Primary, Command::Set { key, value }) => {
                // TODO review: since we're always using the same serialization format,
                // would it make sense to always handle serialization inside the networking calls?
                let sync_message: Bytes = bincode::serialize(&Command::SyncSet {
                    key: key.clone(),
                    value: value.clone(),
                })
                .unwrap()
                .into();

                // forward the command to all replicas and wait for them to respond
                let handlers = self
                    .sender
                    .broadcast(self.peers.clone(), sync_message)
                    .await;
                futures::future::join_all(handlers).await;

                self.store.write(key.into(), value.into()).await
            }
            (Backup, Command::SyncSet { key, value }) => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
            }
            (_, Command::Get { key }) => self.store.read(key.clone().into()).await,
            _ => Err(anyhow!("Unhandled command")),
        };

        // convert the error into something serializable
        let result = result.map_err(|e| e.to_string());

        info!("Sending response {:?}", result);
        let reply = bincode::serialize(&result)?;
        Ok(writer.send(reply.into()).await?)
    }
}
