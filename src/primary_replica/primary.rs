/// This modules contains a basic implementation os a primary node message handler
/// Every Set command to a primary node will be relayed on the `replica_socket` node
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, SimpleSender, Writer},
    store::Store,
};
use log::info;
use std::net::SocketAddr;

use lib::command::Command;

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct SingleNodeServer {
    pub store: Store,
    pub peers: Vec<SocketAddr>,
    pub sender: SimpleSender,
}

pub enum NodeState {
    Primary,
    Backup,
}

#[async_trait]
impl MessageHandler for SingleNodeServer {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let result = match request {
            Command::Set { key, value } => {
                // synced set

                // TODO review if there's a better network primitive for sending to all peers
                for peer in self.peers.iter() {
                    // TODO review: since we're always using the same serialization format,
                    // would it make sense to always handle serialization inside the networking calls?
                    let sync_message: Bytes = bincode::serialize(&Command::SyncSet {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .unwrap()
                    .into();

                    self.sender.send(*peer, sync_message).await;
                }

                self.store.write(key.into(), value.into()).await
            }
            Command::Get { key } => self.store.read(key.clone().into()).await,
            _ => Err(anyhow!("Unhandled command")),
        };

        // convert the error into something serializable
        let result = result.map_err(|e| e.to_string());

        info!("Sending response {:?}", result);
        let reply = bincode::serialize(&result)?;
        Ok(writer.send(reply.into()).await?)
    }
}
