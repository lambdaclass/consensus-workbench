/// This modules contains a basic implementation for a replica node message handler
/// Set commands will return error because they are supposed to be handled by primary
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, Writer},
    store::Store,
};
use log::info;

use lib::command::Command;

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
pub struct ReplicaNodeServer {
    pub store: Store,
}

#[async_trait]
impl MessageHandler for ReplicaNodeServer {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let result = match request {
            Command::Set { .. } => Err(anyhow!("User cannot issue set commands to replicas")),
            Command::SyncSet { key, value } => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
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
