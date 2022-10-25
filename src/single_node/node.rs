/// This module contains an implementation of a single node.
/// The node keeps a state, wich could be updated by tcp requests.
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;
use lib::{
    command::ClientCommand,
    network::{MessageHandler, Writer},
    store::Store,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;


#[derive(Clone)]
/// The node keep a key value store.
pub struct Node {
    pub store: Store,
}

impl Node {
    pub fn new() -> Self {
        Self {
            store: Store::new(".db_single_node").unwrap(),
        }
    }

    /// Runs the node to process network messages incoming in the given receiver
    pub async fn run(
        &mut self,
        mut network_receiver: Receiver<(ClientCommand, oneshot::Sender<Result<Option<Vec<u8>>>>)>,
    ) -> () {
        while let Some((message, reply_sender)) = network_receiver.recv().await {
            self.handle_msg(message, reply_sender).await;
        }
    }

    /// Process each messages coming from clients
    pub async fn handle_msg(&mut self, message: ClientCommand, reply_sender: oneshot::Sender<Result<Option<Vec<u8>>>>) -> (){
        let result = match message {
            ClientCommand::Set { key, value } => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
            }
            ClientCommand::Get { key } => self.store.read(key.clone().into()).await,
        };
        let _ = reply_sender.send(result);
    }
}



#[derive(Clone)]
pub struct NodeReceiverHandler {
    /// Used to forward incoming TCP messages to the node
    pub network_sender: Sender<(ClientCommand, oneshot::Sender<Result<Option<Vec<u8>>>>)>,
}

#[async_trait]
impl MessageHandler for NodeReceiverHandler {
    /// When a TCP message is received, interpret it as a node::Message and forward it to the node task.
    /// Send the node's response back through the TCP connection.
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request: ClientCommand = bincode::deserialize(&bytes)?;
        log::info!("Received request {:?}", request);

        let (reply_sender, reply_receiver) = oneshot::channel();
        self.network_sender.send((request, reply_sender)).await?;
        let reply = reply_receiver.await?.map_err(|e| e.to_string());

        let reply = bincode::serialize(&reply)?;
        log::info!("Sending response {:?}", reply);

        Ok(writer.send(reply.into()).await?)
    }
}
