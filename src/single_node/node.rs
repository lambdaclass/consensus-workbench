/// This module contains an implementation of a single node.
/// The node keeps a state, wich could be updated by tcp requests.
use anyhow::Result;
use lib::command::CommandResult;
use lib::{command::ClientCommand, store::Store};
use log::error;
use tokio::sync::mpsc::Receiver;
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
        mut client_receiver: Receiver<(ClientCommand, oneshot::Sender<CommandResult>)>,
    ) {
        while let Some((message, reply_sender)) = client_receiver.recv().await {
            let result = self
                .handle_msg(message.clone())
                .await
                .map_err(|e| e.to_string());
            if let Err(error) = reply_sender.send(result) {
                error!("failed to send message {:?} response {:?}", message, error);
            };
        }
    }

    /// Process each messages coming from clients
    pub async fn handle_msg(&mut self, message: ClientCommand) -> Result<Option<String>> {
        match message {
            ClientCommand::Set { key, value } => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
                    .unwrap();

                Ok(Some(value))
            }
            ClientCommand::Get { key } => {
                if let Ok(Some(val)) = self.store.read(key.clone().into()).await {
                    let value = String::from_utf8(val).unwrap();
                    return Ok(Some(value));
                }

                Ok(None)
            }
        }
    }
}
