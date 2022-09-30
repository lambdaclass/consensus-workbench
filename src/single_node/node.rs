use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, Receiver, Writer},
    store::Store,
};
use log::info;
use std::error::Error;

use lib::command::KeyValueCommand;

use std::net::SocketAddr;

#[derive(Clone)]
struct PingHandler {
    pub store: Store,
}

#[async_trait]
impl MessageHandler for PingHandler {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let reply = match request {
            KeyValueCommand::Set { key, value } => {
                self.store.write(key, value).await;

                // FIXME add error handling
                let result: Result<(), ()> = Ok(());
                bincode::serialize(&result)?.into()
            }
            KeyValueCommand::Get { key } => {
                let value = self.store.read(key).await.unwrap();

                // FIXME add error handling,
                let result: Result<Option<Vec<u8>>, ()> = Ok(value);
                bincode::serialize(&result)?.into()
            }
        };
        writer.send(reply).await.map_err(|e| e.into())
    }
}

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // TODO may need some parametrization of path to support multiple instances
    let store = Store::new(".db_single_node").unwrap();

    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    Receiver::spawn(
        address,
        PingHandler {
            store: store.clone(),
        },
    )
    .await
    .unwrap()
}
