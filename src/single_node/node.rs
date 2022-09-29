use futures::sink::SinkExt as _;
use network::{Receiver, MessageHandler, Writer};
use async_trait::async_trait;
use bytes::Bytes;
use std::error::Error;
use log::info;
use serde::{Deserialize, Serialize};

use std::net::SocketAddr;

#[derive(Debug, Serialize, Deserialize)]
pub enum PingMessage {
    Ping,
    Pong,
    Other(String)
}

#[derive(Clone)]
struct PingHandler;

#[async_trait]
impl MessageHandler for PingHandler {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let reply =
        match request {
            PingMessage::Ping => PingMessage::Pong,
            _ => PingMessage::Other("unsupported message".to_string()),
        };
        let reply: Bytes = bincode::serialize(&reply)?.into();
        writer.send(reply).await.map_err(|e| e.into())
    }
}

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().env().with_level(log::LevelFilter::Info).init().unwrap();

    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    Receiver::spawn(address, PingHandler).await.unwrap()
}
