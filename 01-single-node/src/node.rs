use futures::sink::SinkExt as _;
use network::{Receiver, MessageHandler, Writer};
use async_trait::async_trait;
use bytes::Bytes;
use std::error::Error;
use log::info;

use std::net::SocketAddr;

#[derive(Clone)]
struct PingHandler;

#[async_trait]
impl MessageHandler for PingHandler {
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        info!("Received request {:?}", message);
        writer.send(message).await.map_err(|e| e.into())
    }
}

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().env().init().unwrap();

    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    Receiver::spawn(address, PingHandler).await.unwrap()
}
