use std::net::SocketAddr;

use bytes::Bytes;
use log::info;
use lib::network::ping::PingMessage;
use lib::network::ReliableSender;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // using a reliable sender to get a response back
    let mut sender = ReliableSender::new();
    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    let message: Bytes = bincode::serialize(&PingMessage::Ping)?.into();
    let cancel_handler = sender.send(address, message).await;

    match cancel_handler.await {
        Ok(response) => {
            let response: PingMessage = bincode::deserialize(&response)?;
            info!("received response: {:?}", response);
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
