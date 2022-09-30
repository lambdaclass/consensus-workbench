use std::net::SocketAddr;

use bytes::Bytes;
use lib::command::KeyValueCommand;
use lib::network::ReliableSender;
use log::info;

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

    // set a value
    let message: Bytes = bincode::serialize(&KeyValueCommand::Set {
        key: "name".into(),
        value: "facundo".into(),
    })?
    .into();
    let cancel_handler = sender.send(address, message).await;
    assert!(cancel_handler.await.is_ok());
    info!("value was set");

    //get the value
    let message: Bytes = bincode::serialize(&KeyValueCommand::Get { key: "name".into() })?.into();
    let cancel_handler = sender.send(address, message).await;

    match cancel_handler.await {
        Ok(response) => {
            let response: Result<Option<Vec<u8>>, ()> =
                bincode::deserialize(&response).expect("failed to deserialize response");
            match response {
                Ok(Some(value)) => info!("received value {}", String::from_utf8_lossy(&value)),
                Ok(None) => info!("value is missing"),
                _error => info!("store command failed"),
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
