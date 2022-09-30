use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, Receiver, Writer},
    store::Store,
};
use log::info;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use lib::command::KeyValueCommand;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
}

// FIXME rename
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
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await;

                let result: Result<Option<String>, String> = Ok(Some(value));
                bincode::serialize(&result)?.into()
            }
            KeyValueCommand::Get { key } => {
                let result = self
                    .store
                    .read(key.clone().into())
                    .await
                    .map_err(|_| "error reading store");

                bincode::serialize(&result)?.into()
            }
        };
        writer.send(reply).await.map_err(|e| e.into())
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // TODO may need some parametrization of path to support multiple instances
    let store = Store::new(".db_single_node").unwrap();
    let address = SocketAddr::new(cli.address, cli.port);
    Receiver::spawn(
        address,
        PingHandler {
            store: store.clone(),
        },
    )
    .await
    .unwrap()
}
