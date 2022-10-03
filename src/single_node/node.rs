use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, Receiver, Writer},
    store::Store,
};
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use lib::command::Command;

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

#[derive(Clone)]
struct StoreHandler {
    pub store: Store,
}

#[async_trait]
impl MessageHandler for StoreHandler {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let result = match request {
            Command::Set { key, value } => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
            }
            Command::Get { key } => self.store.read(key.clone().into()).await,
        };

        let result = result.map_err(|e| e.to_string());

        info!("Sending response {:?}", result);
        let reply = bincode::serialize(&result)?;
        Ok(writer.send(reply.into()).await?)
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
        StoreHandler {
            store: store.clone(),
        },
    )
    .await
    .unwrap()
}
