use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::sink::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver, Writer};
use std::error::Error;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

use network::ping::PingMessage;


#[derive(Parser)]
#[clap(
    author,
    version,
    about
)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
}

#[derive(Clone)]
struct PingHandler;

#[async_trait]
impl MessageHandler for PingHandler {
    async fn dispatch(&self, writer: &mut Writer, bytes: Bytes) -> Result<(), Box<dyn Error>> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let reply = match request {
            PingMessage::Ping => PingMessage::Pong,
            PingMessage::Other(msg) => PingMessage::Other(format!("echo: {}", msg)),
            _ => PingMessage::Other("unsupported message".to_string()),
        };
        let reply: Bytes = bincode::serialize(&reply)?.into();
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

    let address = SocketAddr::new(cli.address, cli.port);
    Receiver::spawn(address, PingHandler).await.unwrap()
}
