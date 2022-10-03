use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use lib::command::Command;
use lib::network::ReliableSender;
use log::{error, info};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The key/value store command to execute.
    #[clap(subcommand)]
    command: Command,

    /// The network port of the node where to send txs.
    #[clap(long, short, value_parser, value_name = "INT", default_value_t = 6100)]
    port: u16,

    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "INT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    info!("Node socket: {}{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()?;

    // using a reliable sender to get a response back
    let mut sender = ReliableSender::new();
    let address = SocketAddr::new(cli.address, cli.port);

    // if two args are passed, set to DB, otherwise get
    let message: Bytes = bincode::serialize(&cli.command)?.into();
    let reply_handler = sender.send(address, message).await;

    let response = reply_handler.await?;
    let response: Result<Option<String>, String> = bincode::deserialize(&response)?;
    match response {
        Ok(Some(value)) => info!("{}", value),
        Ok(None) => info!("null"),
        Err(error) => error!("ERROR {}", error),
    }
    Ok(())
}
