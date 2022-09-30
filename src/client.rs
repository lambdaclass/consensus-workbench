use bytes::Bytes;
use clap::Parser;
use lib::command::KeyValueCommand;
use lib::network::ReliableSender;
use log::{error, info};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(long, short, value_parser, value_name = "INT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "INT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// FIXME
    key: String,
    /// FIXME
    value: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    info!("Node socket: {}{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // using a reliable sender to get a response back
    let mut sender = ReliableSender::new();
    let address = SocketAddr::new(cli.address, cli.port);

    // TODO consider explicitly choosing get or set in the args
    let command = if let Some(value) = cli.value {
        KeyValueCommand::Set {
            key: cli.key.into(),
            value: value.into(),
        }
    } else {
        KeyValueCommand::Get {
            key: cli.key.into(),
        }
    };

    let message: Bytes = bincode::serialize(&command)?.into();
    let cancel_handler = sender.send(address, message).await;

    match cancel_handler.await {
        Ok(response) => {
            let response: Result<Option<Vec<u8>>, ()> =
                bincode::deserialize(&response).expect("failed to deserialize response");

            match (command, response) {
                (_, Ok(Some(value))) => info!("{}", String::from_utf8_lossy(&value)),
                (KeyValueCommand::Get { .. }, Ok(None)) => info!("NOT FOUND"),
                (KeyValueCommand::Set { .. }, Ok(_)) => info!("OK"),
                _error => error!("ERROR"),
            }
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
