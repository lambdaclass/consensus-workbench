use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use clap::Parser;
use bytes::Bytes;
use log::info;
use network::ping::PingMessage;
use network::ReliableSender;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[clap(
    author,
    version,
    about
)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(value_parser, value_name = "INT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "INT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// The node's timeout value in milliseconds.
    #[clap(short, long, value_parser, value_name = "INT", default_value_t = 15000)]
    timeout: u64,
    /// Message to send as Other({message}).
    #[clap(short, long, value_parser, value_name ="STRING")]
    message: Option<String>,
    /// Wait time between messages. If 0, it only sends one message and exits.
    #[clap(short, long, value_parser, value_name = "INT", default_value_t = 0)]
    wait_time: u64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();

    info!("Node socket: {}{}", cli.address, cli.port);
    info!("Wait time between transactions: {}", cli.wait_time);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // using a reliable sender to get a response back
    let mut sender = ReliableSender::new();
    let address = SocketAddr::new(cli.address, cli.port);

    let ping_message = match cli.message{
        None => PingMessage::Ping,
        Some(msg) => PingMessage::Other(msg)
    };

    let result = loop {
        
        let message: Bytes = bincode::serialize(&ping_message)?.into();
        let cancel_handler = sender.send(address, message).await;

        let msg_result = match cancel_handler.await {
            Ok(response) => {
                let response: PingMessage = bincode::deserialize(&response)?;
                info!("received response: {:?}", response);
                Ok(())
            }
            Err(error) => Err(error.into())
        };

        if cli.wait_time == 0 { break msg_result; }
        std::thread::sleep(std::time::Duration::from_millis(cli.wait_time));
    };

    result
}
