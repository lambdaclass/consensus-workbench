use clap::Parser;
use lib::network::Receiver;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::primary::Node;

mod primary;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// Run as replica
    #[arg(long, value_parser)]
    replica: bool,
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// Where to replicate to, if not running as a replica.
    #[clap(short, long, value_parser, value_name = "ADDR")]
    replicate_to: Option<SocketAddr>,
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

    let node = match cli.replica {
        false => {
            info!("Primary: Running as primary on {}.", address);

            // TODO we will eventually handle multiple peers, but for now we keep passing the single replica
            cli.replicate_to
                .is_some()
                .then(|| info!("Primary: Replicating to {}.", cli.replicate_to.unwrap()));

            Node::primary(vec![cli.replicate_to.unwrap()])
        }
        true => {
            info!(
                "Replica: Running as replica on {}, waiting for commands from the primary node...",
                address
            );
            Node::backup()
        }
    };
    Receiver::spawn(address, node).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::Command;
    use std::fs;
    use tokio::time::{sleep, Duration};

    // since logger is meant to be initialized once and tests run in parallel,
    // run this before anything because otherwise it errors out
    #[ctor::ctor]
    fn init() {
        simple_logger::SimpleLogger::new()
            .env()
            .with_level(log::LevelFilter::Info)
            .init()
            .unwrap();
    }

    fn create_store(name_suffix: &str) -> Store {
        let db_path = format!(".db_test_{}", name_suffix);
        let db_path = db_path.as_ref();

        fs::remove_dir_all(db_path).unwrap_or_default();
        Store::new(db_path).unwrap()
    }

    #[tokio::test]
    async fn test_only_primary_server() {
        let store = create_store("primary1");

        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let sender = ReliableSender::new();
        Receiver::spawn(
            address,
            primary::Node {
                state: primary::State::Primary,
                store,
                peers: Vec::new(),
                sender,
            },
        );
        sleep(Duration::from_millis(10)).await;

        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        let reply = Command::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test]
    async fn test_replicated_server() {
        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        Receiver::spawn(
            address_replica,
            primary::Node {
                store: create_store("replica"),
                state: State::Backup,
                peers: vec![],
                sender: ReliableSender::new(),
            },
        );
        Receiver::spawn(
            address_primary,
            primary::Node {
                state: primary::State::Primary,
                store: create_store("primary"),
                peers: vec![address_replica],
                sender: ReliableSender::new(),
            },
        );
        sleep(Duration::from_millis(10)).await;

        // get null value
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set a value on primary
        let reply = Command::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on primary
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on replica to make sure it was replicated
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address_replica)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // should fail since replica should not respond to set commands
        let reply = Command::Set {
            key: "k3".to_string(),
            value: "_".to_string(),
        }
        .send_to(address_replica)
        .await;
        assert!(reply.is_err());
    }
}
