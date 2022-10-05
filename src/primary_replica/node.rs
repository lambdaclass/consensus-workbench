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
    /// if running as a replica, this is the address of the primary
    #[clap(short, long, value_parser, value_name = "ADDR")]
    primary: Option<SocketAddr>,
    /// Store name, useful to have several nodes in same machine.
    #[clap(short, long)]
    db_name: Option<String>,
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
            cli.primary
                .is_some()
                .then(|| info!("Primary: Replicating to {}.", cli.primary.unwrap()));
            
            let db_name = db_name(cli.db_name.unwrap_or("primary".to_string()));
            Node::primary(vec![cli.primary.unwrap()], &db_name)
        }
        true => {
            info!(
                "Replica: Running as replica on {}, waiting for commands from the primary node...",
                address
            );
            let db_name = db_name(cli.db_name.unwrap_or("replica".to_string()));
            Node::backup(&db_name)
        }
    };
    Receiver::spawn(address, node).await.unwrap();
}

fn db_name(name: String) -> String {
    format!(".db_{}", name)
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

    #[tokio::test]
    async fn test_only_primary_server() {
        fs::remove_dir_all(".db_test_primary1").unwrap_or_default();
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        Receiver::spawn(
            address,
            primary::Node::primary(Vec::new(), ".db_test_primary1"),
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
        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        Receiver::spawn(address_replica, primary::Node::backup(".db_test_backup2"));
        Receiver::spawn(
            address_primary,
            primary::Node::primary(vec![address_replica], ".db_test_primary2"),
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
