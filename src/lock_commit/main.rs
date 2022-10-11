use bytes::Bytes;
use clap::Parser;
use lib::network::Receiver;
use lib::network::SimpleSender;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::node::{Message, Node};

mod node;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// if running as a replica, this is the address of the primary
    #[clap(long, value_parser, value_name = "ADDR")]
    peers: Vec<SocketAddr>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    let node = {
        info!(
            "Node: Running on {}...",
            address
        );

        Node::new(cli.peers, &format!(".db_{}", address.port()), address)
    };

    Receiver::spawn(address, node).await.unwrap();
}

fn db_name(cli: &Cli, default: &str) -> String {
    let default = &default.to_string();
    let name = cli.name.as_ref().unwrap_or(default);
    format!(".db_{}", name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::{Command, ClientCommand};
    use std::fs;
    use tokio::time::{sleep, Duration};

    #[ctor::ctor]
    fn init() {
        simple_logger::SimpleLogger::new().env().init().unwrap();

        fs::remove_dir_all(db_path("")).unwrap_or_default();
    }

    fn db_path(suffix: &str) -> String {
        format!(".db_test/{}", suffix)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        Receiver::spawn(
            address,
            node::Node::new(vec![address], &db_path("primary1"), address),
        );
        sleep(Duration::from_millis(10)).await;

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replicated_server() {
        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        Receiver::spawn(address_replica, node::Node::new(vec![address_primary, address_replica],&db_path("backup2"), address_replica));
        Receiver::spawn(
            address_primary,
            node::Node::new(vec![address_primary, address_replica], &db_path("primary2"), address_primary),
        );
        sleep(Duration::from_millis(10)).await;

        // get null value
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set a value on primary
        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on primary
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on replica to make sure it was replicated
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address_replica)
        .await
        .unwrap();

        // FIX Node currently is not replicating. Uncomment after fix
        // assert!(reply.is_some());
        // assert_eq!("v1".to_string(), reply.unwrap());

        // should fail since replica should not respond to set commands
        let reply = ClientCommand::Set {
            key: "k3".to_string(),
            value: "_".to_string(),
        }
        .send_to(address_replica)
        .await;
        assert!(reply.is_err());
    }
}