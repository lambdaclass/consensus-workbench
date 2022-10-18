use bytes::Bytes;
use clap::Parser;
use lib::{network::Receiver as NetworkReceiver, command::Command};
use log::info;
use tokio::sync::mpsc::{channel, Receiver, Sender, self};

use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, time::{Duration, Instant}, sync::{Mutex, Arc, RwLock}};

use crate::node::{Node, State};

mod node;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6101)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// if running as a replica, this is the address of the primary
    #[clap(
        long,
        value_parser,
        value_name = "ADDR",
        use_value_delimiter = true,
        value_delimiter = ' '
    )]
    peers: Vec<SocketAddr>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let timer_start = Arc::new(RwLock::new(Instant::now()));

    let address = SocketAddr::new(cli.address, cli.port);

    let node = Node::new(cli.peers, &format!(".db_{}", address.port()), address, timer_start.clone());
    // todo: this needs to change and use channels
    tokio::spawn(async move {
        let delta = Duration::from_millis(1000);

        loop {
            if timer_start.read().unwrap().elapsed() > delta * 8 {
                *timer_start.write().unwrap() = Instant::now();
                info!("{}: timer expired!", address);
                let blame_message = lib::command::NetworkCommand::Blame { socket_addr: address, view: 0, timer_expired: true };
                let _ = blame_message.send_to(address).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    info!(
        "Node: Running on {}. Primary = {}...",
        node.socket_address,
        matches!(node.get_state(), State::Primary)
    );
    NetworkReceiver::spawn(address, node).await.unwrap();


}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::ClientCommand;
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
        let timer_start = Arc::new(RwLock::new(Instant::now()));
        NetworkReceiver::spawn(
            address,
            node::Node::new(vec![address], &db_path("primary1"), address, timer_start.clone()),
        );
        sleep(Duration::from_millis(10)).await;

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        let _ = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();

        sleep(Duration::from_millis(10)).await;

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
        let timer_start = Arc::new(RwLock::new(Instant::now()));
        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        NetworkReceiver::spawn(
            address_replica,
            node::Node::new(
                vec![address_primary, address_replica],
                &db_path("backup2"),
                address_replica,
                timer_start.clone()
            ),
        );
        NetworkReceiver::spawn(
            address_primary,
            node::Node::new(
                vec![address_primary, address_replica],
                &db_path("primary2"),
                address_primary,
                timer_start.clone()
            ),
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
        let _ = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();

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
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }
}
