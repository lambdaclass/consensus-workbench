/// This modules implements the most basic form of distributed system, a single node server that handles
/// client requests to a key/value store. There is no replication and this no fault-tolerance.
use clap::Parser;
use lib::network::Receiver as NetworkReceiver;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::node::{Node, NodeReceiverHandler};

mod node;
pub const CHANNEL_CAPACITY: usize = 1_000;

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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    log::info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    let (network_handle, _) = spawn_node_tasks(address).await;

    network_handle.await.unwrap();
}

async fn spawn_node_tasks(address: SocketAddr) -> (JoinHandle<()>, JoinHandle<()>) {
    let (network_sender, network_receiver) = mpsc::channel(CHANNEL_CAPACITY);

    let newtor_handle = tokio::spawn(async move {
        let mut node = Node::new();
        node.run(network_receiver).await;
    });

    let node_handle = tokio::spawn(async move {
        let receiver = NetworkReceiver::new(address, NodeReceiverHandler { network_sender });
        receiver.run().await;
    });

    (newtor_handle, node_handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::ClientCommand;
    use std::fs;
    use tokio::time::{sleep, Duration};
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_server() {
        fs::remove_dir_all(".db_single_node").unwrap_or_default();

        let address: SocketAddr = "127.0.0.1:6182".parse().unwrap();
        spawn_node_tasks(address).await;

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

        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v2".to_string(), reply.unwrap());

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v2".to_string(), reply.unwrap());
    }

    /// Send Get commands to the given address with delayed retries to give it time for a transaction
    /// to propagate. Fails if the expected value isn't read after 20 seconds.
    async fn assert_eventually_equals(address: SocketAddr, key: &str, value: &str) {
        let retries = FixedInterval::from_millis(100).take(200);
        let reply = Retry::spawn(retries, || async {
            let reply = ClientCommand::Get {
                key: key.to_string(),
            }
            .send_to(address)
            .await
            .unwrap();
            if reply.is_some() && reply.unwrap() == value.to_string() {
                Ok(())
            } else {
                Err(())
            }
        })
        .await;
        assert!(reply.is_ok());
    }
}
