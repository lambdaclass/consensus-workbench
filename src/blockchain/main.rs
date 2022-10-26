/// This module is a binary that listens for TCP connections, runs a node and forwards to it incoming client and peer messages.
use clap::Parser;
use log::info;
use receiver::Receiver as NetworkReceiver;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

use crate::node::Node;

mod ledger;
mod node;
mod receiver;

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
    /// if running as a replica, this is the address of the primary
    #[clap(long, value_parser, value_name = "ADDR")]
    seed: Option<SocketAddr>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let network_address = SocketAddr::new(cli.address, cli.port);
    // FIXME lazily assuming +1000 for client port. move to CLI if anyone cares about it
    let client_address = SocketAddr::new(cli.address, cli.port + 1000);

    let (_, network_handle, _) = spawn_node_tasks(network_address, client_address, cli.seed).await;

    network_handle.await.unwrap();
}

/// Spawn the network receiver and a blockchain node with a channel to pass messages from one to the other.
async fn spawn_node_tasks(
    network_address: SocketAddr,
    client_address: SocketAddr,
    seed: Option<SocketAddr>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
    let (network_sender, network_receiver) = channel(CHANNEL_CAPACITY);
    let (client_sender, client_receiver) = channel(CHANNEL_CAPACITY);

    let node_handle = tokio::spawn(async move {
        let mut node = Node::new(network_address, seed);
        node.run(network_receiver, client_receiver).await;
    });

    let network_handle = tokio::spawn(async move {
        let receiver = NetworkReceiver::new(network_address, network_sender);
        receiver.run().await;
    });

    let client_handle = tokio::spawn(async move {
        let receiver = NetworkReceiver::new(client_address, client_sender);
        receiver.run().await;
    });

    (node_handle, network_handle, client_handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::ClientCommand;
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

    // since logger is meant to be initialized once and tests run in parallel,
    // run this before anything because otherwise it errors out
    #[ctor::ctor]
    fn init() {
        simple_logger::SimpleLogger::new().env().init().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_node() {
        let network_address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let client_address: SocketAddr = "127.0.0.1:7379".parse().unwrap();
        spawn_node_tasks(network_address, client_address, None).await;

        // get k1 -> null
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set k1
        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // eventually value gets into a block and get k1 -> v1
        assert_eventually_equals(client_address, "k1", "v1").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_nodes() {
        let network_address1: SocketAddr = "127.0.0.1:6279".parse().unwrap();
        let network_address2: SocketAddr = "127.0.0.1:6289".parse().unwrap();
        let network_address3: SocketAddr = "127.0.0.1:6299".parse().unwrap();

        let client_address1: SocketAddr = "127.0.0.1:7279".parse().unwrap();
        let client_address2: SocketAddr = "127.0.0.1:7289".parse().unwrap();
        let client_address3: SocketAddr = "127.0.0.1:7299".parse().unwrap();
        spawn_node_tasks(network_address1, client_address1, None).await;
        spawn_node_tasks(network_address2, client_address2, Some(network_address1)).await;
        spawn_node_tasks(network_address3, client_address3, Some(network_address1)).await;

        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address1)
        .await
        .unwrap();

        // eventually value gets into a block and get k1 -> v1 (in all nodes)
        assert_eventually_equals(client_address1, "k1", "v1").await;
        assert_eventually_equals(client_address2, "k1", "v1").await;
        assert_eventually_equals(client_address3, "k1", "v1").await;

        // set k=v2 (another node) -> eventually v2 in 1
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(client_address2)
        .await
        .unwrap();
        assert_eventually_equals(client_address1, "k1", "v2").await;
        assert_eventually_equals(client_address2, "k1", "v2").await;
        assert_eventually_equals(client_address3, "k1", "v2").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_node_catch_up() {
        // start two nodes
        let network_address1: SocketAddr = "127.0.0.1:6179".parse().unwrap();
        let network_address2: SocketAddr = "127.0.0.1:6189".parse().unwrap();

        let client_address1: SocketAddr = "127.0.0.1:7179".parse().unwrap();
        let client_address2: SocketAddr = "127.0.0.1:7189".parse().unwrap();
        spawn_node_tasks(network_address1, client_address1, None).await;
        spawn_node_tasks(network_address2, client_address2, Some(network_address1)).await;

        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address1)
        .await
        .unwrap();

        // eventually value gets into a block and get k1 -> v1 (in all nodes)
        assert_eventually_equals(client_address1, "k1", "v1").await;
        assert_eventually_equals(client_address2, "k1", "v1").await;

        // set k=v2 (another node) -> eventually v2 in 1
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(client_address2)
        .await
        .unwrap();
        assert_eventually_equals(client_address1, "k1", "v2").await;
        assert_eventually_equals(client_address2, "k1", "v2").await;

        // start another node, which should eventually catch up with the longest chain from its peers
        let network_address3: SocketAddr = "127.0.0.1:6199".parse().unwrap();
        let client_address3: SocketAddr = "127.0.0.1:7199".parse().unwrap();
        spawn_node_tasks(network_address3, client_address3, Some(network_address1)).await;
        assert_eventually_equals(client_address3, "k1", "v2").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn node_crash_recover() {
        let network_address1: SocketAddr = "127.0.0.1:6579".parse().unwrap();
        let network_address2: SocketAddr = "127.0.0.1:6589".parse().unwrap();
        let network_address3: SocketAddr = "127.0.0.1:6599".parse().unwrap();

        let client_address1: SocketAddr = "127.0.0.1:7579".parse().unwrap();
        let client_address2: SocketAddr = "127.0.0.1:7589".parse().unwrap();
        let client_address3: SocketAddr = "127.0.0.1:7599".parse().unwrap();

        spawn_node_tasks(network_address1, client_address1, None).await;
        // keep the handles to abort later
        let (node_handle, network_handle, client_handle) =
            spawn_node_tasks(network_address2, client_address2, Some(network_address1)).await;
        spawn_node_tasks(network_address3, client_address3, Some(network_address1)).await;

        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address1)
        .await
        .unwrap();

        // eventually value gets into a block and get k1 -> v1 (in all nodes)
        assert_eventually_equals(client_address1, "k1", "v1").await;
        assert_eventually_equals(client_address2, "k1", "v1").await;
        assert_eventually_equals(client_address3, "k1", "v1").await;

        // abort the 2nd node
        network_handle.abort();
        node_handle.abort();
        client_handle.abort();

        // send another transaction
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(client_address1)
        .await
        .unwrap();

        // the nodes that are still alive eventually update the value
        assert_eventually_equals(client_address3, "k1", "v2").await;
        assert_eventually_equals(client_address1, "k1", "v2").await;

        // start the second node again
        spawn_node_tasks(network_address2, client_address2, Some(network_address1)).await;

        // send a new transaction to the fresh right away
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v3".to_string(),
        }
        .send_to(client_address2)
        .await
        .unwrap();

        // the agreed ledger should eventually include the last transaction
        assert_eventually_equals(client_address1, "k1", "v3").await;
        assert_eventually_equals(client_address2, "k1", "v3").await;
        assert_eventually_equals(client_address3, "k1", "v3").await;
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
