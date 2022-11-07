use crate::node::Node;
/// This module is a binary that listens for TCP connections, runs a node and forwards to it incoming client and peer messages.
use clap::Parser;
use lib::network::Receiver;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::task::JoinHandle;

mod ledger;
mod node;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    client_port: u16,
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6200)]
    network_port: u16,
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

    info!(
        "Node socket for client request {}:{}, network request: {}:{}",
        cli.address, cli.client_port, cli.address, cli.network_port
    );

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let network_address = SocketAddr::new(cli.address, cli.client_port);
    let client_address = SocketAddr::new(cli.address, cli.network_port);

    let (_, network_handle, _) = spawn_node_tasks(network_address, client_address, cli.seed).await;

    network_handle.await.unwrap();
}

/// Spawn the client and network receivers and a blockchain node that listents to messages from both.
async fn spawn_node_tasks(
    network_address: SocketAddr,
    client_address: SocketAddr,
    seed: Option<SocketAddr>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
    // listen for peer network tcp connections
    let (network_tcp_receiver, network_channel_receiver) = Receiver::new(network_address);
    let network_handle = tokio::spawn(async move {
        network_tcp_receiver.run().await;
    });

    // listen for client command tcp connections
    let (client_tcp_receiver, client_channel_receiver) = Receiver::new(client_address);
    let client_handle = tokio::spawn(async move {
        client_tcp_receiver.run().await;
    });

    // run a task to manage the blockchain node state, listening for messages from client and network
    let mut node = Node::new(network_address, seed);
    let node_handle = tokio::spawn(async move {
        node.run(network_channel_receiver, client_channel_receiver)
            .await;
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
    #[serial_test::serial]
    async fn single_node() {
        let network_address: SocketAddr = "127.0.0.1:9101".parse().unwrap();
        let client_address: SocketAddr = "127.0.0.1:9102".parse().unwrap();
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
    #[serial_test::serial]
    async fn multiple_nodes() {
        let network_address1: SocketAddr = "127.0.0.1:9103".parse().unwrap();
        let network_address2: SocketAddr = "127.0.0.1:9104".parse().unwrap();
        let network_address3: SocketAddr = "127.0.0.1:9105".parse().unwrap();

        let client_address1: SocketAddr = "127.0.0.1:9106".parse().unwrap();
        let client_address2: SocketAddr = "127.0.0.1:9107".parse().unwrap();
        let client_address3: SocketAddr = "127.0.0.1:9108".parse().unwrap();
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
    #[serial_test::serial]
    async fn new_node_catch_up() {
        // start two nodes
        let network_address1: SocketAddr = "127.0.0.1:9109".parse().unwrap();
        let network_address2: SocketAddr = "127.0.0.1:9110".parse().unwrap();

        let client_address1: SocketAddr = "127.0.0.1:9111".parse().unwrap();
        let client_address2: SocketAddr = "127.0.0.1:9112".parse().unwrap();
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
        let network_address3: SocketAddr = "127.0.0.1:9113".parse().unwrap();
        let client_address3: SocketAddr = "127.0.0.1:9114".parse().unwrap();
        spawn_node_tasks(network_address3, client_address3, Some(network_address1)).await;
        assert_eventually_equals(client_address3, "k1", "v2").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn node_crash_recover() {
        let network_address1: SocketAddr = "127.0.0.1:9115".parse().unwrap();
        let network_address2: SocketAddr = "127.0.0.1:9116".parse().unwrap();
        let network_address3: SocketAddr = "127.0.0.1:9117".parse().unwrap();

        let client_address1: SocketAddr = "127.0.0.1:9118".parse().unwrap();
        let client_address2: SocketAddr = "127.0.0.1:9119".parse().unwrap();
        let client_address3: SocketAddr = "127.0.0.1:9120".parse().unwrap();

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
            if reply.is_some() && reply.unwrap() == value {
                Ok(())
            } else {
                Err(())
            }
        })
        .await;
        assert!(reply.is_ok());
    }
}
