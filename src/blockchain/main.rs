/// This module is a binary that listens for TCP connections, runs a node and forwards to it incoming client and peer messages.
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::SinkExt;
use lib::network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::node::Node;

mod ledger;
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
    /// if running as a replica, this is the address of the primary
    #[clap(long, value_parser, value_name = "ADDR")]
    seed: Option<SocketAddr>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    let (network_handle, _) = spawn_node_tasks(address, cli.seed).await;

    network_handle.await.unwrap();
}

/// Spawn the network receiver and a blockchain node with a channel to pass messages from one to the other.
async fn spawn_node_tasks(
    address: SocketAddr,
    seed: Option<SocketAddr>,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let (network_sender, network_receiver) = channel(CHANNEL_CAPACITY);

    let network_handle = tokio::spawn(async move {
        let mut node = Node::new(address, seed);
        node.run(network_receiver).await;
    });

    let node_handle = tokio::spawn(async move {
        let receiver = NetworkReceiver::new(address, NodeReceiverHandler { network_sender });
        receiver.run().await;
    });

    (network_handle, node_handle)
}

#[derive(Clone)]
struct NodeReceiverHandler {
    /// Used to forward incoming TCP messages to the node
    network_sender: Sender<(node::Message, oneshot::Sender<Result<Option<String>>>)>,
}

#[async_trait]
impl MessageHandler for NodeReceiverHandler {
    /// When a TCP message is received, interpret it as a node::Message and forward it to the node task.
    /// Send the node's response back through the TCP connection.
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = node::Message::deserialize(bytes)?;

        let (reply_sender, reply_receiver) = oneshot::channel();
        self.network_sender.send((request, reply_sender)).await?;
        let reply = reply_receiver.await?.map_err(|e| e.to_string());
        let reply = bincode::serialize(&reply)?;
        Ok(writer.send(reply.into()).await?)
    }
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
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        spawn_node_tasks(address, None).await;

        // get k1 -> null
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set k1
        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // eventually value gets into a block and get k1 -> v1
        assert_eventually_equals(address, "k1", "v1").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_nodes() {
        let address1: SocketAddr = "127.0.0.1:6279".parse().unwrap();
        let address2: SocketAddr = "127.0.0.1:6289".parse().unwrap();
        let address3: SocketAddr = "127.0.0.1:6299".parse().unwrap();
        spawn_node_tasks(address1, None).await;
        spawn_node_tasks(address2, Some(address1)).await;
        spawn_node_tasks(address3, Some(address1)).await;

        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address1)
        .await
        .unwrap();

        // eventually value gets into a block and get k1 -> v1 (in all nodes)
        assert_eventually_equals(address1, "k1", "v1").await;
        assert_eventually_equals(address2, "k1", "v1").await;
        assert_eventually_equals(address3, "k1", "v1").await;

        // set k=v2 (another node) -> eventually v2 in 1
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(address2)
        .await
        .unwrap();
        assert_eventually_equals(address1, "k1", "v2").await;
        assert_eventually_equals(address2, "k1", "v2").await;
        assert_eventually_equals(address3, "k1", "v2").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn new_node_catch_up() {
        // start two nodes
        let address1: SocketAddr = "127.0.0.1:6179".parse().unwrap();
        let address2: SocketAddr = "127.0.0.1:6189".parse().unwrap();
        spawn_node_tasks(address1, None).await;
        spawn_node_tasks(address2, Some(address1)).await;

        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address1)
        .await
        .unwrap();

        // eventually value gets into a block and get k1 -> v1 (in all nodes)
        assert_eventually_equals(address1, "k1", "v1").await;
        assert_eventually_equals(address2, "k1", "v1").await;

        // set k=v2 (another node) -> eventually v2 in 1
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(address2)
        .await
        .unwrap();
        assert_eventually_equals(address1, "k1", "v2").await;
        assert_eventually_equals(address2, "k1", "v2").await;

        // start another node, which should eventually catch up with the longest chain from its peers
        let address3: SocketAddr = "127.0.0.1:6199".parse().unwrap();
        spawn_node_tasks(address3, Some(address1)).await;
        assert_eventually_equals(address3, "k1", "v2").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn node_crash_recover() {
        let address1: SocketAddr = "127.0.0.1:6579".parse().unwrap();
        let address2: SocketAddr = "127.0.0.1:6589".parse().unwrap();
        let address3: SocketAddr = "127.0.0.1:6599".parse().unwrap();

        spawn_node_tasks(address1, None).await;
        // keep the handles to abort later
        let (network_handle, node_handle) = spawn_node_tasks(address2, Some(address1)).await;
        spawn_node_tasks(address3, Some(address1)).await;

        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address1)
        .await
        .unwrap();

        // eventually value gets into a block and get k1 -> v1 (in all nodes)
        assert_eventually_equals(address1, "k1", "v1").await;
        assert_eventually_equals(address2, "k1", "v1").await;
        assert_eventually_equals(address3, "k1", "v1").await;

        // abort the 2nd node
        network_handle.abort();
        node_handle.abort();

        // send another transaction
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(address1)
        .await
        .unwrap();

        // the nodes that are still alive eventually update the value
        assert_eventually_equals(address1, "k1", "v2").await;
        assert_eventually_equals(address3, "k1", "v2").await;

        // start the second node again
        spawn_node_tasks(address2, Some(address1)).await;

        // send a new transaction to the fresh right away
        ClientCommand::Set {
            key: "k1".to_string(),
            value: "v3".to_string(),
        }
        .send_to(address2)
        .await
        .unwrap();

        // the agreed ledger should eventually include the last transaction
        assert_eventually_equals(address1, "k1", "v3").await;
        assert_eventually_equals(address2, "k1", "v3").await;
        assert_eventually_equals(address3, "k1", "v3").await;
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
