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

// TODO should this be defined here?
#[derive(Clone)]
struct NodeReceiverHandler {
    network_sender: Sender<(node::Message, oneshot::Sender<Result<Option<String>>>)>,
}

#[async_trait]
impl MessageHandler for NodeReceiverHandler {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = node::Message::deserialize(bytes)?;

        let (reply_sender, reply_receiver) = oneshot::channel();
        self.network_sender.send((request, reply_sender)).await?;
        let reply = reply_receiver.await?.map_err(|e| e.to_string());
        let reply = bincode::serialize(&reply)?;
        Ok(writer.send(reply.into()).await?)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    let (network_sender, network_receiver) = channel(CHANNEL_CAPACITY);

    let mut node = Node::new(address, cli.seed);
    tokio::spawn(async move {
        node.run(network_receiver).await;
    });

    NetworkReceiver::spawn(address, NodeReceiverHandler { network_sender })
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::Command;
    use tokio_retry::strategy::FixedInterval;
    use tokio_retry::Retry;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn single_node() {
        // FIXME too much duplication (also with main) try to extract to a function
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let (network_sender, network_receiver) = channel(CHANNEL_CAPACITY);
        NetworkReceiver::spawn(address, NodeReceiverHandler { network_sender });

        tokio::spawn(async move {
            let mut node = Node::new(address, None);
            node.run(network_receiver).await;
        });

        // get k1 -> null
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set k1
        let reply = Command::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // eventually value gets into a block and get k1 -> v1
        let retries = FixedInterval::from_millis(100).take(100);
        let reply = Retry::spawn(retries, || async {
            let reply = Command::Get {
                key: "k1".to_string(),
            }
            .send_to(address)
            .await
            .unwrap();
            reply.ok_or(())
        })
        .await;

        assert!(reply.is_ok());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test]
    async fn multiple_nodes() {
        // spawn node 1
        // spawn node 2
        // spawn node 3
        // get k -> null
        // set k=v -> eventually v in 1
        // eventually v in 2
        // eventually v in 3

        // set k=v2 (another node) -> eventually v2 in 1
        // eventually v2 in 2
        // eventually v2 in 3
    }

    #[tokio::test]
    async fn new_node_catch_up() {}

    #[tokio::test]
    async fn node_crash_recover() {}
}
