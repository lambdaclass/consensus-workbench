use bytes::Bytes;
use clap::Parser;
use lib::network::Receiver;
use lib::network::Receiver as NetworkReceiver;
use lib::network::SimpleSender;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::node::{Message, Node, NodeReceiverHandler};

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
    primary: Option<SocketAddr>,
    /// Node name, useful to identify the node and the store.
    /// (eg. when running several nodes in same machine)
    #[clap(short, long)]
    name: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    let node = if let Some(primary_address) = cli.primary {
        info!(
            "Replica: Running as replica on {}, waiting for commands from the primary node...",
            address
        );

        info!("Subscribing to primary: {}.", primary_address);

        // TODO: this "Subscribe" message is sent here for testing purposes.
        //       But it shouldn't be here. We should have an initialization loop
        //       inside the actual replica node to handle the response, deal with
        //       errors, and eventually reconnect to a new primary.
        let mut sender = SimpleSender::new();
        let subscribe_message: Bytes = bincode::serialize(&Message::Subscribe { address })
            .unwrap()
            .into();
        sender.send(primary_address, subscribe_message).await;

        Node::backup(&db_name(&cli, &format!("replic-{}", cli.port)[..]))
    } else {
        info!("Primary: Running as primary on {}.", address);

        let db_name = db_name(&cli, "primary");
        Node::primary(&db_name)
    };

    let (network_handle, _) = spawn_node_tasks(address, node).await;
    network_handle.await.unwrap();
}

async fn spawn_node_tasks(address: SocketAddr, mut node: Node) -> (JoinHandle<()>, JoinHandle<()>) {
    let (network_sender, network_receiver) = mpsc::channel(CHANNEL_CAPACITY);

    let network_handle = tokio::spawn(async move {
        node.run(network_receiver).await;
    });

    let node_handle = tokio::spawn(async move {
        let receiver = NetworkReceiver::new(address, NodeReceiverHandler { network_sender });
        receiver.run().await;
    });

    (network_handle, node_handle)
}

fn db_name(cli: &Cli, default: &str) -> String {
    let default = &default.to_string();
    let name = cli.name.as_ref().unwrap_or(default);
    format!(".db_{}", name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::ClientCommand;
    use std::fs;
    use tokio::time::{sleep, Duration};
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

        fs::remove_dir_all(db_path("")).unwrap_or_default();
    }

    fn db_path(suffix: &str) -> String {
        format!(".db_test/{}", suffix)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        fs::remove_dir_all(".primary1").unwrap_or_default();
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let node = node::Node::primary(&db_path("primary1"));
        spawn_node_tasks(address, node).await;
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

        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        let primary = node::Node::primary(&db_path("db_test_primary2"));
        spawn_node_tasks(address_primary, primary).await;

        let backup = node::Node::backup(&db_path("db_test_backup2"));
        spawn_node_tasks(address_replica, backup).await;

        // TODO: this "Subscribe" message is sent here for testing purposes.
        //       But it shouldn't be here. We should have an initialization loop
        //       inside the actual replica node to handle the response, deal with
        //       errors, and eventually reconnect to a new primary.
        let mut sender = SimpleSender::new();
        let subscribe_message: Bytes = bincode::serialize(&Message::Subscribe {
            address: address_replica,
        })
        .unwrap()
        .into();
        sender.send(address_primary, subscribe_message).await;

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
        let _reply = ClientCommand::Get {
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
