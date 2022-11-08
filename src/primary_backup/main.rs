use bytes::Bytes;
use clap::Parser;
use lib::network::Receiver;
use lib::network::SimpleSender;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::task::JoinHandle;

use crate::node::{Message, Node};

mod node;
pub const CHANNEL_CAPACITY: usize = 1_000;

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
    primary: Option<SocketAddr>,
    /// Node name, useful to identify the node and the store.
    /// (eg. when running several nodes in same machine)
    #[clap(short, long)]
    name: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!(
        "Node socket for client request {}:{}, network request: {}:{}",
        cli.address, cli.client_port, cli.address, cli.network_port
    );

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let network_address = SocketAddr::new(cli.address, cli.client_port);
    let client_address = SocketAddr::new(cli.address, cli.network_port);

    let node = if let Some(primary_address) = cli.primary {
        info!(
            "Replica: Running as replica on {}, waiting for commands from the primary node...",
            network_address
        );

        info!("Subscribing to primary: {}.", primary_address);

        // TODO: this "Subscribe" message is sent here for testing purposes.
        //       But it shouldn't be here. We should have an initialization loop
        //       inside the actual replica node to handle the response, deal with
        //       errors, and eventually reconnect to a new primary.
        let mut sender = SimpleSender::new();
        let subscribe_message: Bytes = bincode::serialize(&Message::Subscribe {
            address: network_address,
        })
        .unwrap()
        .into();
        sender.send(primary_address, subscribe_message).await;

        Node::backup(
            &db_name(&cli, &format!("replic-{}", cli.network_port)[..]),
            network_address,
            Vec::new()
        )
    } else {
        info!("Primary: Running as primary on {}.", network_address);

        let db_name = db_name(&cli, "primary");
        Node::primary(&db_name, network_address, Vec::new())
    };

    let (_, network_handle, _) = spawn_node_tasks(network_address, client_address, node).await;
    network_handle.await.unwrap();

    println!("hola");
}

async fn spawn_node_tasks(
    network_address: SocketAddr,
    client_address: SocketAddr,
    mut node: Node,
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

    let node_handle = tokio::spawn(async move {
        node.run(network_channel_receiver, client_channel_receiver)
            .await;
    });

    (node_handle, network_handle, client_handle)
}

fn db_name(cli: &Cli, default: &str) -> String {
    let default = &default.to_string();
    let name = cli.name.as_ref().unwrap_or(default);
    format!(".db_{name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::{command::ClientCommand, network::ReliableSender};
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

        fs::remove_dir_all(db_path("")).unwrap_or_default();
    }

    fn db_path(suffix: &str) -> String {
        format!(".db_test/{suffix}")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        let network_address: SocketAddr = "127.0.0.1:6680".parse().unwrap();
        let client_address: SocketAddr = "127.0.0.1:6781".parse().unwrap();
        let mut peers = Vec::new();
        peers.push(network_address.clone());

        let node = node::Node::primary(&db_path("primary1"), network_address, peers);
        spawn_node_tasks(network_address, client_address, node).await;

        sleep(Duration::from_millis(2000)).await;

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address)
        .await
        .unwrap();
        assert!(reply.is_none());

        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address)
        .await
        .unwrap();

        sleep(Duration::from_millis(2000)).await;
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address)
        .await
        .unwrap();

        sleep(Duration::from_millis(20)).await;
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replicated_server() {
        let network_address_primary: SocketAddr = "127.0.0.1:6882".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:6883".parse().unwrap();

        let network_address_replica: SocketAddr = "127.0.0.1:6984".parse().unwrap();
        let client_address_replica: SocketAddr = "127.0.0.1:6985".parse().unwrap();

        let mut peers = Vec::new();
        peers.push(network_address_primary.clone());
        peers.push(network_address_replica.clone());

        let primary = node::Node::primary(
            &db_path("db_test_primary2"),
            network_address_primary,
            peers.clone(),
        );
        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;

        let backup =
            node::Node::backup(&db_path("db_test_backup2"), network_address_replica, peers);
        spawn_node_tasks(network_address_replica, client_address_replica, backup).await;

        // TODO: this "Subscribe" message is sent here for testing purposes.
        //       But it shouldn't be here. We should have an initialization loop
        //       inside the actual replica node to handle the response, deal with
        //       errors, and eventually reconnect to a new primary.
        let mut sender = SimpleSender::new();
        let subscribe_message: Bytes = bincode::serialize(&Message::Subscribe {
            address: network_address_replica,
        })
        .unwrap()
        .into();
        sender
            .send(network_address_primary, subscribe_message)
            .await;

        sleep(Duration::from_millis(10)).await;

        // get null value
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set a value on primary
        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on primary
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on replica to make sure it was replicated
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_replica)
        .await
        .unwrap();

        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // should fail since replica should not respond to set commands
        let reply = ClientCommand::Set {
            key: "k3".to_string(),
            value: "_".to_string(),
        }
        .send_to(client_address_replica)
        .await;
        assert!(reply.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_backup_change_to_primary() {
        let network_address_primary: SocketAddr = "127.0.0.1:6890".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:6891".parse().unwrap();

        let network_address_replica: SocketAddr = "127.0.0.1:6992".parse().unwrap();
        let client_address_replica: SocketAddr = "127.0.0.1:6993".parse().unwrap();

        let mut primary_peers = Vec::new();
        primary_peers.push(network_address_primary.clone());
        
        let mut backup_peers = Vec::new();
        backup_peers.push(network_address_primary.clone());
        backup_peers.push(network_address_replica.clone());


        let primary = node::Node::primary(
            &db_path("db_test_primary3"),
            network_address_primary,
            primary_peers,
        );
        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;

        let backup =
            node::Node::backup(&db_path("db_test_backup3"), network_address_replica, backup_peers);
        spawn_node_tasks(network_address_replica, client_address_replica, backup).await;

        tokio::time::sleep(Duration::from_millis(1000 * 9)).await;

        let mut sender = ReliableSender::new();

        let message: Bytes = bincode::serialize(&(Message::PrimaryAddress)).unwrap().into();
        let reply_handler = sender.send(network_address_replica, message).await;

        let response = reply_handler.await.unwrap();
        let response: String = bincode::deserialize(&response).unwrap();

        assert_eq!(response, network_address_replica.to_string());
    }


    #[tokio::test(flavor = "multi_thread")]
    async fn test_backup_change_to_primary_backup_replicate_to_replicas() {
        let network_address_primary: SocketAddr = "127.0.0.1:6894".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:6895".parse().unwrap();

        let network_address_replica: SocketAddr = "127.0.0.1:6996".parse().unwrap();
        let client_address_replica: SocketAddr = "127.0.0.1:6997".parse().unwrap();

        let network_address_second_replica: SocketAddr = "127.0.0.1:6998".parse().unwrap();
        let client_address_second_replica: SocketAddr = "127.0.0.1:6999".parse().unwrap();

        let mut primary_peers = Vec::new();
        primary_peers.push(network_address_primary.clone());

        let mut backup_peers = Vec::new();
        backup_peers.push(network_address_primary.clone());
        backup_peers.push(network_address_replica.clone());
        backup_peers.push(network_address_second_replica.clone());

        let mut second_backup_peers = Vec::new();
        second_backup_peers.push(network_address_primary.clone());
        second_backup_peers.push(network_address_replica.clone());
        second_backup_peers.push(network_address_second_replica.clone());

        let primary = node::Node::primary(
            &db_path("db_test_primary4"),
            network_address_primary,
            primary_peers,
        );
        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;

        let backup =
            node::Node::backup(&db_path("db_test_backup4"), network_address_replica, backup_peers);
        spawn_node_tasks(network_address_replica, client_address_replica, backup).await;

        let second_backup =
        node::Node::backup(&db_path("db_test_backup5"), network_address_second_replica, second_backup_peers);
        spawn_node_tasks(network_address_second_replica, client_address_second_replica, second_backup).await;

        tokio::time::sleep(Duration::from_millis(1000 * 9)).await;

        let mut sender = ReliableSender::new();

        let message: Bytes = bincode::serialize(&(Message::PrimaryAddress)).unwrap().into();
        let reply_handler = sender.send(network_address_replica, message).await;

        let response = reply_handler.await.unwrap();
        let response: String = bincode::deserialize(&response).unwrap();

        assert_eq!(response, network_address_replica.to_string());

        // set a value on new primary
        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address_replica)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on the second replica to make sure it was replicated
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_second_replica)
        .await
        .unwrap();

        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());


    }


}
