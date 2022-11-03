use clap::Parser;
use lib::{command::ClientCommand, network::Receiver as NetworkReceiver};
use log::{info, warn};
use tokio::sync::mpsc;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::{
    command_ext::Command,
    node::{Node, NodeReceiverHandler, State},
};

use tokio::task::JoinHandle;

mod command_ext;
mod node;

pub const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6109)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// If running as a replica, this is the address of the primary
    #[clap(
        long,
        value_parser,
        value_name = "ADDR",
        use_value_delimiter = true,
        value_delimiter = ' '
    )]
    peers: Vec<SocketAddr>,

    /// If view-change mechanism is enabled, you can set the delta time (in ms)
    #[clap(short, long, value_parser, value_name = "UINT")]
    view_change_delta_ms: Option<u16>,

    /// The key/value store command to execute.
    #[clap(subcommand)]
    command: Option<ClientCommand>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    // because the Client application does not work with this (sends a ClientCommand not wrapped in Command())
    // if the CLI has a command, this works as a client
    if let Some(cmd) = cli.command {
        return send_command(address, Command::Client(cmd)).await;
    }

    let node = Node::new(
        cli.peers,
        &format!(".db_{}", address.port()),
        address,
        cli.view_change_delta_ms,
    );

    info!(
        "Node: Running on {}. Primary = {}...",
        node.socket_address,
        matches!(node.get_state(), State::Primary)
    );

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
async fn send_command(socket_addr: SocketAddr, command: Command) {
    // using a reliable sender to get a response back
    match command.send_to(socket_addr).await {
        Ok(Some(value)) => info!("{}", value),
        Ok(None) => info!("null"),
        Err(error) => warn!("ERROR {}", error),
    }
}

#[cfg(test)]
mod tests {
    use crate::command_ext::Command;

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
        format!(".db_test/{suffix}")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        fs::remove_dir_all(".db_test_primary1").unwrap_or_default();
        let node = node::Node::new(vec![address], &db_path("primary1"), address, None);

        spawn_node_tasks(address, node).await;

        sleep(Duration::from_millis(10)).await;

        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        let _ = Command::Client(ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        })
        .send_to(address)
        .await
        .unwrap();

        sleep(Duration::from_millis(10)).await;

        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test()]
    async fn test_replicated_server() {
        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        let backup = node::Node::new(
            vec![address_primary, address_replica],
            &db_path("backup2"),
            address_replica,
            None,
        );

        let primary = node::Node::new(
            vec![address_primary, address_replica],
            &db_path("primary2"),
            address_primary,
            None,
        );

        spawn_node_tasks(address_primary, primary).await;
        spawn_node_tasks(address_replica, backup).await;

        sleep(Duration::from_millis(10)).await;

        // get null value
        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set a value on primary
        let _ = Command::Client(ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        })
        .send_to(address_primary)
        .await
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        // get value on primary
        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        sleep(Duration::from_millis(100)).await;

        // get value on replica to make sure it was replicated
        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(address_replica)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test()]
    async fn test_view_change() {
        let address_primary: SocketAddr = "127.0.0.1:6280".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6281".parse().unwrap();

        let backup = Box::new(node::Node::new(
            vec![address_primary, address_replica],
            &db_path("backup_vc"),
            address_replica,
            Some(100),
        ));

        let primary = Box::new(node::Node::new(
            vec![address_primary, address_replica],
            &db_path("primary_vc"),
            address_primary,
            Some(100),
        ));

        let backup_raw = &*backup as *const Node;
        let primary_raw = &*primary as *const Node;

        spawn_node_tasks_test(address_primary, primary).await;
        spawn_node_tasks_test(address_replica, backup).await;

        sleep(Duration::from_millis(1500)).await;

        unsafe {
            // because ownership moves to spawn_node_tasks_test(), we have to deref the raw pointers
            // which will have the same memory location because they were boxed
            assert!((*backup_raw).current_view > 0);
            assert!((*primary_raw).current_view > 0);
        }
    }

    // in order for the `move` not to change Node's memory location, this function takes a Box<Node> instead of a <Node>
    async fn spawn_node_tasks_test(
        address: SocketAddr,
        mut node: Box<Node>,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        let (network_sender, network_receiver) = mpsc::channel(CHANNEL_CAPACITY);

        let network_handle = tokio::spawn(async move {
            (*node).run(network_receiver).await;
        });

        let node_handle = tokio::spawn(async move {
            let receiver = NetworkReceiver::new(address, NodeReceiverHandler { network_sender });
            receiver.run().await;
        });

        (network_handle, node_handle)
    }
}
