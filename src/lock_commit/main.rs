use crate::node::{Node, State};
use clap::Parser;
use lib::{command::ClientCommand, network::Receiver};
use log::{info, warn};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::task::JoinHandle;

mod command_ext;
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

    info!(
        "Node socket for client request {}:{}, network request: {}:{}",
        cli.address, cli.client_port, cli.address, cli.network_port
    );

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let network_address = SocketAddr::new(cli.address, cli.client_port);
    let client_address = SocketAddr::new(cli.address, cli.network_port);

    // because the Client application does not work with this (sends a ClientCommand not wrapped in Command())
    // if the CLI has a command, this works as a client
    if let Some(cmd) = cli.command {
        return send_command(client_address, cmd).await;
    }

    let node = Node::new(
        cli.peers,
        &format!(".db_{}", network_address.port()),
        network_address,
        cli.view_change_delta_ms,
    );

    info!(
        "Node: Running on {}. Primary = {}...",
        node.socket_address,
        matches!(node.get_state(), State::Primary)
    );

    let (_, network_handle, _) = spawn_node_tasks(network_address, client_address, node).await;
    network_handle.await.unwrap();
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

    // run a task to manage the blockchain node state, listening for messages from client and network
    let node_handle = tokio::spawn(async move {
        node.run(network_channel_receiver, client_channel_receiver)
            .await;
    });

    (node_handle, network_handle, client_handle)
}

async fn send_command(socket_addr: SocketAddr, command: ClientCommand) {
    // using a reliable sender to get a response back
    match command.send_to(socket_addr).await {
        Ok(Some(value)) => info!("{}", value),
        Ok(None) => info!("null"),
        Err(error) => warn!("ERROR {}", error),
    }
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
        format!(".db_test/{suffix}")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        let network_address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        fs::remove_dir_all(".db_test_primary1").unwrap_or_default();

        let primary = node::Node::new(
            vec![network_address_primary],
            &db_path("primary"),
            network_address_primary,
            None,
        );

        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;

        sleep(Duration::from_millis(10)).await;

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        let _ = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();

        sleep(Duration::from_millis(10)).await;

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test()]
    async fn test_replicated_server() {
        fs::remove_dir_all(".db_test_primary2").unwrap_or_default();
        fs::remove_dir_all(".db_test_backup2").unwrap_or_default();

        let network_address_primary: SocketAddr = "127.0.0.1:6480".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:6481".parse().unwrap();

        let network_address_replica: SocketAddr = "127.0.0.1:6580".parse().unwrap();
        let client_address_replica: SocketAddr = "127.0.0.1:6581".parse().unwrap();

        let backup = node::Node::new(
            vec![network_address_primary, network_address_replica],
            &db_path("backup2"),
            network_address_replica,
            None,
        );

        let primary = node::Node::new(
            vec![network_address_primary, network_address_replica],
            &db_path("primary2"),
            network_address_primary,
            None,
        );

        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;
        spawn_node_tasks(network_address_replica, client_address_replica, backup).await;

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
        let _ = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        // get value on primary
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        sleep(Duration::from_millis(100)).await;

        // get value on replica to make sure it was replicated
        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(client_address_replica)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test()]
    async fn test_view_change() {
        let network_address_primary: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:9100".parse().unwrap();

        let network_address_replica: SocketAddr = "127.0.0.1:9998".parse().unwrap();
        let client_address_replica: SocketAddr = "127.0.0.1:9998".parse().unwrap();

        let backup = Box::new(node::Node::new(
            vec![network_address_primary, network_address_replica],
            &db_path("backup2"),
            network_address_replica,
            None,
        ));

        let primary = Box::new(node::Node::new(
            vec![network_address_primary, network_address_replica],
            &db_path("primary2"),
            network_address_primary,
            None,
        ));

        let backup_raw = &*backup as *const Node;
        let primary_raw = &*primary as *const Node;

        spawn_node_tasks_test(network_address_primary, client_address_primary, primary).await;
        spawn_node_tasks_test(network_address_replica, client_address_replica, backup).await;

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
        network_address: SocketAddr,
        client_address: SocketAddr,
        mut node: Box<Node>,
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
        let node_handle = tokio::spawn(async move {
            (*node)
                .run(network_channel_receiver, client_channel_receiver)
                .await;
        });

        (node_handle, network_handle, client_handle)
    }
}
