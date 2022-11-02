use clap::Parser;
use lib::{command::ClientCommand, network::Receiver};
use log::{info, warn};

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use crate::{
    command_ext::{Command, NetworkCommand},
    node::{Node, State},
};

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
    /// if running as a replica, this is the address of the primary
    #[clap(
        long,
        value_parser,
        value_name = "ADDR",
        use_value_delimiter = true,
        value_delimiter = ' '
    )]
    peers: Vec<SocketAddr>,

    #[clap(short, long, value_parser, value_name = "UINT")]
    view_change: bool,

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

    let timer_start = Arc::new(RwLock::new(Instant::now()));

    let network_address = SocketAddr::new(cli.address, cli.client_port);
    let client_address = SocketAddr::new(cli.address, cli.network_port);

    // because the Client application does not work with this (sends a ClientCommand not wrapped in Command())
    // if the CLI has a command, this works as a client
    if let Some(cmd) = cli.command {
        return send_command(client_address, Command::Client(cmd)).await;
    }

    let node = Node::new(
        cli.peers,
        &format!(".db_{}", network_address.port()),
        network_address,
        timer_start.clone(),
    );

    // todo/fixme: this needs to change and use channels
    if cli.view_change {
        tokio::spawn(async move {
            let delta = Duration::from_millis(1000);

            loop {
                if timer_start.read().unwrap().elapsed() > delta * 8 {
                    *timer_start.write().unwrap() = Instant::now();
                    info!("{}: timer expired!", network_address);
                    let blame_message = Command::Network(NetworkCommand::Blame {
                        socket_addr: network_address,
                        view: 0,
                        timer_expired: true,
                    });
                    let _ = blame_message.send_to(network_address).await;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
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
        let network_address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let client_address_primary: SocketAddr = "127.0.0.1:6381".parse().unwrap();

        let timer_start = Arc::new(RwLock::new(Instant::now()));

        fs::remove_dir_all(".db_test_primary1").unwrap_or_default();

        let primary = node::Node::new(
            vec![network_address_primary],
            &db_path("primary"),
            network_address_primary,
            timer_start,
        );

        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;

        sleep(Duration::from_millis(10)).await;

        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        let _ = Command::Client(ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        })
        .send_to(client_address_primary)
        .await
        .unwrap();

        sleep(Duration::from_millis(10)).await;

        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test()]
    async fn test_replicated_server() {
        let timer_start = Arc::new(RwLock::new(Instant::now()));
        let timer_start_secondary = Arc::new(RwLock::new(Instant::now()));
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
            timer_start,
        );

        let primary = node::Node::new(
            vec![network_address_primary, network_address_replica],
            &db_path("primary2"),
            network_address_primary,
            timer_start_secondary,
        );

        spawn_node_tasks(network_address_primary, client_address_primary, primary).await;
        spawn_node_tasks(network_address_replica, client_address_replica, backup).await;

        sleep(Duration::from_millis(10)).await;

        // get null value
        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set a value on primary
        let _ = Command::Client(ClientCommand::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        })
        .send_to(client_address_primary)
        .await
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        // get value on primary
        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(client_address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        sleep(Duration::from_millis(100)).await;

        // get value on replica to make sure it was replicated
        let reply = Command::Client(ClientCommand::Get {
            key: "k1".to_string(),
        })
        .send_to(client_address_replica)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }
}
