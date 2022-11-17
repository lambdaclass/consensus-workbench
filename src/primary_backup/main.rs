use crate::node::Node;
use clap::Parser;
use lib::network::Receiver;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::task::JoinHandle;

mod node;

pub const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The client port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    client_port: u16,
    /// The network port where other nodes sends msg.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6200)]
    network_port: u16,
    /// Node Address
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

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let network_address = SocketAddr::new(cli.address, cli.network_port);
    let client_address = SocketAddr::new(cli.address, cli.client_port);

    let node = if let Some(primary_address) = cli.primary {
        info!(
            "Replica: Running as replica on {}, waiting for commands from the primary node...",
            network_address
        );

        let db_name = &db_name(&cli, &format!("replic-{}", cli.network_port)[..]);
        Node::backup(db_name, network_address, primary_address)
    } else {
        info!("Primary: Running as primary on {}.", network_address);
        let db_name = db_name(&cli, "primary");
        Node::primary(&db_name, network_address, network_address)
    };

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
    use crate::node::{Message, State};
    use anyhow::{anyhow, Result};
    use bytes::Bytes;
    use lib::{command::ClientCommand, network::ReliableSender};
    use std::fs;
    use tokio::time::Duration;
    use tokio_retry::{strategy::FixedInterval, Retry};

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

    pub const KEY: &str = "KEY";
    pub const VALUE: &str = "VALUE";
    pub const BASE_PORT: u16 = 6780;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        let (network_address, client_address) = get_address_pair(BASE_PORT);
        run_node(
            db_path("primary1"),
            network_address,
            client_address,
            network_address,
            State::Primary,
        )
        .await;

        assert_get_msg(KEY, VALUE, client_address, true).await;
        assert_set_msg(KEY, VALUE, client_address).await;
        assert_get_msg(KEY, VALUE, client_address, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replicated_server() {
        let (network_address_primary, client_address_primary) = get_address_pair(BASE_PORT + 2);
        let (network_address_replica, client_address_replica) = get_address_pair(BASE_PORT + 4);

        run_node(
            db_path("db_test_primary2"),
            network_address_primary,
            client_address_primary,
            network_address_primary,
            State::Primary,
        )
        .await;
        run_node(
            db_path("db_test_backup2"),
            network_address_replica,
            client_address_replica,
            network_address_primary,
            State::Backup,
        )
        .await;

        // get value on primary should be None
        assert_get_msg(KEY, VALUE, client_address_primary, true).await;
        // set value on primary
        assert_set_msg(KEY, VALUE, client_address_primary).await;
        // get value on primary
        assert_get_msg(KEY, VALUE, client_address_primary, false).await;
        // get value on replica to make sure it was replicated
        assert_get_msg(KEY, VALUE, client_address_replica, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_backup_change_to_primary() {
        let (network_address_primary, client_address_primary) = get_address_pair(BASE_PORT + 6);
        let (network_address_replica, client_address_replica) = get_address_pair(BASE_PORT + 8);

        let (node_handle, _, _) = run_node(
            db_path("db_test_primary3"),
            network_address_primary,
            client_address_primary,
            network_address_primary,
            State::Primary,
        )
        .await;
        run_node(
            db_path("db_test_backup3"),
            network_address_replica,
            client_address_replica,
            network_address_primary,
            State::Backup,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(1000)).await;

        //kill primary
        node_handle.abort();

        //check that the fromer backup is now primary
        assert_eventually_equals(network_address_replica).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_backup_change_to_primary_backup_replicate_to_replicas() {
        let (network_address_primary, client_address_primary) = get_address_pair(BASE_PORT + 10);
        let (network_address_replica, client_address_replica) = get_address_pair(BASE_PORT + 12);
        let (network_address_second_replica, client_address_second_replica) =
            get_address_pair(BASE_PORT + 14);

        let (node_handle, _, _) = run_node(
            db_path("db_test_primary4"),
            network_address_primary,
            client_address_primary,
            network_address_primary,
            State::Primary,
        )
        .await;
        run_node(
            db_path("db_test_backup4"),
            network_address_replica,
            client_address_replica,
            network_address_primary,
            State::Backup,
        )
        .await;
        run_node(
            db_path("db_test_backup5"),
            network_address_second_replica,
            client_address_second_replica,
            network_address_primary,
            State::Backup,
        )
        .await;

        tokio::time::sleep(Duration::from_millis(1000)).await;

        //kill primary
        node_handle.abort();

        //wait to replica to take the lead
        tokio::time::sleep(Duration::from_millis(1000 * 9)).await;

        //check that the fromer backup is now primary
        assert_eventually_equals(network_address_replica).await;

        // set a value on new primary
        assert_set_msg(KEY, VALUE, client_address_replica).await;
        // get value on new primary
        assert_get_msg(KEY, VALUE, client_address_replica, false).await;
        // get value on the second replica to make sure it was replicated
        assert_get_msg(KEY, VALUE, client_address_second_replica, false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_set_command_to_backup_is_forwarded_to_primary() {
        let (network_address_primary, client_address_primary) = get_address_pair(BASE_PORT + 16);
        let (network_address_replica, client_address_replica) = get_address_pair(BASE_PORT + 18);
        let (network_address_second_replica, client_address_second_replica) =
            get_address_pair(BASE_PORT + 20);

        run_node(
            db_path("db_test_primary5"),
            network_address_primary,
            client_address_primary,
            network_address_primary,
            State::Primary,
        )
        .await;
        run_node(
            db_path("db_test_backup6"),
            network_address_replica,
            client_address_replica,
            network_address_primary,
            State::Backup,
        )
        .await;
        run_node(
            db_path("db_test_backup7"),
            network_address_second_replica,
            client_address_second_replica,
            network_address_primary,
            State::Backup,
        )
        .await;

        // set a value on replica, should be forwarded to primary
        assert_set_msg(KEY, VALUE, client_address_replica).await;
        // get value on primary
        assert_get_msg(KEY, VALUE, client_address_primary, false).await;
        // get value on replica to make sure it was replicated
        assert_get_msg(KEY, VALUE, client_address_replica, false).await;
        // get value on second replica to make sure it was replicated
        assert_get_msg(KEY, VALUE, client_address_second_replica, false).await;
    }

    fn db_path(suffix: &str) -> String {
        format!(".db_test/{suffix}")
    }

    impl Message {
        pub async fn send_to(self, address: SocketAddr) -> Result<String> {
            let mut sender = ReliableSender::new();

            let message: Bytes = bincode::serialize(&(self))?.into();
            let reply_handler = sender.send(address, message).await;

            let response = reply_handler.await?;
            bincode::deserialize(&response).map_err(|e| anyhow!(e))
        }
    }

    async fn assert_get_msg(key: &str, value: &str, address: SocketAddr, none: bool) {
        let command = ClientCommand::Get {
            key: key.to_string(),
        };
        match (command.send_to(address).await, none) {
            (Ok(Some(val)), false) => assert_eq!(value, val),
            (Ok(val), true) => assert!(val.is_none()),
            _ => panic!("Error"),
        }
    }

    async fn assert_set_msg(key: &str, value: &str, address: SocketAddr) {
        let command = ClientCommand::Set {
            key: key.to_string(),
            value: value.to_string(),
        };
        if let Ok(Some(msg_result)) = command.send_to(address).await {
            assert_eq!(value, msg_result);
        }
    }

    async fn run_node(
        db_path: String,
        network_address: SocketAddr,
        client_address: SocketAddr,
        primary: SocketAddr,
        state: State,
    ) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {
        let node = match state {
            State::Primary => node::Node::primary(&db_path, network_address, primary),
            State::Backup => node::Node::backup(&db_path, network_address, primary),
        };

        spawn_node_tasks(network_address, client_address, node).await
    }

    fn get_address_pair(port: u16) -> (SocketAddr, SocketAddr) {
        (
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port + 1),
        )
    }

    /// Send Get commands to the given address with delayed retries to give it time for a transaction
    /// to propagate. Fails if the expected value isn't read after 20 seconds.
    async fn assert_eventually_equals(address: SocketAddr) {
        let retries = FixedInterval::from_millis(100).take(200);
        let reply = Retry::spawn(retries, || async {
            let reply = Message::PrimaryAddress.send_to(address).await.unwrap();
            if reply == address.to_string() {
                Ok(())
            } else {
                Err(())
            }
        })
        .await;
        assert!(reply.is_ok());
    }
}
