use bytes::Bytes;
use clap::Parser;
use lib::network::Receiver;
use lib::network::SimpleSender;
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::node::{Message, Node};

mod node;

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

    simple_logger::SimpleLogger::new().env().init().unwrap();

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

    tokio::spawn(async move {
        let receiver = Receiver::new(address, node);
        receiver.run().await;
    })
    .await
    .unwrap();
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

    #[ctor::ctor]
    fn init() {
        simple_logger::SimpleLogger::new().env().init().unwrap();

        fs::remove_dir_all(db_path("")).unwrap_or_default();
    }

    fn db_path(suffix: &str) -> String {
        format!(".db_test/{}", suffix)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_primary_server() {
        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        tokio::spawn(async move {
            let receiver = Receiver::new(address, node::Node::primary(&db_path("primary1")));
            receiver.run().await;
        });
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
        tokio::spawn(async move {
            let receiver = Receiver::new(address_replica, node::Node::backup(&db_path("backup2")));
            receiver.run().await;
        });
        tokio::spawn(async move {
            let receiver =
                Receiver::new(address_primary, node::Node::primary(&db_path("primary2")));
            receiver.run().await;
        });

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
        let reply = ClientCommand::Get {
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
}
