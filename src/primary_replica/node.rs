use clap::Parser;
use lib::{
    network::{Receiver, SimpleSender},
    store::Store,
};
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

mod primary;
mod replica;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// Run as replica
    #[arg(long, value_parser)]
    replica: bool,
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
    /// Where to replicate to, if not running as a replica.
    #[clap(short, long, value_parser, value_name = "ADDR")]
    replicate_to: Option<SocketAddr>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new()
        .env()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    let address = SocketAddr::new(cli.address, cli.port);

    // check if node should start as replica
    match cli.replica {
        false => {
            let store = Store::new(".db_primary").unwrap();
            info!("Primary: Running as primary on {}.", address);

            // if not a replica, see if there is a parameter of a socket to replicate to
            let server = primary::SingleNodeServer {
                store,
                // TODO will eventually handle multiple peers, but for now we keep passing the single replica
                peers: vec![cli.replicate_to.unwrap()],
                sender: SimpleSender::new(),
            };
            cli.replicate_to
                .is_some()
                .then(|| info!("Primary: Replicating to {}.", cli.replicate_to.unwrap()));

            Receiver::spawn(address, server).await.unwrap();
        }
        true => {
            let store = Store::new(".db_replica").unwrap();
            let node = replica::ReplicaNodeServer { store };
            info!(
                "Replica: Running as replica on {}, waiting for commands from the primary node...",
                address
            );
            Receiver::spawn(address, node).await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lib::command::Command;
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
    }

    fn create_store(name_suffix: &str) -> Store {
        let db_path = format!(".db_test_{}", name_suffix);
        let db_path = db_path.as_ref();

        fs::remove_dir_all(db_path).unwrap_or_default();
        Store::new(db_path).unwrap()
    }

    #[tokio::test]
    async fn test_only_primary_server() {
        let store = create_store("primary1");

        let address: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let simple_sender = SimpleSender::new();
        Receiver::spawn(
            address,
            primary::SingleNodeServer {
                store,
                peers: None,
                sender: simple_sender,
            },
        );
        sleep(Duration::from_millis(10)).await;

        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_none());

        let reply = Command::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());
    }

    #[tokio::test]
    async fn test_replicated_server() {
        let address_primary: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let address_replica: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let simple_sender = SimpleSender::new();
        Receiver::spawn(
            address_replica,
            replica::ReplicaNodeServer {
                store: create_store("replica"),
            },
        );
        Receiver::spawn(
            address_primary,
            primary::SingleNodeServer {
                store: create_store("primary"),
                peers: Some(address_replica),
                sender: simple_sender,
            },
        );
        sleep(Duration::from_millis(10)).await;

        // get null value
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_none());

        // set a value on primary
        let reply = Command::Set {
            key: "k1".to_string(),
            value: "v1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on primary
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address_primary)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // get value on replica to make sure it was replicated
        let reply = Command::Get {
            key: "k1".to_string(),
        }
        .send_to(address_replica)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v1".to_string(), reply.unwrap());

        // should fail since replica should not respond to set commands
        let reply = Command::Set {
            key: "k3".to_string(),
            value: "_".to_string(),
        }
        .send_to(address_replica)
        .await;
        assert!(reply.is_err());
    }
}
