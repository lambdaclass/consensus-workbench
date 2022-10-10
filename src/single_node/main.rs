/// This modules implements the most basic form of distributed system, a single node server that handles
/// client requests to a key/value store. There is no replication and this no fault-tolerance.
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use futures::sink::SinkExt as _;
use lib::{
    network::{MessageHandler, Receiver, Writer},
    store::Store,
};
use log::info;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use lib::command::ClientCommand;

#[derive(Parser)]
#[clap(author, version, about)]
struct Cli {
    /// The network port of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = 6100)]
    port: u16,
    /// The network address of the node where to send txs.
    #[clap(short, long, value_parser, value_name = "UINT", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    address: IpAddr,
}

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
struct Node {
    pub store: Store,
}

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let result = match request {
            ClientCommand::Set { key, value } => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
            }
            ClientCommand::Get { key } => self.store.read(key.clone().into()).await,
            _ => Err(anyhow!("Unhandled command")),
        };

        // convert the error into something serializable
        let result = result.map_err(|e| e.to_string());

        info!("Sending response {:?}", result);
        let reply = bincode::serialize(&result)?;
        Ok(writer.send(reply.into()).await?)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    info!("Node socket: {}:{}", cli.address, cli.port);

    simple_logger::SimpleLogger::new().env().init().unwrap();

    let address = SocketAddr::new(cli.address, cli.port);
    let store = Store::new(".db_single_node").unwrap();
    tokio::spawn(async move {
        let receiver = Receiver::new(address, Node { store });
        receiver.run().await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_server() {
        let db_path = ".db_test";
        fs::remove_dir_all(db_path).unwrap_or_default();
        let store = Store::new(db_path).unwrap();

        simple_logger::SimpleLogger::new().env().init().unwrap();

        let address: SocketAddr = "127.0.0.1:6182".parse().unwrap();
        tokio::spawn(async move {
            let receiver = Receiver::new(address, Node { store });
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

        let reply = ClientCommand::Set {
            key: "k1".to_string(),
            value: "v2".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v2".to_string(), reply.unwrap());

        let reply = ClientCommand::Get {
            key: "k1".to_string(),
        }
        .send_to(address)
        .await
        .unwrap();
        assert!(reply.is_some());
        assert_eq!("v2".to_string(), reply.unwrap());
    }
}
