/// This modules implements the most basic form of distributed system, a single node server that handles
/// client requests to a key/value store. There is no replication and this no fault-tolerance.
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use clap::Parser;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use lib::{
    command::ClientCommand,
    network::{MessageHandler, Receiver, Writer},
    store::Store,
};
use log::info;
use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, thread::JoinHandle};

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
}

#[derive(Clone)]
/// A message handler that just forwards key/value store requests from clients to an internal rocksdb store.
struct Node {
    pub store: Store,
}

#[async_trait]
impl MessageHandler for Node {
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request: ClientCommand = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let result = match request {
            ClientCommand::Set { key, value } => {
                self.store
                    .write(key.clone().into(), value.clone().into())
                    .await
            }
            ClientCommand::Get { key } => self.store.read(key.clone().into()).await,
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
    tokio::spawn(async move {
        let receiver = Receiver::new(address, Node { store });
        receiver.run().await;
    });
}

async fn spawn_node_tasks(address: SocketAddr, seed: Option<SocketAddr>) -> (JoinHandle<()>, JoinHandle<()>) {
    let (network_sender, network_receiver) = channel(CHANNEL_CAPACITY);

    let store = Store::new(".db_single_node").unwrap();

    let newtor_handle = tokio::spawn( async move {
        let mut node = Node::new();
        node.run(network_receiver);
    });

    let node_handle = tokio::spawn(async move {
        let receiver = NetworkReceiver::new(address, NodeReceiverHandler { network_sender });
        receiver.run().await;
    });

    (newtor_handle, newtor_handle)
}


impl Node {
    pub fn new() -> Self {
        Self {
            store: Store::new(".db_single_node").unwrap(),
        }
    }

    pub async fn run(&mut self, mut network_receiver: Receiver<(ClientCommand, oneshot::Sender<Result<Option<Vec<u8>>>>)>) -> () {
        while let Some((message, reply_sender)) = network_receiver.recv().await {
            let result = match message {
                ClientCommand::Set { key, value } => {
                        self.store
                        .write(key.clone().into(), value.clone().into())
                        .await
                }
                ClientCommand::Get { key } => self.store.read(key.clone().into()).await,
            };

            reply_sender.send(result);
        }
    }

}


#[derive(Clone)]
struct NodeReceiverHandler {
    /// Used to forward incoming TCP messages to the node
    network_sender: Sender<(ClientCommand, oneshot::Sender<Result<Option<String>>>)>,
}


#[async_trait]
impl MessageHandler for NodeReceiverHandler {
    /// When a TCP message is received, interpret it as a node::Message and forward it to the node task.
    /// Send the node's response back through the TCP connection.
    async fn dispatch(&mut self, writer: &mut Writer, bytes: Bytes) -> Result<()> {
        let request: ClientCommand = bincode::deserialize(&bytes)?;
        info!("Received request {:?}", request);

        let (reply_sender, reply_receiver) = oneshot::channel();
        self.network_sender.send((request, reply_sender)).await?;
        let reply = reply_receiver.await?.map_err(|e| e.to_string());
        let reply = bincode::serialize(&reply)?;
        Ok(writer.send(reply.into()).await?)
    }
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
