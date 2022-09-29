// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// We keep alive one TCP connection per peer, each connection is handled by a separate task (called `Connection`).
/// We communicate with our 'connections' through a dedicated channel kept by the HashMap called `connections`.
pub struct SimpleSender {
    /// A map holding the channels to our connections.
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
}

impl std::default::Default for SimpleSender {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            rng: SmallRng::from_entropy(),
        }
    }

    /// Helper function to spawn a new connection.
    fn spawn_connection(address: SocketAddr) -> Sender<Bytes> {
        let (tx, rx) = channel(1_000);
        Connection::spawn(address, rx);
        tx
    }

    /// Try (best-effort) to send a message to a specific address.
    /// This is useful to answer sync requests.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) {
        // Try to re-use an existing connection if possible.
        if let Some(tx) = self.connections.get(&address) {
            if tx.send(data.clone()).await.is_ok() {
                return;
            }
        }

        // Otherwise make a new connection.
        let tx = Self::spawn_connection(address);
        if tx.send(data).await.is_ok() {
            self.connections.insert(address, tx);
        }
    }

    /// Try (best-effort) to broadcast the message to all specified addresses.
    pub async fn broadcast(&mut self, addresses: Vec<SocketAddr>, data: Bytes) {
        for address in addresses {
            self.send(address, data.clone()).await;
        }
    }

    /// Pick a few addresses at random (specified by `nodes`) and try (best-effort) to send the
    /// message only to them. This is useful to pick nodes with whom to sync.
    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

/// A connection is responsible to establish and keep alive (if possible) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<Bytes>,
}

impl Connection {
    fn spawn(address: SocketAddr, receiver: Receiver<Bytes>) {
        tokio::spawn(async move {
            Self { address, receiver }.run().await;
        });
    }

    /// Main loop trying to connect to the peer and transmit messages.
    async fn run(&mut self) {
        // Try to connect to the peer.
        let (mut writer, mut reader) = match TcpStream::connect(self.address).await {
            Ok(stream) => Framed::new(stream, LengthDelimitedCodec::new()).split(),
            Err(e) => {
                warn!(
                    "{}",
                    NetworkError::FailedToConnect(self.address, /* retry */ 0, e)
                );
                return;
            }
        };
        info!("Outgoing connection established with {}", self.address);

        // Transmit messages once we have established a connection.
        loop {
            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                Some(data) = self.receiver.recv() => {
                    if let Err(e) = writer.send(data).await {
                        warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                        return;
                    }
                },
                response = reader.next() => {
                    match response {
                        Some(Ok(_)) => {
                            // Sink the reply.
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            return;
                        }
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use futures::future::try_join_all;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    #[tokio::test]
    async fn simple_send() {
        // Run a TCP server.
        let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
        let message = "Hello, world!";
        let handle = listener(address, message.to_string());

        // Make the network sender and send the message.
        let mut sender = SimpleSender::new();
        sender.send(address, Bytes::from(message)).await;

        // Ensure the server received the message (ie. it did not panic).
        assert!(handle.await.is_ok());
    }

    #[tokio::test]
    async fn broadcast() {
        // Run 3 TCP servers.
        let message = "Hello, world!";
        let (handles, addresses): (Vec<_>, Vec<_>) = (0..3)
            .map(|x| {
                let address = format!("127.0.0.1:{}", 6_200 + x)
                    .parse::<SocketAddr>()
                    .unwrap();
                (listener(address, message.to_string()), address)
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unzip();

        // Make the network sender and send the message.
        let mut sender = SimpleSender::new();
        sender.broadcast(addresses, Bytes::from(message)).await;

        // Ensure all servers received the broadcast.
        assert!(try_join_all(handles).await.is_ok());
    }

    fn listener(address: SocketAddr, expected: String) -> JoinHandle<()> {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&address).await.unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            match reader.next().await {
                Some(Ok(received)) => {
                    assert_eq!(received, expected);
                    writer.send(Bytes::from("Ack")).await.unwrap()
                }
                _ => panic!("Failed to receive network message"),
            }
        })
    }
}
