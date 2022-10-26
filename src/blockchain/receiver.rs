// Copyright(C) Facebook, Inc. and its affiliates.
use futures::stream::StreamExt as _;
use futures::SinkExt;
use log::{debug, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, channel};
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const CHANNEL_CAPACITY: usize = 1_000;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Failed to accept connection: {0}")]
    FailedToListen(std::io::Error),

    #[error("Failed to receive message from {0}: {1}")]
    FailedToReceiveMessage(SocketAddr, std::io::Error),
}

// FIXME consider renaming to TcpSender, TcpReceiver, ChannelSender, ChannelReceiver, etc. to reduce ambiguity
/// A TCP Receiver listens for peer connections and writes messages from all connections to a multi-producer
/// single-consumer (mpsc) tokio channel.
pub struct Receiver<Request, Response> {
    /// Address to listen to.
    address: SocketAddr,

    /// The sending end of the channel where incoming tpc messages will be forwarded to
    sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
}

impl<
        Request: DeserializeOwned + Send + Debug + 'static,
        Response: Serialize + Send + Debug + 'static,
    > Receiver<Request, Response>
{
    /// Spawn a new network receiver handling connections from any incoming peer.
    /// The messages received through those connections are written to channel, whose receiving end is returned.
    pub fn new(
        address: SocketAddr,
    ) -> (Self, mpsc::Receiver<(Request, oneshot::Sender<Response>)>) {
        let (sender, receiver) = channel(CHANNEL_CAPACITY);
        (Self { address, sender }, receiver)
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    pub async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::FailedToListen(e));
                    continue;
                }
            };
            debug!("Incoming connection established with {}", peer);
            Self::spawn_runner(socket, peer, self.sender.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(
        socket: TcpStream,
        peer: SocketAddr,
        sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
    ) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        // TODO add comments
                        // FIXME unwraps
                        let request = bincode::deserialize(&message.freeze()).unwrap();
                        let (reply_sender, reply_receiver) = oneshot::channel();
                        sender.send((request, reply_sender)).await.unwrap();
                        let reply = reply_receiver.await.unwrap();
                        let reply = bincode::serialize(&reply).unwrap();
                        writer.send(reply.into()).await.unwrap();
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            debug!("Connection closed by peer {}", peer);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use tokio::time::{sleep, Duration};

    use tokio::net::TcpStream;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    #[tokio::test]
    async fn receive() {
        // Make the network receiver.
        let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
        let (receiver, mut rx): (Receiver<String, ()>, _) = Receiver::new(address);
        tokio::spawn(async move {
            receiver.run().await;
        });
        sleep(Duration::from_millis(50)).await;

        // Send a message.
        let sent = "Hello, world!".to_string();
        let bytes = Bytes::from(bincode::serialize(&sent).unwrap());
        let stream = TcpStream::connect(address).await.unwrap();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        transport.send(bytes.clone()).await.unwrap();

        // Ensure the message gets passed to the channel.
        let message = rx.recv().await;
        assert!(message.is_some());
        let (received, _) = message.unwrap();
        assert_eq!(received, sent);
    }
}
