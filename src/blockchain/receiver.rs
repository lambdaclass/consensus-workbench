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
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Failed to accept connection: {0}")]
    FailedToListen(std::io::Error),

    #[error("Failed to receive message from {0}: {1}")]
    FailedToReceiveMessage(SocketAddr, std::io::Error),
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Request, Response> {
    /// Address to listen to.
    address: SocketAddr,
    sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
}

impl<
        Request: DeserializeOwned + Send + Debug + 'static,
        Response: Serialize + Send + Debug + 'static,
    > Receiver<Request, Response>
{
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn new(
        address: SocketAddr,
        sender: mpsc::Sender<(Request, oneshot::Sender<Response>)>,
    ) -> Self {
        // FIXME create channel here
        Self { address, sender }
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
    use tokio::sync::mpsc::channel;
    use tokio::time::{sleep, Duration};

    use tokio::net::TcpStream;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    #[tokio::test]
    async fn receive() {
        // Make the network receiver.
        let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
        let (tx, mut rx) = channel(1);
        tokio::spawn(async move {
            let receiver: Receiver<String, ()> = Receiver::new(address, tx);
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
