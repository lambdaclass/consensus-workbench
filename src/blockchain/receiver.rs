// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use log::{debug, warn};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Failed to accept connection: {0}")]
    FailedToListen(std::io::Error),

    #[error("Failed to receive message from {0}: {1}")]
    FailedToReceiveMessage(SocketAddr, std::io::Error),
}

/// Convenient alias for the writer end of the TCP channel.
pub type Writer = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<M> {
    /// Address to listen to.
    address: SocketAddr,
    sender: mpsc::Sender<M>
}

impl<M: DeserializeOwned + std::fmt::Debug + Send + 'static> Receiver<M> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn new(address: SocketAddr, sender: mpsc::Sender<M>) -> Self {
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
    async fn spawn_runner(socket: TcpStream, peer: SocketAddr, sender: mpsc::Sender<M>) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            // FIXME add optional writing
            let (mut _writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        // FIXME unwrap
                        let request = bincode::deserialize(&message.freeze()).unwrap();
                        sender.send(request).await.unwrap();
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
    use futures::sink::SinkExt as _;
    use tokio::sync::mpsc::channel;
    use tokio::sync::mpsc::Sender;
    use tokio::time::{sleep, Duration};

    use async_trait::async_trait;
    use tokio::net::TcpStream;
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    #[derive(Clone)]
    struct TestHandler {
        deliver: Sender<String>,
    }

    #[async_trait]
    impl MessageHandler for TestHandler {
        async fn dispatch(&mut self, writer: &mut Writer, message: Bytes) -> Result<()> {
            // Reply with an ACK.
            let _ = writer.send(Bytes::from("Ack")).await;

            // Deserialize the message.
            let message = bincode::deserialize(&message).unwrap();

            // Deliver the message to the application.
            self.deliver.send(message).await.unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn receive() {
        // Make the network receiver.
        let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
        let (tx, mut rx) = channel(1);
        tokio::spawn(async move {
            let receiver = Receiver::new(address, TestHandler { deliver: tx });
            receiver.run().await;
        });
        sleep(Duration::from_millis(50)).await;

        // Send a message.
        let sent = "Hello, world!";
        let bytes = Bytes::from(bincode::serialize(sent).unwrap());
        let stream = TcpStream::connect(address).await.unwrap();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        transport.send(bytes.clone()).await.unwrap();

        // Ensure the message gets passed to the channel.
        let message = rx.recv().await;
        assert!(message.is_some());
        let received = message.unwrap();
        assert_eq!(received, sent);
    }
}
