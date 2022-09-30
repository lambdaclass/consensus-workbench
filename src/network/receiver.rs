// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use log::{debug, warn};
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// Convenient alias for the writer end of the TCP channel.
pub type Writer = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgements to the sender machine (see unit tests for examples).
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) -> JoinHandle<()> {
        tokio::spawn(async move { Self { address, handler }.run().await })
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
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
            Self::spawn_runner(socket, peer, self.handler.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(socket: TcpStream, peer: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        if let Err(e) = handler.dispatch(&mut writer, message.freeze()).await {
                            warn!("{}", e);
                            return;
                        }
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
        async fn dispatch(
            &self,
            writer: &mut Writer,
            message: Bytes,
        ) -> Result<(), Box<dyn Error>> {
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
        Receiver::spawn(address, TestHandler { deliver: tx });
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
