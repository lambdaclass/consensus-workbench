use std::{net::SocketAddr, sync::mpsc::Receiver};
use tokio::{net::TcpStream, io::AsyncReadExt};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
/* 
// sender does not make much sense for now since we don't maintain a list of connections

struct Sender {
    // connections: Vec<Connection>
    connection: Connection
}

impl Sender {
    pub async fn send(&mut self, address: SocketAddr, data: Vec<u8>) {
        // todo: try to reuse existing connections, if any

        let tx = Connection::spawn(address);
        if tx.send(data).await.is_ok() {
            self.connections.insert(address, tx);
        }
    }
}
 */

/// A connection is responsible to establish and keep alive (if possible) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<Vec<u8>>,
}

impl Connection {
    fn spawn(address: SocketAddr, receiver: Receiver<Vec<u8>>) {
        tokio::spawn(async move {
            Self { address, receiver }.run().await;
        });
    }

    /// Main loop trying to connect to the peer and transmit messages.
    async fn run(&mut self) {
        // Try to connect to the peer.
        let (mut reader, mut writer) = match TcpStream::connect(self.address).await {
            Ok(stream) => stream.split(),
            Err(e) => {
                println!("Error connecting");
                return;
            }
        };
        println!("Outgoing connection established with {}", self.address);
        // Transmit messages once we have established a connection.
        loop {
            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                Some(data) = self.receiver.recv() => {
                    if let Err(e) = writer.send(data).await {
                        return;
                    }
                    // data was sent succesfully
                },
                response = reader.read() => {
                    match response {
                        Some(Ok(_)) => {
                            // Sink the reply.
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            return;
                        }
                    }
                },
            }
        }
    }

    fn disconnect(&self) {

    }
}
