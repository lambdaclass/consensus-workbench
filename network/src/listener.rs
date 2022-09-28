use std::{net::SocketAddr};
use tokio::{net::{TcpListener}, io::AsyncReadExt};

pub struct Listener {
    socket: SocketAddr,
}

impl Listener {
    pub async fn listen(&self) {
        let listener = TcpListener::bind(&self.socket)
            .await
            .expect("Error binding socket");

            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok((socket, addr)) => {
                        println!("new client connection: {:?}", addr); 
                        (socket, addr)
                    }
                    Err(e) => {
                        println!("error accepting connection: {:?}", e);
                        continue;
                    }
                };

                // we got a connection so we are good to spawn a new thread to handle it

                tokio::spawn( async move {
                    let mut buf = vec![0; 1024];
                    let n = socket.read(&mut buf).await.unwrap();
                    println!("received {:?}",buf);
                });
            }

        // accept connections and process them
        /* tokio::run(listener.incoming()
            .map_err(|e| eprintln!("failed to accept socket; error = {:?}", e))
            .for_each(|socket| {
                process_socket(socket);
                Ok(())
            }) 
        )*/
    }
}