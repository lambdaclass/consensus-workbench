use std::{sync::mpsc::Sender, net::SocketAddr};

use network::{*, listener::Listener, sender::Connection};

#[tokio::main]
async fn main(){
    let socket: SocketAddr = "127.0.0.1:6399".parse().unwrap();

    let listener = Listener::spawn(socket).await;
    let sender  = network::sender::Connection::spawn(socket).await;
}
