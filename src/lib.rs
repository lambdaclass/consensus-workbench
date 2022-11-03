pub mod command;
pub mod network;
pub mod store;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub type NetworkReceiver<T> = mpsc::Receiver<(T, oneshot::Sender<Result<Option<Vec<u8>>>>)>;
pub type NetworkSender<T> = mpsc::Sender<(T, oneshot::Sender<Result<Option<Vec<u8>>>>)>;
