use anyhow::{anyhow, Context, Result};
use log::error;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;

type Key = Vec<u8>;
type Value = Vec<u8>;

#[derive(Debug)]
pub enum StoreCommand {
    Write(Key, Value),
    Read(Key),
}

/// (sender, command) pair used to interact with the task that manages the store.
/// The sender is used to reply with responses.
type CommandMessage = (oneshot::Sender<Result<Option<Value>>>, StoreCommand);

#[derive(Clone)]
pub struct Store {
    channel: Sender<CommandMessage>,
}

impl Store {
    pub fn new(path: &str) -> Result<Self> {
        let db = rocksdb::DB::open_default(path)?;
        let (tx, mut rx): (Sender<CommandMessage>, Receiver<CommandMessage>) = channel(100);

        tokio::spawn(async move {
            while let Some((sender, command)) = rx.recv().await {
                let response = match command {
                    StoreCommand::Write(key, value) => db.put(key, &value).and(Ok(Some(value))),
                    StoreCommand::Read(key) => db.get(key),
                };

                // convert internal rocksdb error to anyhow before returning
                let response = response.map_err(|e| anyhow!(e));

                if let Err(error) = sender.send(response) {
                    error!("store failed to send reply to send channel {:?}", error);
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&self, key: Key, value: Value) -> Result<Option<Value>> {
        self.send(StoreCommand::Write(key, value)).await
    }

    pub async fn read(&self, key: Key) -> Result<Option<Value>> {
        self.send(StoreCommand::Read(key)).await
    }

    async fn send(&self, command: StoreCommand) -> Result<Option<Value>> {
        let (sender, receiver) = oneshot::channel();

        self.channel
            .send((sender, command))
            .await
            .context("failed to send command to store")?;

        receiver
            .await
            .context("failed to receive reply from store")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn create_store() {
        // Create new store.
        let path = ".db_test_create_store";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path);
        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn read_write_value() {
        // Create new store.
        let path = ".db_test_read_write_value";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();

        // Write value to the store.
        let key = vec![0u8, 1u8, 2u8, 3u8];
        let value = vec![4u8, 5u8, 6u8, 7u8];
        store.write(key.clone(), value.clone()).await.unwrap();

        // Read value.
        let result = store.read(key).await;
        assert!(result.is_ok());
        let read_value = result.unwrap();
        assert!(read_value.is_some());
        assert_eq!(read_value.unwrap(), value);
    }

    #[tokio::test]
    async fn read_unknown_key() {
        // Create new store.
        let path = ".db_test_read_unknown_key";
        let _ = fs::remove_dir_all(path);
        let store = Store::new(path).unwrap();

        // Try to read unknown key.
        let key = vec![0u8, 1u8, 2u8, 3u8];
        let result = store.read(key).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
