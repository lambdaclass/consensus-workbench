use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

pub type StoreError = rocksdb::Error;
type StoreResult<T> = Result<T, StoreError>;

// FIXME why not just strings?
type Key = Vec<u8>;
type Value = Vec<u8>;

pub enum StoreCommand {
    Write(Key, Value),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
}

#[derive(Clone)]
pub struct Store {
    channel: Sender<StoreCommand>,
}

impl Store {
    pub fn new(path: &str) -> StoreResult<Self> {
        let db = rocksdb::DB::open_default(path)?;
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value) => {
                        db.put(&key, &value).unwrap();
                    }
                    StoreCommand::Read(key, sender) => {
                        let response = db.get(&key);
                        sender.send(response).unwrap();
                    }
                }
            }
        });
        Ok(Self { channel: tx })
    }

    pub async fn write(&self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            // FIXME return error instead of panicking
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    pub async fn read(&self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            // FIXME return error instead of panicking
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read command from store")
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
        let mut store = Store::new(path).unwrap();

        // Write value to the store.
        let key = vec![0u8, 1u8, 2u8, 3u8];
        let value = vec![4u8, 5u8, 6u8, 7u8];
        store.write(key.clone(), value.clone()).await;

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
        let mut store = Store::new(path).unwrap();

        // Try to read unknown key.
        let key = vec![0u8, 1u8, 2u8, 3u8];
        let result = store.read(key).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
