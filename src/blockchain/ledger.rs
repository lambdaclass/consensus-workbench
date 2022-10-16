/// TODO
/// loosely based on https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/
use std::fmt::Display;

use anyhow::{bail, Result};
use itertools::Itertools;

use lib::command::Command;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DIFFICULTY_PREFIX: &str = "00";

// TODO add type alias for txid and transaction

// TODO consider adding height
// TODO consider adding miner node
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Block {
    pub hash: String,
    pub previous_hash: String,
    pub data: Vec<(String, Command)>,
    pub nonce: u64,
}

impl Block {
    // FIXME what's the point of using vec here instead of the string only?
    pub fn calculate_hash(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(&self.previous_hash);
        hasher.update(self.nonce.to_string());
        for (txid, cmd) in &self.data {
            hasher.update(txid);
            hasher.update(cmd.to_string());
        }
        hasher.finalize().as_slice().to_owned()
    }

    pub fn genesis() -> Self {
        // TODO using ugly placeholder values for genesis, see if there are better ones
        let data = vec![];
        let mut block = Self {
            previous_hash: "genesis".to_string(),
            hash: "temporary".to_string(),
            data,
            nonce: 0,
        };
        block.hash = hex::encode(block.calculate_hash());

        block
    }

    /// TODO
    fn extends(&self, other: &Block) -> bool {
        if self.previous_hash != other.hash {
            warn!(
                "block has wrong previous hash {}, expected {}",
                self.previous_hash, other.hash
            );
            return false;
        }
        true
    }

    /// TODO
    fn is_valid(&self) -> bool {
        let decoded_hash = hex::decode(&self.hash);
        if decoded_hash.is_err() {
            warn!("block hash couldn't be decoded from hex {}", self.hash);
            return false;
        }

        if !hash_to_binary_representation(&decoded_hash.unwrap()).starts_with(DIFFICULTY_PREFIX) {
            warn!("block has invalid difficulty {}", self.hash);
            return false;
        } else if hex::encode(self.calculate_hash()) != self.hash {
            warn!("block has invalid hash {}", self.hash);
            return false;
        }
        true
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ledger {
    pub blocks: Vec<Block>,
}

impl Ledger {
    /// Creates a new ledger with a genesis block in it.
    pub fn new() -> Self {
        Self {
            blocks: vec![Block::genesis()],
        }
    }

    /// Viewing the ledger as the commit log of key/value commands, return the current value
    /// of the given key.i
    pub fn get(&self, key: &str) -> Option<String> {
        for block in self.blocks.iter().rev() {
            for (_, cmd) in &block.data {
                if let Command::Set {
                    key: block_key,
                    value,
                } = cmd
                {
                    if block_key == key {
                        return Some(value.clone());
                    }
                }
            }
        }

        None
    }

    /// Returns true if there's a transaction with the given id commited in some block of this ledger.
    pub fn contains(&self, txid: &str) -> bool {
        for block in self.blocks.iter().rev() {
            for (stored_txid, _) in &block.data {
                if stored_txid == txid {
                    return true;
                }
            }
        }
        false
    }

    /// FIXME
    pub fn is_valid(&self) -> bool {
        if self.blocks.is_empty() || *self.blocks.first().unwrap() != Block::genesis() {
            warn!("ledger has an invalid genesis block");
            return false;
        }

        for (previous, block) in self.blocks.iter().tuple_windows() {
            if !block.is_valid() || !block.extends(previous) {
                return false;
            }
        }

        true
    }

    /// FIXME
    pub fn extend(&self, block: Block) -> Result<Self> {
        if !block.is_valid() || !block.extends(self.blocks.last().unwrap()) {
            bail!("block {:?} is not a valid extension of the ledger", block);
        }
        let mut new_ledger = self.clone();
        new_ledger.blocks.push(block);
        Ok(new_ledger)
    }

    /// TODO
    pub fn mine_block(previous_block: Block, transactions: Vec<(String, Command)>) -> Block {
        info!("mining block...");
        let mut candidate = Block {
            previous_hash: previous_block.hash,
            hash: "not known yet".to_string(),
            data: transactions,
            nonce: 0,
        };

        loop {
            if candidate.nonce % 100000 == 0 {
                info!("nonce: {}", candidate.nonce);
            }
            let hash = candidate.calculate_hash();
            candidate.hash = hex::encode(&hash);
            let binary_hash = hash_to_binary_representation(&hash);
            if binary_hash.starts_with(DIFFICULTY_PREFIX) {
                info!(
                    "mined! nonce: {}, hash: {}, binary hash: {}",
                    candidate.nonce, candidate.hash, binary_hash
                );

                return candidate;
            }
            candidate.nonce += 1;
        }
    }
}

impl Display for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Ledger {{ length: {}, latest: {:?}  }}",
            self.blocks.len(),
            self.blocks.last().unwrap()
        )
    }
}

fn hash_to_binary_representation(hash: &[u8]) -> String {
    let mut res: String = String::default();
    for c in hash {
        res.push_str(&format!("{:b}", c));
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn calculate_hash() {
        // test that each of the block attributes contributes to the hash
        // (not the hash value itself)
        let mut block = Block {
            hash: "temporary hash".to_string(),
            previous_hash: Block::genesis().hash,
            data: vec![],
            nonce: 0,
        };
        let hash1 = block.calculate_hash();
        let hash2 = block.calculate_hash();
        assert_eq!(hash1, hash2);

        block.nonce = 1;
        let hash3 = block.calculate_hash();
        assert_ne!(hash1, hash3);

        block.previous_hash = "another".to_string();
        let hash4 = block.calculate_hash();
        assert_ne!(hash3, hash4);

        block.data = vec![(
            "txid".to_string(),
            Command::Set {
                key: "k".to_string(),
                value: "v".to_string(),
            },
        )];
        let hash5 = block.calculate_hash();
        assert_ne!(hash4, hash5);

        // same command, different txid
        block.data = vec![(
            "txid2".to_string(),
            Command::Set {
                key: "k".to_string(),
                value: "v".to_string(),
            },
        )];
        let hash6 = block.calculate_hash();
        assert_ne!(hash5, hash6);

        // the block's own hash does not affect it's hash calculation
        block.hash = "another".to_string();
        let hash7 = block.calculate_hash();
        assert_eq!(hash6, hash7);
    }

    #[tokio::test]
    async fn block_validation() {
        let genesis = Block::genesis();
        let mut block = Block {
            previous_hash: genesis.hash.to_string(),
            hash: "invalid".to_string(),
            data: vec![],
            nonce: 919,
        };

        assert!(block.extends(&genesis));

        // hash is invalid hex
        assert!(!block.is_valid());

        block.hash = hex::encode(block.calculate_hash());
        assert!(block.is_valid());
        assert!(block.extends(&genesis));

        // hash is invalid --the current hash is based on a different nonce
        block.nonce = 918;
        assert!(!block.is_valid());

        // hash is valid but doesn't meet proof of work
        block.hash = hex::encode(block.calculate_hash());
        assert!(!block.is_valid());
    }

    #[tokio::test]
    async fn ledger_operations() {
        let ledger = Ledger::new();
        assert_eq!(1, ledger.blocks.len());
        assert_eq!(Block::genesis(), *ledger.blocks.first().unwrap());

        // extend with valid block
        let block = Block {
            previous_hash: Block::genesis().hash.to_string(),
            hash: "00009c985d0019b01d8c0f865c32c4af76f6aa9216c361d843bfc0849598376a".to_string(),
            data: vec![],
            nonce: 919,
        };

        let ledger = ledger.extend(block.clone()).unwrap();
        assert_eq!(2, ledger.blocks.len());

        // fail extend on invalid block
        assert!(ledger.extend(block).is_err());
    }

    #[tokio::test]
    async fn ledger_validation() {
        // a valid block that extends genesis
        let mut block = Block {
            previous_hash: Block::genesis().hash.to_string(),
            hash: "00009c985d0019b01d8c0f865c32c4af76f6aa9216c361d843bfc0849598376a".to_string(),
            data: vec![],
            nonce: 919,
        };

        let mut ledger = Ledger::new();
        assert!(ledger.is_valid());
        ledger.blocks.push(block.clone());
        assert!(ledger.is_valid());

        // fail if first block is valid but not genesis
        ledger.blocks = vec![block.clone()];
        assert!(!ledger.is_valid());

        // fail if missing a genesis block
        ledger.blocks = vec![];
        assert!(!ledger.is_valid());

        // fail if invalid extension
        ledger.blocks = vec![Block::genesis(), block.clone(), block.clone()];
        assert!(!ledger.is_valid());

        // fail if invalid block
        block.nonce = 0;
        ledger.blocks = vec![Block::genesis(), block.clone()];
        assert!(!ledger.is_valid());
    }

    #[tokio::test]
    async fn mine_block() {
        let ledger = Ledger::new();

        let genesis = ledger.blocks.first().unwrap().clone();
        let transaction = (
            "tx1".to_string(),
            Command::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            },
        );
        let new_block = Ledger::mine_block(genesis.clone(), vec![transaction]);
        assert!(new_block.is_valid());
        assert!(new_block.extends(&genesis));

        let ledger = ledger.extend(new_block.clone()).unwrap();
        assert!(ledger.is_valid());
        assert!(ledger.contains("tx1"));
        assert_eq!("value", &ledger.get("key").unwrap());

        // repeat
        let transaction = (
            "tx2".to_string(),
            Command::Set {
                key: "key".to_string(),
                value: "another".to_string(),
            },
        );
        let new_new_block = Ledger::mine_block(new_block.clone(), vec![transaction]);
        assert!(new_new_block.is_valid());
        assert!(new_new_block.extends(&new_block));

        let ledger = ledger.extend(new_new_block.clone()).unwrap();
        assert!(ledger.is_valid());
        assert!(ledger.contains("tx2"));
        assert_eq!("another", &ledger.get("key").unwrap());
    }
}
