/// This module contains blocks and a ledger (a list of those blocks where each element contains a hash of the previous one)
/// used as the commit log of a key value store: each block contains a (possibly empty) list of write (set) commands of key values.
use std::fmt::Display;

use anyhow::{bail, Result};
use itertools::Itertools;

use lib::command::ClientCommand;
use log::{debug, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// Difficulty is expressed as the maximum u32 unsigned integer shifted a number of
// bits to the right. This way, a difficulty of u32::MAX >> n means "the hash has
// to start with n zeroes". The higher the n, the higher the difficulty.
const DIFFICULTY_TARGET: u32 = if cfg!(test) {
    // Lower the difficulty for testing so it doesn't take very long
    u32::MAX >> 16
} else {
    u32::MAX >> 18
};

pub type TransactionId = String;
pub type Transaction = (TransactionId, ClientCommand);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Block {
    pub miner_id: String,
    height: u64,
    hash: String,
    previous_hash: String,
    data: Vec<Transaction>,
    nonce: u64,
}

impl Block {
    /// Generate a hex string of a Sha256 hash for the attributes in this block.
    /// The hash field itself doesn't affect the result.
    pub fn calculate_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(&self.height.to_string());
        hasher.update(&self.miner_id);
        hasher.update(&self.previous_hash);
        hasher.update(self.nonce.to_string());
        for (txid, cmd) in &self.data {
            hasher.update(txid);
            hasher.update(cmd.to_string());
        }
        let hash = hasher.finalize().as_slice().to_owned();
        hex::encode(&hash)
    }

    /// Create a genesis block, which is expected to be the first block of any valid ledger.
    pub fn genesis() -> Self {
        // using ugly placeholder values for genesis, maybe there are better ones
        let data = vec![];
        // We use a random initial nonce so different nodes start at different values
        // If they all start at the same value they will take the same amount of time
        // to mine a block.
        let mut block = Self {
            height: 0,
            miner_id: "god".to_string(),
            previous_hash: "genesis".to_string(),
            hash: "temporary".to_string(),
            data,
            nonce: 0,
        };
        block.hash = block.calculate_hash();

        block
    }

    /// Returns if this is a valid node: if its hash attribute matches the result of hashing the block data
    /// and meets the difficulty prefix (the amount of leading zeros) for the proof of work.
    fn is_valid(&self) -> bool {
        match is_below_difficulty_target(&self.hash) {
            Ok(false) => {
                warn!("block has invalid difficulty {}", self.hash);
                return false;
            }
            Err(_) => {
                warn!("block has malformed hash {}", self.hash);
                return false;
            }
            Ok(true) => {}
        }

        if self.calculate_hash() != self.hash {
            warn!("block has invalid hash {}", self.hash);
            return false;
        }
        true
    }

    /// Returns true if the given block is an extension of this one.
    fn extends(&self, other: &Block) -> bool {
        if self.previous_hash != other.hash {
            warn!(
                "block has wrong previous hash {}, expected {}",
                self.previous_hash, other.hash
            );
            return false;
        }
        if self.height != other.height + 1 {
            warn!(
                "block has wrong height {}, expected {}",
                self.height,
                other.height + 1
            );
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
    /// of the given key.
    pub fn get(&self, key: &str) -> Option<String> {
        for block in self.blocks.iter().rev() {
            for (_, cmd) in &block.data {
                if let ClientCommand::Set {
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

    /// Return whether this blockchain is valid: it starts with the expected genesis block and
    /// each subsequent block is a valid extensions of the previous.
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

    /// Return a new ledger that is the same as the current one with the given block added at the top.
    /// Fails if the block is an invalid extension of this ledger.
    pub fn extend(&self, block: Block) -> Result<Self> {
        if !block.is_valid() || !block.extends(self.blocks.last().unwrap()) {
            bail!("block {:?} is not a valid extension of the ledger", block);
        }
        let mut new_ledger = self.clone();
        new_ledger.blocks.push(block);
        Ok(new_ledger)
    }

    const MINER_LOG_EVERY: u64 = 100000;

    /// Produce a block that extends the given one and includes the given list of transactions as its
    /// data, by trying different nonce values until the hash of the block meets the difficulty prefix
    /// --- the amount of leading zeros in the hash that is the proof of work.
    /// Note that the transactions are assumed to be safe for inclusion in the block, no duplicate
    /// checks are run here.
    pub async fn mine_block(
        miner_id: &str,
        previous_block: Block,
        transactions: Vec<Transaction>,
    ) -> Block {
        debug!("mining block...");
        let initial_nonce = rand::thread_rng().gen_range(0, 100000000);
        let mut candidate = Block {
            height: previous_block.height + 1,
            miner_id: miner_id.to_string(),
            previous_hash: previous_block.hash,
            hash: "not known yet".to_string(),
            data: transactions,
            nonce: initial_nonce,
        };

        loop {
            if candidate.nonce % Self::MINER_LOG_EVERY == 0 {
                debug!("nonce: {}", candidate.nonce);
                // This yield deserves some explanation. The problem is the
                // following: if a different node finds a valid PoW block before
                // us, the mining task needs to be reset so we can start on a proof
                // of work for the new chain. To do this, what we do is send an abort
                // signal (https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html#method.abort)
                // to the task to shut it down. For this to work, however, the mining task
                // has to yield control to the executor, otherwise the signal is never sent
                // and the task keeps mining until it finds a (now invalid and thus useless) PoW.
                // Therefore, every once in a while this mining function will yield to make sure
                // it aborts if it has to.
                // In a production-like environment, we would setup a worker pool outside of tokio to handle this
                // cpu-bound job, but we prefer to keep it simple for this implementation.
                tokio::task::yield_now().await;
            }
            candidate.hash = candidate.calculate_hash();
            // I'm unwrapping because the only posible error is `candidate.hash` not
            // being a valid hexstring, and that's not possible here.
            if is_below_difficulty_target(&candidate.hash).unwrap() {
                debug!(
                    "mined! nonce: {}, hash: {}",
                    candidate.nonce, candidate.hash
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

/// Returns whether the first four bytes of a given hash, when
/// expressed as a 32 bit unsigned integer, are less than the
/// the difficulty target or not. In practice, because we express the
/// difficulty target as a number consisting of n zeroes followed by
/// (32 - n) ones, what we are checking is if the first n bits are zero.
fn is_below_difficulty_target(hash: &str) -> Result<bool> {
    let hash_bytes = hex::decode(hash)?;
    let first_four_bytes = u32::from(hash_bytes[0])
        + (u32::from(hash_bytes[1]) << 8)
        + (u32::from(hash_bytes[2]) << 16)
        + (u32::from(hash_bytes[3]) << 24);

    Ok(first_four_bytes < DIFFICULTY_TARGET)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn calculate_hash() {
        // test that each of the block attributes contributes to the hash
        // (not the hash value itself)
        let mut block = Block {
            height: 1,
            miner_id: "127.0.0.1:6100".to_string(),
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
            ClientCommand::Set {
                key: "k".to_string(),
                value: "v".to_string(),
            },
        )];
        let hash5 = block.calculate_hash();
        assert_ne!(hash4, hash5);

        // same command, different txid
        block.data = vec![(
            "txid2".to_string(),
            ClientCommand::Set {
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
            height: 1,
            miner_id: "127.0.0.1:6100".to_string(),
            previous_hash: genesis.hash.to_string(),
            hash: "invalid".to_string(),
            data: vec![],
            nonce: 67048562,
        };

        assert!(block.extends(&genesis));

        // height is not the next from the previous block
        block.height = 10;
        assert!(!block.extends(&genesis));
        // restore
        block.height = 1;

        // hash is invalid hex
        assert!(!block.is_valid());

        block.hash = block.calculate_hash();
        assert!(block.is_valid());
        assert!(block.extends(&genesis));

        // hash is invalid --the current hash is based on a different nonce
        block.nonce = 918;
        assert!(!block.is_valid());

        // hash is valid but doesn't meet proof of work
        block.hash = block.calculate_hash();
        assert!(!block.is_valid());
    }

    #[tokio::test]
    async fn ledger_operations() {
        let ledger = Ledger::new();
        assert_eq!(1, ledger.blocks.len());
        assert_eq!(Block::genesis(), *ledger.blocks.first().unwrap());

        // extend with valid block
        let block = Block {
            height: 1,
            miner_id: "127.0.0.1:6100".to_string(),
            previous_hash: Block::genesis().hash,
            hash: "ad260000963facb4d34b6b503d01beba7c58edaf6176b46ca92db75592cd8cf0".to_string(),
            data: vec![],
            nonce: 67048562,
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
            height: 1,
            miner_id: "127.0.0.1:6100".to_string(),
            previous_hash: Block::genesis().hash,
            hash: "ad260000963facb4d34b6b503d01beba7c58edaf6176b46ca92db75592cd8cf0".to_string(),
            data: vec![],
            nonce: 67048562,
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
        ledger.blocks = vec![Block::genesis(), block];
        assert!(!ledger.is_valid());
    }

    #[tokio::test]
    async fn mine_block() {
        let ledger = Ledger::new();

        let genesis = ledger.blocks.first().unwrap().clone();
        let transaction = (
            "tx1".to_string(),
            ClientCommand::Set {
                key: "key".to_string(),
                value: "value".to_string(),
            },
        );
        let new_block =
            Ledger::mine_block("127.0.0.1:6100", genesis.clone(), vec![transaction]).await;
        assert!(new_block.is_valid());
        assert!(new_block.extends(&genesis));

        let ledger = ledger.extend(new_block.clone()).unwrap();
        assert!(ledger.is_valid());
        assert!(ledger.contains("tx1"));
        assert_eq!("value", &ledger.get("key").unwrap());

        // repeat
        let transaction = (
            "tx2".to_string(),
            ClientCommand::Set {
                key: "key".to_string(),
                value: "another".to_string(),
            },
        );
        let new_new_block =
            Ledger::mine_block("127.0.0.1:6100", new_block.clone(), vec![transaction]).await;
        assert!(new_new_block.is_valid());
        assert!(new_new_block.extends(&new_block));

        let ledger = ledger.extend(new_new_block).unwrap();
        assert!(ledger.is_valid());
        assert!(ledger.contains("tx2"));
        assert_eq!("another", &ledger.get("key").unwrap());
    }
}
