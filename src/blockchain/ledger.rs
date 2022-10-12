use itertools::Itertools;
/// TODO
/// loosely based on https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/
use lib::command::Command;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const DIFFICULTY_PREFIX: &str = "00";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Block {
    pub hash: String,
    pub previous_hash: String,
    pub data: Vec<(String, Command)>,
    pub nonce: u64,
}

impl Block {
    // TODO rename to calculate_hash
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
        let data = vec![(
            "0000000-0000-0000-0000-000000000000".to_string(),
            Command::Get {
                key: "genesis".to_string(),
            },
        )];
        Block {
            previous_hash: "genesis".to_string(),
            hash: "genesis".to_string(),
            data,
            nonce: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ledger {
    blocks: Vec<Block>,
}

impl Ledger {
    /// Creates a new ledger with a genesis block in it.
    pub fn new() -> Self {
        Self {
            blocks: vec![Block::genesis()],
        }
    }

    pub fn length(&self) -> usize {
        self.blocks.len()
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

        let mut n = 1;
        for (previous, block) in self.blocks.iter().tuple_windows() {
            if block.previous_hash != previous.hash {
                warn!(
                    "block {} has wrong previous hash {}, expected {}",
                    n, block.previous_hash, previous.hash
                );
                return false;
            } else if !hash_to_binary_representation(
                &hex::decode(&block.hash).expect("couldn't decode from hex"),
            )
            .starts_with(DIFFICULTY_PREFIX)
            {
                warn!("block {} has invalid difficulty {}", n, block.hash);
                return false;
            } else if hex::encode(block.calculate_hash()) != block.hash {
                warn!("block {} has invalid hash {}", n, block.hash);
                return false;
            }

            n += 1;
        }

        true
    }

    /// TODO
    pub fn mine(&self, transactions: Vec<(String, Command)>) -> Block {
        // TODO this will have to run on a cancellable async task
        //

        let previous_block = self.blocks.last().unwrap().clone();

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
                    candidate.nonce,
                    hex::encode(&candidate.hash),
                    binary_hash
                );
                return candidate;
            }
            candidate.nonce += 1;
        }
    }
}

fn hash_to_binary_representation(hash: &[u8]) -> String {
    let mut res: String = String::default();
    for c in hash {
        res.push_str(&format!("{:b}", c));
    }
    res
}
