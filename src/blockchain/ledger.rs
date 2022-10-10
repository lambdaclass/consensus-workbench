/// TODO
/// loosely based on https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/
use lib::command::Command;
use log::{info, warn};
use serde::{Serialize, Deserialize};
use sha2::{Digest, Sha256};

const DIFFICULTY_PREFIX: &str = "00";
type Transaction = (String, Command);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Block {
    pub hash: String,
    pub previous_hash: String,
    pub data: Vec<Transaction>,
    pub nonce: u64,
}

impl Block {
    // TODO rename to calculate_hash
    pub fn calculate_hash(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(self.previous_hash.to_string());
        hasher.update(self.nonce.to_string());
        for (txid, cmd) in &self.data {
            hasher.update(txid);
            hasher.update(cmd.to_string());
        }
        hasher.finalize().as_slice().to_owned()
    }
}

pub struct Ledger {
    blocks: Vec<Block>
}

impl Ledger {
    /// Creates a new ledger with a genesis block in it.
    pub fn new() -> Self {
        // TODO using ugly placeholder values for genesis, see if there are better ones
        let data = vec![("0000000-0000-0000-0000-000000000000".to_string(),
                         Command::Get { key: "genesis".to_string() })];
        let genesis = Block {
            previous_hash: "genesis".to_string(),
            hash: "genesis".to_string(),
            data,
            nonce: 0
        };
        Self {
            blocks: vec![genesis]
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
                if let Command::Set {key: block_key, value} = cmd {
                    if block_key == key {
                        return Some(value.clone());
                    }
                }
            }
        }

        None
    }

    /// Returns true if the given block is valid candidate to extend the current chain.
    pub fn is_valid(&self, candidate_block: &Block) -> bool {
        // it should be safe to unwrap as there always should be a genesis block
        let previous_block = self.blocks.last().unwrap();

        // TODO make these checks more readable
        if candidate_block.previous_hash != previous_block.hash {
            warn!("block has wrong previous hash {}", candidate_block.previous_hash);
            return false;
        } else if !hash_to_binary_representation(
            &hex::decode(&candidate_block.hash).expect("couldn't decode from hex"),
        )
            .starts_with(DIFFICULTY_PREFIX)
        {
            warn!("block has invalid difficulty {}", candidate_block.hash);
            return false;

        } else if hex::encode(candidate_block.calculate_hash()) != candidate_block.hash
        {
            warn!("block has invalid hash {}", candidate_block.hash);
            return false;
        }
        true
    }


    /// TODO
    pub fn mine(&self, transactions: Vec<Transaction>) -> Block {
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
