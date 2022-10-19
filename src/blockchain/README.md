# Proof of work blockchain (Nakamoto consensus)

This module contains an implementation of a key/value store using a blockchain as a commit log: each block contains a list of transactions that are write commands to the store (e.g. set key = "value").
The keys are read by scanning for the most recent value for the key in the ledger.

Nodes put new transactions in mempool (a list of pending transactions) and attempt to mine blocks extending the current ledger and including transactions from that mempool. A block is "mined" by changing a nonce value until the block hash has the desired amount of leading zeros (this is the proof of work)

The blockchain code was originally based on [this tutorial](https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/).

## Limitations and potential improvements
This is a simplistic implementation, with known limitations that would need to be addressed for a production-like environment:

- There's currently a single message to share state between peers, and it includes the entire ledger every time. This is used both for new or temporary crashed nodes to catch up and for all nodes to broadcast their latest mined block. This could be improved by sending chunks of the ledger every time, but that would also require adding logic to deal with missing pieces of the ledger.
- The difficulty prefix is small (two leading zeros in the hash of the block) as to make mining fast for illustratory and testing purposes.
- There is no limit enforced in the block size or in the amount of transactions to be included in a block.
- There is no reward or incentive mechanism for miners.
- There is no gossip protocol, the nodes learn eagerly about all peers in the network and broadcast transaction and blocks to all known peers every time.
- Messages are sent once per node, no acknowledge or retry is attempted.
- New or crashed nodes start mining with an empty mempool, there's no attempt to learn currently pending transactions from other peers.
- Reads of the store are done by scanning backwards the ledger (this could be improved by having an in-memory map of key/values)

## Example usage

Start an new empty node:

	cargo run --bin blockchain -- -p 6100

Start a node using another as the seed (to learn about current blockchain state)

    cargo run --bin blockchain -- -p 6101 --seed 127.0.0.1:6100

Send a command to a node:

    cargo run --bin client -- -p 6100 set v1 hello

The command will be propagated to each node's mempool and eventually included in a block, which will then be propagated in the network until all nodes agree on a version of the chain. Once the transaction is committed to the ledger, the value can be retrieved:

    cargo run --bin client -- -p 6100 get key
