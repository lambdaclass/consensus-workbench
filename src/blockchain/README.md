# Proof of work blockchain (Nakamoto consensus)

TODO

The ledger code was partially based in [this tutorial](https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/)

## Limitations and potential improvements
This is a simplistic implementation, with known limitations that would need to be addressed for a production-like environment:

- There's currently a single message to share state between peers, and it includes the entire ledger every time. This is used both for new or temporary crashed nodes to catch up and for all nodes to broadcast their latest mined block. This could be improved by sending chunks of the ledger every time, but that would also require adding logic to deal with missing pieces of the ledger.
- The difficulty prefix is really low (two leading zeros in the hash of the block) as to make mining fast for illustratory and testing purposes.
- There is no gossip protocol, the nodes learn eagerly about all peers in the network and broadcast transaction and blocks to all known peers every time.
- Messages are sent once per node, no acknowledge or retry is attempted.
- New or crashed nodes start mining with an empty mempool, there's no attempt to learn currently pending transactions from other peers.

## Example usage

TODO
