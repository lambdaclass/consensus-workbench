# Consensus workbench
Proof of concept Rust implementations for various distributed systems protocols.

Each sub-directory contains an implementation of a key/value store with specific replication or consensus strategies.

1. [Single node server](/src/single_node)
1. [Primary/backup server](/src/primary_backup)
1. Two-phase commit (TODO)
2. [Lock-commit](/src/lock_commit)
3. Raft (TODO)
3. [Proof of work blockchain](/src/blockchain)
4. Streamlet (TODO)
5. Tendermint (TODO)
5. HotStuff (TODO)
6. Narwhal+Tusk (TODO)
6. Narwhal+Bullshark (TODO)


## Example usage

    # run a single-node server
    cargo run --bin single_node

    # on a separate shell, send key/value commands to the server
    cargo run --bin client -- set v1 hello
    cargo run --bin client -- get v1

The default log level is `INFO`, to change it set the `RUST_LOG` environment variable before running. Possible values are `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG` and `TRACE`.

See the specific implementation directories for details on how to run each of them.

## Suggested reads

This project is intended to be used as learning and training material for an introduction to conensus in distributed systems. 
For those new to these topics, we suggest checking the following background material before jumping into the code:

1. Chapter 12 of [Real-world cryptography](https://www.manning.com/books/real-world-cryptography): _Crypto as in cryptocurrency_ provides a great introduction to the replication problem in distributed systems and goes from classical to blockchain solutions, including details about DiemBFT one of the most advanced consensus algorithms available.
2. [This raft vizualization](http://thesecretlivesofdata.com/raft/) shows one of the most popular algorithms in action.
    - As a complement the [raft paper], as well as the algorithm itself, is the best example of a consensus algorithm as found in popular distributed systems as etcd.
3. The [tendermint 2014 whitepaper](https://tendermint.com/static/docs/tendermint.pdf) is a short read that shows how consensus algorithms are used in the context of blockchain, and offers an alternative to Bitcoin's onerous proof-of-work mining.
    - This [longer 2016 thesis](https://knowen-production.s3.amazonaws.com/uploads/attachment/file/1814/Buchman_Ethan_201606_Msater%2Bthesis.pdf) extends the material providing a lot of useful historical context that helps briding the classical distributed systems research with the modern blockchain-related efforts.
4. The [Tokio tutorial](https://tokio.rs/tokio/tutorial) is a good introduction to the async Rust techniques used in this project.

Additionally, we suggest reading the reference articles mentioned in each implementation's README.
    
## Acknowledgments

- The network and store modules of this projects were originally copied from [asonnino/hotstuff](https://github.com/asonnino/hotstuff) and are under the Apache license.
- The ledger module in the blockchain implementation was originally based on [this tutorial](https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/).
- Some of the algorithms were based on guides from the [Decentralized Thoughts](https://decentralizedthoughts.github.io/) blog.
