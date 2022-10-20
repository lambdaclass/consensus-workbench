# Consensus workbench
Proof of concept Rust implementations for various distributed systems protocols.

## Example usage

    # run a single-node server
    cargo run --bin single_node

    # on a separate shell, send key/value commands to the server
    cargo run --bin client -- set v1 hello
    cargo run --bin client -- get v1

The default log level is `INFO`, to change it set the `RUST_LOG` environment variable before running. Possible values are `OFF`, `ERROR`, `WARN`, `INFO`, `DEBUG` and `TRACE`.

See the specific implementation directories for details on how to run each of them.

## Implementations

Each sub-directory contains an implementation of a key/value store with specific replication or consensus strategies.

1. [Single node server](/src/single_node)
1. [Primary/backup server](/src/primary_backup)
1. Two-phase commit (TODO)
2. Lock-commit (TODO)
3. Raft (TODO)
3. [Proof of work blockchain](/src/blockchain)
4. Streamlet (TODO)
5. Tendermint (TODO)
5. HotStuff (TODO)
6. Narwhal+Tusk (TODO)
6. Narwhal+Bullshark (TODO)

## Acknowledgments

- The network and store modules of this projects were originally copied from [asonnino/hotstuff](https://github.com/asonnino/hotstuff) and are under the Apache license.
- The ledger module in the blockchain implementation was originally based on [this tutorial](https://blog.logrocket.com/how-to-build-a-blockchain-in-rust/).
- Some of the algorithms were based on guides from the [Decentralized Thoughts](https://decentralizedthoughts.github.io/) blog.
