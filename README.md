# Consensus workbench
Proof of concept Rust implementations for various distributed systems protocols.

## Example usage

    # run a single echo server
    cargo run --bin single_node

    # on a separate shell, send key/value commands to the server
    cargo run --bin client -- set v1 hello
    cargo run --bin client -- get v1

## Implementations

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
