# Consensus workbench
Proof of concept Rust implementations for various distributed systems patterns.

## Example usage

    # run a single echo server
    cargo run --bin single_node

    # on a separate shell, send key/value commands to the server
    cargo run --bin client -- set v1 hello
    cargo run --bin client -- get v1

## Implementations

1. [Single node server](single_node)
1. [Primary/backup server](primary_backup) (planned)
1. [Two-phase commit](two_phase_commit) (planned)
