# Consensus workbench
Proof of concept Rust implementations for various distributed systems patterns.

## Example usage

    # run a single echo server
    cargo run --bin single_node

    # on a separate shell, send a command to the server
    cargo run --bin client

## Implementations

1. [Single node server](single_node)
1. [Primary/backup server](primary_backup) (planned)
1. [Two-phase commit](two_phase_commit) (planned)
