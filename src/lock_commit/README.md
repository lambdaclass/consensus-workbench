# Node with backup replication
This folder contains project files for a node that servers client requests and replicates its data to backup nodes.
Backup nodes take over when the primary stops responding (not implemented yet). For background see:

* [Primary-Backup State Machine Replication for Crash Failures](https://decentralizedthoughts.github.io/2019-11-01-primary-backup/)
* [Distributed systems for fun and profit](http://book.mixu.net/distsys/replication.html#primary-backup-replication)

`main.rs` is in charge of parsing CLI args and creating the node data. `node.rs` contains the state-machine states and logic to handle its supported commands.

It compiles to `/target/[cfg]/node_replication`.


## Example usage
In the bin output folder, you can run the binaries as follows:

Running a replica and a primary node that replicates to it (different ports)
```
./node_replication --port 6100 & ./node_replication --port 6101 --replicate-to 127.0.0.1:6100
```

Or you can run them on separate shell stdouts:
```
./node_replication --port 6100
./node_replication --port 6101 --replicate-to 127.0.0.1:6100
```

You can then issue commands from the client bin:
```
# send set to primary port
./client --port 6101 set k 123
# -> 2022-10-04T17:25:42.241Z INFO [client] 123

# send set to replica port, should fail
./client --port 6100 set k v2
# -> 2022-10-04T17:25:30.626Z ERROR [client] ERROR User cannot issue set commands to replicas
```
