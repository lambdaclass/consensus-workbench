# Node with replication
This folder contains project files for a node that can replicate its data.

`node.rs` is the file that contains main(), and is in charge of parsing CLI args and creating the node data.
`primary.rs` and `replica.rs` contain mainly the message handlers for each of those alternatives.

It compiles to `/target/[cfg]/node_replication`.


## Example usage
In the bin output folder, you can run the binaries as follows:

Running a single primary node on port 6100
```
./node_replication --port 6100
```

Running a replica and a primary node that replicates to it (different ports)
```
./node_replication --replica --port 6100 & ./node_replication --port 6101 --replicate-to 127.0.0.1:6100 
```

Or you can run them on separate shell stdouts:
```
./node_replication --replica --port 6100
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

