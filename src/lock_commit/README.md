# Node with lock-commit paradigm
This folder contains project files for a node that serves client requests with a lock-commit paradigm.

* [The Lock-Commit Paradigm](https://decentralizedthoughts.github.io/2020-11-29-the-lock-commit-paradigm/)
* [The Lock-Commit Paradigm: Multi-shot and Mixed Faults](https://decentralizedthoughts.github.io/2020-11-30-the-lock-commit-paradigm-multi-shot-and-mixed-faults/)

`main.rs` is in charge of parsing CLI args and creating the node data. `node.rs` contains the logic to handle its supported commands.

It compiles to `/target/[cfg]/node_lock_commit`.
Some important points:

- While the original lock-commit technique is used for getting consensus for a log of size 1, this example has been extended for a growing log list with a changing primary
- The blog posts are pretty generic in some senses, so this implementation has to make some assumptions
    - For instance, the view->primary mapping is done through `peers.len() % view_number`
- By default, the nodes do not run with a view-change mechanism in place (you can turn it on by passing `view-change` to the binary)
    - While it works, the view-change mechanism has not been tested much
- The shared ./client application for now does not work with this because it does not know about the Command enum (it only sends ClientCommands), so deserialization is incorrect 
    - As a workaround for this, if you pass a ClientCommand to the node_lock_commit binary, it will act as a client instead of node server (see examples)

Some key to-dos/leftover work:

- Right now, we are using the shared network library which will clone the message handler. This message handler is implemented on the Node struct, which has shared state with Arc<RwLocks<>>.
    - Ideally this implementation needs to change and use channels
- Tests need to include view-change mechanism, and general queries to the nodes to see if Propose/Lock messages have been broadcast on the network
- General cleanup/finishing touches

## Example usage
`start.sh [number_nodes]` is a small script used to start up as many nodes as you want in background:

````
$ ./start.sh 2

2022-10-20T17:48:33.452Z INFO [node_lock_commit] Node: Running on 127.0.0.1:6101. Primary = false...
2022-10-20T17:48:33.452Z INFO [node_lock_commit] Node: Running on 127.0.0.1:6100. Primary = true...

$ killall node-lock-commit
````

As mentioned in the intro section, hte node_lock_commit can also work as a client CLI if you pass a command:

````
/node_lock_commit --port 6100 set key value!
````

Node outputs:
````
➜ 2022-10-20T18:37:07.115Z INFO [node_lock_commit::node] 127.0.0.1:6100: Received request Client(Set { key: "key", value: "value!" })
2022-10-20T18:37:07.115Z INFO [node_lock_commit::node] Received command, broadcasting Propose
2022-10-20T18:37:07.116Z INFO [node_lock_commit::node] 127.0.0.1:6100: Received request Network(Propose { command_view: CommandView { command: Set { key: "key", value: "value!" }, view: 1 } })
2022-10-20T18:37:07.116Z INFO [node_lock_commit::node] 127.0.0.1:6100: Locked command view CommandView { command: Set { key: "key", value: "value!" }, view: 1 }
2022-10-20T18:37:07.116Z INFO [node_lock_commit::node] 127.0.0.1:6100: View-command locked, sending out Lock message
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] 127.0.0.1:6101: Received request Network(Propose { command_view: CommandView { command: Set { key: "key", value: "value!" }, view: 1 } })
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] 127.0.0.1:6101: Locked command view CommandView { command: Set { key: "key", value: "value!" }, view: 1 }
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] 127.0.0.1:6101: View-command locked, sending out Lock message
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] 127.0.0.1:6100: Received request Network(Lock { socket_addr: 127.0.0.1:6100, command_view: CommandView { command: Set { key: "key", value: "value!" }, view: 1 } })
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] Received lock, did we get quorum? 1 responses so far vs expected quorum of 2
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] 127.0.0.1:6100: Received request Network(Lock { socket_addr: 127.0.0.1:6101, command_view: CommandView { command: Set { key: "key", value: "value!" }, view: 1 } })
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] Received lock, did we get quorum? 2 responses so far vs expected quorum of 2
2022-10-20T18:37:07.117Z INFO [node_lock_commit::node] Quorum achieved, commiting first and sending out Commit message!
2022-10-20T18:37:07.118Z INFO [node_lock_commit::node] 127.0.0.1:6101: Received request Network(Commit { command_view: CommandView { command: Set { key: "key", value: "value!" }, view: 1 } })
2022-10-20T18:37:07.118Z INFO [node_lock_commit::node] about to try commit as a response to Commit message
2022-10-20T18:37:07.118Z INFO [node_lock_commit::node] 127.0.0.1:6100: Received request Network(Commit { command_view: CommandView { command: Set { key: "key", value: "value!" }, view: 1 } })
2022-10-20T18:37:07.118Z INFO [node_lock_commit::node] about to try commit as a response to Commit message
2022-10-20T18:37:07.118Z INFO [node_lock_commit::node] 127.0.0.1:6100: trying to commit CommandView { command: Set { key: "key", value: "value!" }, view: 1 } but we had locked CommandView { command: Get { key: "-" }, view: 0 }
2022-10-20T18:37:07.118Z INFO [node_lock_commit::node] 127.0.0.1:6101: Committed command, response was [118, 97, 108, 117, 101, 33]
````

Afterward you can get the key from any of the nodes
````
./node_lock_commit --port 6100 get key
2022-10-20T18:40:28.761Z INFO [node_lock_commit] value!

./node_lock_commit --port 6101 get key
2022-10-20T18:51:28.761Z INFO [node_lock_commit] value!
````