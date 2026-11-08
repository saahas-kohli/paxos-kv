# CS 134 KV Store
Starter code for CS 134's assignments 2, 3, and 4.

## Overview of files

Assignment 2 uses primary/backup replication, assisted by a view service that decides which machines are alive. The view service allows the primary/backup service to work correctly in the presence of network partitions. The view service itself is not replicated, and is a single point of failure. You will need to edit the files in the `viewservice` and `pbservice` folders.

Assignment 3 uses the Paxos protocol to replicate the key/value database with no single point of failure, and handles network partitions correctly. This key/value service is slower than a non-replicated key/value server would be, but is fault tolerant. You will need to edit the files in the `paxos` and `kvpaxos` folder.

Assignment 4 is a sharded key/value database, where each shard replicates its state using Paxos. This key/value service can perform Put/Get operations in parallel on different shards, allowing it to support applications such as MapReduce that can put a high load on a storage system. Assignment 4 also has a replicated configuration service, which tells the shards what key range they are responsible for. It can change the assignment of keys to shards, for example, in response to changing load. Assignment 4 has the core of a real-world design for thousands of servers. You will be reusing the code you wrote for paxos in the `paxos` folder from assignment 3, and adding code in the `shardmaster` and `shardkv` folders.

## Using this repository

You should have been added to this GitHub repository alongside your teammate, if any. Edit the code in whatever manner is easiest for you, and when ready to submit, in Gradescope, select the "Submit from GitHub" option, link your GitHub account, and then submit your repository.
