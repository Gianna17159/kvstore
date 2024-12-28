Concurrent and Distributed Store
====================

This project implements two kinds of key-value stores, one supporting concurrent client requests, and the other extending the former implementation to support a distributed system. Servers are sharded by the first character of a key. The shard controller determines the shards and stores which servers are responsible for each shard. The server periodically queries the shard controller to determine if any of its shards have been move to another server, and if so, moves the key-value pairs for that shard to the new server. The client queries the shard controller to determine which server(s) have the key(s) relevant to its request, then directs its request to those server(s).


COMPILE AND RUN

To compile, cd into the build/ directory and run make, creating four executables: shardcontroller, simple_client, client, and server.


*Each of the following commands need to be run in separate terminals*

IF USING SIMPLE CLIENT (simple concurrent system)

To run the server, specify the port and optionally the number of workers:

    ./server <port> [n_workers]

To run the simple client, specify the server address:

    ./simple_client <server hostname:port>


IF USING CLIENT (distributed system w/shards)

To run a shardcontroller, specify the port:

    ./shardcontroller <port>

To run the server, specify the port and optionally the number of workers:

    ./server <port> [n_workers]

To run the client, specify the server address:

    ./client <server hostname:port>


USING THE CLIENT

Once connected to the server, you can use one or more clients to run standard key-value store operations, like get, put, append, and delete. You can also use two additional requests, multiget and multiput, for multiple key-value pairs. If using the distributed system implementation of client, you can also use two additional requests: query and move. For specific usage instructions, type help in the command prompt.


USING THE SERVER

Upon starting, the server automatically sends a join request to the shardcontroller, and sends a leave request upon shutting down. To leave the shardcontroller configuration without closing a server terminal, use the leave command in the terminal. To rejoin the configuration, use the join command in the terminal.


To exit the client and any other executable, press Ctrl-D or Cmd-D.

To run tests, you must be in the build/ directory. Then, run make check.