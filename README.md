kv_store
========

My Cloud Computing Project
The project has been done in multiple stages though assignments. Each assignment in the repository is a build-up over the previous one.

INSTALL
========
go get github.com/nileshjagnik/kv_store

Stage 1
========
Contains a document with a statement of "Why I got an AA grade". The idea is to set up a healthy expectation of myself. Let us see how well I am able live up to my own expectations :)

Stage 2
========
cluster.go - Contains an implementation of multiple nodes that communicate with each other. The communication has been simulated by creating threads for each server node and communicating between them. ZeroMQ library's PUSH/PULL protocol has been used to achieve this goal.
cluster_test.go - Contains a test routine for the above implementation to get a clear idea of how well the cluster system is working. It creates two threads - one that sends messages in a predefined manner and the second which accepts them. In the end the number of messages that are send from each node to each other is calculated to check if all the values are as expected.

Stage 3
========
[In this part](asgn3), I have started to implement RAFT system and have implemented the leader election part. To know more about raft system read :
In Search of an Understandable Consensus Algorithm, Diego Ongaro and John Ousterhout, 2013

raft.go - leader election has been implemented. Two functions Start() and Close() have been implemented. Close() will effectively exit the goroutine that handles RPCs hence simulating the real-time behaviour of a crashed server. Start() will restart the goroutine.

raft_test.go - contains a test routine that checks for the presence of leader under various conditions. It closes and starts the servers several times for checking whether the raftServer behaves as expected
