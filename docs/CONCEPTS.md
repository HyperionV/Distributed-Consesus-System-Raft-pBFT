# Core Concepts & Techniques

This document introduces the fundamental techniques and algorithms used in this project to help students understand the implementation.

## 1. Distributed Consensus

In a distributed system, consensus is the process of getting multiple nodes to agree on a single value or a sequence of operations, even if some nodes fail.

### Why do we need it?

To build fault-tolerant systems (like a database that stays up even if one server dies). Without consensus, different servers might think different things are true (e.g., "A=10" vs "A=20"), leading to data corruption.

## 2. Raft (Crash Fault Tolerance - CFT)

Raft is designed to be easy to understand while providing the same safety as older algorithms like Paxos.

### Key Terms:

- **Leader**: The node that handles all client requests.
- **Term**: A "logical clock" (incrementing integer) used to detect stale information.
- **Quorum**: A majority of nodes (e.g., 3 out of 5). No decision can be made without a quorum.
- **Log**: A sequence of commands that the cluster has agreed to execute.

### The Two Rules:

1. **Leader Election**: If a follower doesn't hear from a leader for a while, it becomes a candidate and asks for votes.
2. **Log Replication**: The leader sends new commands to followers. Once a majority acknowledges, the command is "committed".

## 3. pBFT (Byzantine Fault Tolerance - BFT)

Unlike Raft, which assumes nodes are honest but might crash, pBFT assumes some nodes might be **malicious** (Byzantine).

### Fundamental Difference:

- Raft needs **2f + 1** nodes to tolerate **f** crashes.
- pBFT needs **3f + 1** nodes to tolerate **f** malicious actors.

### The Three Phases:

1. **Pre-Prepare**: The primary suggests a command and a sequence number.
2. **Prepare**: Replicas acknowledge the suggestion and ensure they agree with each other.
3. **Commit**: Replicas confirm that enough nodes saw the "Prepare" phase.

## 4. gRPC and Protocol Buffers

gRPC (Google Remote Procedure Call) is used for communication between nodes in this project.

- **RPC (Remote Procedure Call)**: Makes calling a function on another server look like a local function call.
- **Protobuf (Protocol Buffers)**: A way to define the "shape" of messages and services in `.proto` files. It is faster and more efficient than JSON for network communication.

## 5. Write-Ahead Log (WAL)

A WAL is a common technique in databases to ensure durability. Before any data is changed in memory or the main database, the change is first written (appended) to a log on disk. If the server crashes, it can replay the log to restore its state.
