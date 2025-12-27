# System Components Overview

This document describes the various components implemented in the Distributed Consensus System, explaining their roles and technical details.

## 1. Core Consensus Modules

### RaftConsensus (`src/consensus/raft.py`)

The heart of the Raft implementation. It implements the state machine for a Raft node.

- **Roles**: Manages terms, votes, log appending, and commitment.
- **Technical Aspects**:
  - Uses a background thread for election timeouts.
  - Uses `ThreadPoolExecutor` for parallel RPCs (faster elections).
  - Implements the "Leader", "Follower", and "Candidate" states.
  - Handles `RequestVote` and `AppendEntries` RPC logic.

### PBFTConsensus (`src/consensus/pbft.py`)

Implements the Practical Byzantine Fault Tolerance algorithm.

- **Roles**: Ensures consensus even if some nodes are malicious.
- **Technical Aspects**:
  - Implements the three-phase protocol: **Pre-Prepare**, **Prepare**, and **Commit**.
  - Uses SHA-256 digests to verify message integrity.
  - Supports a "Malicious" mode to simulate Byzantine behavior.
  - Handles view changes when the primary node fails.

## 2. Infrastructure & Communication

### Node (`src/infrastructure/node.py`)

The main entry point for a running process.

- **Roles**: Hosts the gRPC server and wires up the consensus modules.
- **Technical Aspects**:
  - Loads configuration from `nodes_config.json`.
  - Initializes both the Raft and pBFT service stubs.
  - Manages the server lifecycle (start/stop).

### Communicator (`src/infrastructure/comms.py`)

A wrapper around gRPC client stubs.

- **Roles**: Simplifies sending RPCs to other nodes.
- **Technical Aspects**:
  - Handles connection management (channels).
  - Implements **Network Partition Simulation** through the `_is_blocked` check.
  - Provides a clean API for `ping`, `request_vote`, `append_entries`, and pBFT phases.

## 3. Storage & State

### StateMachine (`src/consensus/state_machine.py`)

A thread-safe key-value store.

- **Roles**: Holds the actual "data" that consensus is reached upon.
- **Technical Aspects**:
  - Supports `SET`, `DELETE`, and `GET` operations.
  - Thread-safe using `threading.Lock`.

### WAL - Write-Ahead Log (`src/storage/wal.py`)

The persistence layer for Raft.

- **Roles**: Ensures that if a node crashes, it can restore its term, vote, and log.
- **Technical Aspects**:
  - Uses atomic file writes (write to temp file then rename).
  - Serializes state to JSON for simplicity in this project.

## 4. Network Simulation

### SetPartition (`control.proto`)

A special RPC used for testing chaos scenarios.

- **Roles**: Instructs a node to "ignore" traffic from specific node IDs or IPs.
- **Technical Aspects**:
  - Updates the `blocked_node_ids` set in the `Node`.
  - The `Communicator` checks this set before sending any RPC.
