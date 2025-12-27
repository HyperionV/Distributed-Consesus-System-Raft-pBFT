# Testing & Verification Guide

This project includes a comprehensive test suite in the `scripts/` directory to verify the correctness and robustness of the consensus implementations.

## 1. Test Overview

### `test_all.py` (The Master Runner)

Run this to execute the entire test suite sequentially. It manages node lifecycles between tests.

### Raft Election Suite

- **`test_election.py`**: Verifies a single leader is elected correctly.
- **`test_automated_failover.py`**: Kills the leader and ensures a new one is elected automatically.
- **`test_split_vote.py`**: Restarts nodes rapidly to trigger split-vote scenarios and verifies the cluster recovers.
- **`test_node_rejoin.py`**: Verifies that a node can crash, restart, and rejoin the cluster correctly.

### Raft Log & Persistence Suite

- **`test_replication.py`**: Submits commands to the leader and verifies they replicate to all followers.
- **`test_persistence.py`**: Verifies that after a crash, the WAL restores the node's term and log correctly.

### Chaos Engineering Suite

- **`test_leader_failure.py`**: Specifically tests data survival when the leader node is killed.
- **`test_network_partition.py`**: Simulates a "Split Brain" by partitioning nodes into two groups (3-2). It verifies that only the majority can commit and that the minority heals when the partition is removed.

### pBFT Suite

- **`test_pbft.py`**: Tests normal pBFT consensus and Byzantine tolerance (3 honest nodes + 1 malicious node).

## 2. How to Run Tests

### Prerequisites

1. Ensure dependencies are installed: `pip install -r requirements.txt`
2. Generate gRPC code: `python -m grpc_tools.protoc -I protos --python_out=src/generated --grpc_python_out=src/generated protos/*.proto`

### Running the Whole Suite

```bash
python scripts/test_all.py
```

### Troubleshooting Test Failures

- **Stale State**: If tests fail after a previous crash, delete the WAL files: `rm wal_data_*` or `Remove-Item wal_data_*`.
- **Hanging Tests**: Ensure all previous node processes are killed using `python scripts/restart_cluster.py --stop-only`.

### Running Individual Tests

Most tests are self-contained but require the cluster to be clean. It is recommended to run:

```bash
# To clean up any hung processes
python scripts/restart_cluster.py --stop-only

# Run your test
python scripts/test_replication.py
```

## 3. Interpreting Logs

When a test runs, look for these markers:

- `✓ PASS`: The scenario was handled correctly.
- `✗ FAIL`: An assertion was not met (e.g., no leader elected).
- `[Node X]`: Logs prefixed with the Node ID show the internal consensus state (e.g., `Leader`, `Follower`, `term=5`).
- `WAL loaded`: Indicates persistence is working after a restart.
