# Distributed Consensus System

Implementation of **Raft** (Crash Fault Tolerant) and **pBFT** (Byzantine Fault Tolerant) consensus algorithms in Python using gRPC.

## Features

- ✅ **Raft Consensus**: Leader election, log replication, persistence (WAL)
- ✅ **pBFT Consensus**: 3-phase Byzantine fault tolerance
- ✅ **Network Simulation**: Partition testing, failure injection
- ✅ **Comprehensive Tests**: 9 end-to-end test scenarios
- ✅ **Production-Ready**: Atomic WAL writes, parallel RPCs, optimized timeouts

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate gRPC Code

```bash
python -m grpc_tools.protoc -I protos --python_out=src/generated --grpc_python_out=src/generated protos/*.proto
```

### 3. Run Tests

```bash
# Run all tests (Raft + pBFT)
python scripts/test_all.py

# Or run specific test suites
python scripts/test_election.py          # Raft leader election
python scripts/test_replication.py       # Raft log replication
python scripts/test_network_partition.py # Chaos engineering
python scripts/test_pbft.py              # pBFT Byzantine tolerance
```

## Running the Cluster

### Raft Cluster (5 nodes, ports 5001-5005)

```bash
# Start all nodes
python scripts/run_cluster.py

# Or start individual nodes
python src/main.py --id 1
python src/main.py --id 2
# ... etc
```

### pBFT Cluster (4 nodes, ports 6001-6004)

```bash
# Start all nodes
python scripts/run_pbft_cluster.py

# With malicious node for testing
python scripts/run_pbft_cluster.py --malicious 4
```

## Project Structure

```
project_01_source/
├── protos/                    # Protocol Buffer definitions
│   ├── raft.proto            # Raft RPCs (RequestVote, AppendEntries)
│   ├── pbft.proto            # pBFT RPCs (PrePrepare, Prepare, Commit)
│   └── control.proto         # Network simulation (SetPartition)
├── src/
│   ├── consensus/
│   │   ├── raft.py           # Raft implementation (424 lines)
│   │   ├── pbft.py           # pBFT implementation (379 lines)
│   │   └── state_machine.py # Key-value store
│   ├── storage/
│   │   └── wal.py            # Write-Ahead Log for persistence
│   ├── infrastructure/
│   │   ├── node.py           # Node server
│   │   ├── comms.py          # gRPC client wrapper
│   │   └── interceptor.py   # Partition simulation
│   ├── main.py               # Raft node entry point
│   └── pbft_main.py          # pBFT node entry point
└── scripts/
    ├── test_all.py           # Comprehensive test runner
    ├── test_*.py             # Individual test scenarios
    ├── run_cluster.py        # Raft cluster manager
    └── run_pbft_cluster.py   # pBFT cluster manager
```

## Test Coverage

| Category              | Tests                                         | Status       |
| --------------------- | --------------------------------------------- | ------------ |
| **Raft Election**     | Leader election, failover, rejoin, split-vote | ✅ 4/4       |
| **Raft Replication**  | Log replication, WAL persistence              | ✅ 2/2       |
| **Chaos Engineering** | Leader failure, network partition             | ✅ 2/2       |
| **pBFT Byzantine**    | Normal operation, Byzantine tolerance         | ✅ 2/2       |
| **Total**             |                                               | ✅ **10/10** |

## Key Implementation Details

### Raft Optimizations

- **Parallel RequestVote RPCs**: Using `ThreadPoolExecutor` for faster elections
- **Optimized Timeouts**: 300-600ms election, 100ms vote RPC, 50ms heartbeat
- **Atomic WAL Writes**: Temp file + rename for crash safety
- **Background Application**: Separate thread applies committed entries

### pBFT Features

- **SHA-256 Digest Verification**: Prevents tampering
- **View-Based Primary**: Deterministic primary selection
- **Malicious Mode**: `--malicious` flag for Byzantine testing
- **Quorum Tracking**: 2f+1 messages per phase

## Configuration

### Raft Nodes (`nodes_config.json`)

```json
[
  {"id": 1, "ip": "127.0.0.1", "port": 5001},
  {"id": 2, "ip": "127.0.0.1", "port": 5002},
  ...
]
```

### pBFT Nodes (`pbft_nodes_config.json`)

```json
[
  {"id": 1, "ip": "127.0.0.1", "port": 6001},
  {"id": 2, "ip": "127.0.0.1", "port": 6002},
  ...
]
```

## Raft vs pBFT Comparison

| Aspect          | Raft           | pBFT            |
| --------------- | -------------- | --------------- |
| **Fault Model** | Crash (CFT)    | Byzantine (BFT) |
| **Nodes (f=1)** | 3              | 4               |
| **Quorum**      | Majority (2/3) | 2f+1 (3/4)      |
| **Phases**      | 2              | 3               |
| **Latency**     | 1-2 RTT        | 3-4 RTT         |
| **Throughput**  | Higher         | Lower           |
| **Use Case**    | Trusted env    | Untrusted env   |

See [raft_vs_pbft_comparison.md](./raft_vs_pbft_comparison.md) for detailed analysis.

## Troubleshooting

**Import errors**: Regenerate gRPC code (step 2)

**Port conflicts**:

- Raft uses 5001-5005
- pBFT uses 6001-6004
- Check with `netstat -ano | findstr ":5001"`

**Tests hang or fail unexpectedly**: Stale state in the Write-Ahead Log (WAL) can interfere with new test runs. Clean up old data files:

```bash
# On Windows (PowerShell)
Remove-Item wal_data_* -ErrorAction SilentlyContinue

# On Linux/macOS
rm wal_data_*
```

**Node won't start**: Kill existing processes:

```bash
python scripts/restart_cluster.py
```

## License

MIT
