## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate gRPC Code

```bash
# Generate protobuf files
python -m grpc_tools.protoc -I protos --python_out=src/generated --grpc_python_out=src/generated protos/*.proto

# Fix imports (required - converts bare imports to package imports)
python src/fix_imports.py
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

## Troubleshooting

**Import errors** (`ModuleNotFoundError: No module named 'raft_pb2'` or `ImportError: attempted relative import`):

```bash
# Regenerate protobuf files and fix imports
python -m grpc_tools.protoc -I protos --python_out=src/generated --grpc_python_out=src/generated protos/*.proto
python src/fix_imports.py
```

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
