"""
Test Log Replication
Verifies that commands sent to the leader are replicated to all followers
"""
import sys
import os
import time

# Add src directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, src_dir)

from infrastructure.node import Node
from infrastructure.comms import Communicator


def find_leader():
    """Find the current leader in the cluster"""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    for node_id in range(1, 6):
        peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
        state = comm.get_state(peer)
        if state and state.state == 'Leader':
            return node_id, peer
    return None, None


def get_all_states():
    """Get state from all nodes"""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    
    for node_id in range(1, 6):
        peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
        state = comm.get_state(peer)
        if state:
            states[node_id] = {
                'state': state.state,
                'term': state.term,
                'log_length': state.log_length,
                'commit_index': state.commit_index
            }
    return states


def test_basic_replication():
    print("="*60)
    print("LOG REPLICATION TEST")
    print("="*60)
    
    # 1. Find leader
    leader_id, leader_peer = find_leader()
    if not leader_id:
        print("✗ FAIL: No leader found")
        return False
    print(f"✓ Leader found: Node {leader_id}")
    
    # 2. Submit a command
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    command = "SET A=10"
    print(f"  Submitting command: {command}")
    
    response = comm.submit_command(leader_peer, command)
    if not response or not response.success:
        print(f"✗ FAIL: Command submission failed: {response.message if response else 'No response'}")
        return False
    print(f"✓ Command accepted: {response.message}")
    
    # 3. Wait for replication
    print("  Waiting for replication (2s)...")
    time.sleep(2.0)
    
    # 4. Check all nodes have the log entry
    states = get_all_states()
    print("\nCluster State:")
    for node_id, state in sorted(states.items()):
        print(f"  Node {node_id}: {state['state']:10s} | log={state['log_length']} | commit={state['commit_index']}")
    
    # All nodes should have log_length >= 1
    all_replicated = all(s['log_length'] >= 1 for s in states.values())
    if not all_replicated:
        print("✗ FAIL: Not all nodes have the log entry")
        return False
    print("✓ All nodes have the log entry")
    
    # 5. Verify data is readable from all nodes
    print("\nVerifying data consistency...")
    for node_id in range(1, 6):
        peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
        data = comm.get_data(peer, 'A')
        if data and data.success:
            print(f"  Node {node_id}: A = {data.value}")
        else:
            print(f"  Node {node_id}: A = NOT FOUND (may not be committed yet)")
    
    print("\n" + "="*60)
    print("✓ REPLICATION TEST PASSED")
    print("="*60)
    return True


def test_multiple_commands():
    print("\n" + "="*60)
    print("MULTIPLE COMMANDS TEST")
    print("="*60)
    
    leader_id, leader_peer = find_leader()
    if not leader_id:
        print("✗ FAIL: No leader found")
        return False
    
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    commands = ["SET X=100", "SET Y=200", "SET Z=300"]
    for cmd in commands:
        response = comm.submit_command(leader_peer, cmd)
        if response and response.success:
            print(f"✓ Submitted: {cmd}")
        else:
            print(f"✗ Failed: {cmd}")
            return False
    
    time.sleep(2.0)
    
    states = get_all_states()
    expected_log_len = min(s['log_length'] for s in states.values())
    print(f"\nAll nodes have at least {expected_log_len} log entries")
    
    if expected_log_len >= 3:
        print("✓ MULTIPLE COMMANDS TEST PASSED")
        return True
    else:
        print("✗ FAIL: Not all commands replicated")
        return False


if __name__ == '__main__':
    success = test_basic_replication()
    if success:
        success = test_multiple_commands()
    sys.exit(0 if success else 1)
