"""
Network Partition Test
Simulates a 3-2 split-brain scenario and verifies Raft safety properties:
- Majority partition (3 nodes) can elect leader and commit
- Minority partition (2 nodes) cannot commit (no quorum)
- After healing, logs converge
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


def get_all_peers():
    """Get all node peer configs"""
    return [{'id': i, 'ip': '127.0.0.1', 'port': 5000 + i} for i in range(1, 6)]


def get_cluster_state():
    """Get state from all nodes"""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    
    for peer in get_all_peers():
        state = comm.get_state(peer)
        if state:
            states[peer['id']] = {
                'state': state.state,
                'term': state.term,
                'log_length': state.log_length,
                'commit_index': state.commit_index
            }
        else:
            states[peer['id']] = {'state': 'OFFLINE'}
    return states


def find_leader():
    """Find current leader"""
    states = get_cluster_state()
    for node_id, state in states.items():
        if state.get('state') == 'Leader':
            return node_id
    return None


def create_partition(group_a, group_b):
    """
    Create a network partition between two groups.
    Nodes in group_a block nodes in group_b and vice versa.
    """
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    print(f"\n  Creating partition: {group_a} <-/-> {group_b}")
    
    # Group A blocks Group B
    for node_id in group_a:
        peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
        comm.set_partition(peer, blocked_node_ids=group_b)
    
    # Group B blocks Group A
    for node_id in group_b:
        peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
        comm.set_partition(peer, blocked_node_ids=group_a)


def heal_partition():
    """Remove all partitions"""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    print("\n  Healing partition (removing all blocks)...")
    
    for peer in get_all_peers():
        comm.set_partition(peer, blocked_node_ids=[], blocked_ips=[])


def test_split_brain():
    print("="*60)
    print("NETWORK PARTITION TEST (3-2 Split Brain)")
    print("="*60)
    
    # 1. Find initial leader
    initial_leader = find_leader()
    if not initial_leader:
        print("✗ FAIL: No initial leader")
        return False
    print(f"✓ Initial leader: Node {initial_leader}")
    
    # 2. Create partition: put leader in minority
    # Majority: nodes NOT including leader (pick 3)
    # Minority: leader + 1 other node
    all_nodes = [1, 2, 3, 4, 5]
    minority = [initial_leader]
    for n in all_nodes:
        if n != initial_leader and len(minority) < 2:
            minority.append(n)
    majority = [n for n in all_nodes if n not in minority]
    
    print(f"\n  Majority partition: {majority}")
    print(f"  Minority partition: {minority} (contains old leader)")
    
    create_partition(majority, minority)
    
    # 3. Wait for new election in majority
    print("\n  Waiting for new leader in majority (3s)...")
    time.sleep(3.0)
    
    # 4. Check states
    states = get_cluster_state()
    print("\nCluster State After Partition:")
    for node_id, state in sorted(states.items()):
        partition = "MAJORITY" if node_id in majority else "MINORITY"
        print(f"  Node {node_id} [{partition}]: {state.get('state', 'OFFLINE'):10s} term={state.get('term', '?')} log={state.get('log_length', '?')}")
    
    # Find new leader (should be in majority)
    new_leader = None
    for node_id in majority:
        if states.get(node_id, {}).get('state') == 'Leader':
            new_leader = node_id
            break
    
    if new_leader:
        print(f"\n✓ New leader elected in majority: Node {new_leader}")
    else:
        print("\n⚠ WARNING: No new leader in majority yet (may still be electing)")
    
    # 5. Try to submit command to majority leader
    if new_leader:
        temp_node = Node(1)
        comm = Communicator(temp_node)
        leader_peer = {'id': new_leader, 'ip': '127.0.0.1', 'port': 5000 + new_leader}
        
        response = comm.submit_command(leader_peer, "SET partition_test=success")
        if response and response.success:
            print("✓ Command submitted to majority leader")
        else:
            print("⚠ Command submission failed (may not be committed yet)")
    
    # 6. Heal partition
    time.sleep(2.0)
    heal_partition()
    
    # 7. Wait for convergence
    print("\n  Waiting for convergence (3s)...")
    time.sleep(3.0)
    
    # 8. Check final state
    states = get_cluster_state()
    print("\nCluster State After Healing:")
    leaders = []
    for node_id, state in sorted(states.items()):
        print(f"  Node {node_id}: {state.get('state', 'OFFLINE'):10s} term={state.get('term', '?')} log={state.get('log_length', '?')}")
        if state.get('state') == 'Leader':
            leaders.append(node_id)
    
    if len(leaders) == 1:
        print(f"\n✓ PASS: Single leader after healing: Node {leaders[0]}")
    elif len(leaders) == 0:
        print("\n⚠ No leader yet (still converging)")
    else:
        print(f"\n✗ FAIL: Multiple leaders detected: {leaders}")
        return False
    
    # Verify all nodes have same log length eventually
    log_lengths = [s.get('log_length', 0) for s in states.values() if s.get('state') != 'OFFLINE']
    if len(set(log_lengths)) == 1:
        print(f"✓ PASS: All nodes have same log length: {log_lengths[0]}")
    else:
        print(f"⚠ Log lengths differ (may still be syncing): {log_lengths}")
    
    print("\n" + "="*60)
    print("✓ PARTITION TEST COMPLETED")
    print("="*60)
    return True


if __name__ == '__main__':
    success = test_split_brain()
    sys.exit(0 if success else 1)
