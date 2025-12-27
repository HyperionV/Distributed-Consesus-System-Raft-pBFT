"""
Test Raft leader election
Verifies that exactly one leader is elected and remains stable
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

def get_cluster_state():
    """Query all running nodes via RPC and return their states"""
    # Create a temporary node just for communication
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    states = {}
    
    # Query all 5 nodes via GetState RPC
    for peer_id in range(1, 6):
        # Find the peer config
        if peer_id == 1:
            # Query self (node 1)
            peer = {'id': 1, 'ip': '127.0.0.1', 'port': 5001}
        else:
            peer = next((p for p in temp_node.peers if p['id'] == peer_id), None)
            if not peer:
                peer = {'id': peer_id, 'ip': '127.0.0.1', 'port': 5000 + peer_id}
        
        try:
            response = comm.get_state(peer)
            if response:
                states[peer_id] = {
                    'state': response.state,
                    'term': response.term,
                    'voted_for': None  # Not exposed in GetState
                }
            else:
                states[peer_id] = {'state': 'OFFLINE', 'term': -1}
        except Exception as e:
            states[peer_id] = {'state': 'ERROR', 'term': -1, 'error': str(e)}
    
    return states

def count_states(states):
    """Count leaders, candidates, followers"""
    leaders = sum(1 for s in states.values() if s['state'] == 'Leader')
    candidates = sum(1 for s in states.values() if s['state'] == 'Candidate')
    followers = sum(1 for s in states.values() if s['state'] == 'Follower')
    return leaders, candidates, followers

def test_election():
    print("="*60)
    print("RAFT LEADER ELECTION TEST")
    print("="*60)
    
    print("\nWaiting for election to complete (500ms)...")
    time.sleep(0.5)
    
    # Test 1: Exactly one leader
    print("\n--- Test 1: Leader Election ---")
    states = get_cluster_state()
    leaders, candidates, followers = count_states(states)
    
    print(f"\nCluster State:")
    for node_id, state in sorted(states.items()):
        if 'error' in state:
            print(f"  Node {node_id}: {state['state']:10s} - {state.get('error', '')}")
        else:
            print(f"  Node {node_id}: {state['state']:10s} (term {state['term']})")
    
    print(f"\nSummary: {leaders} Leader(s), {candidates} Candidate(s), {followers} Follower(s)")
    
    if leaders == 1:
        print("✓ PASS: Exactly 1 leader elected")
    else:
        print(f"✗ FAIL: Expected 1 leader, got {leaders}")
        return False
    
    if followers == 4:
        print("✓ PASS: 4 followers present")
    else:
        print(f"⚠ WARNING: Expected 4 followers, got {followers}")
    
    # Test 2: Leader stability
    print("\n--- Test 2: Leader Stability ---")
    leader_id = next(nid for nid, s in states.items() if s['state'] == 'Leader')
    print(f"Current leader: Node {leader_id}")
    
    print("Waiting 2 seconds to verify stability...")
    time.sleep(2.0)
    
    states2 = get_cluster_state()
    leaders2, _, _ = count_states(states2)
    leader_id2 = next((nid for nid, s in states2.items() if s['state'] == 'Leader'), None)
    
    if leaders2 == 1 and leader_id2 == leader_id:
        print(f"✓ PASS: Leader stable (still Node {leader_id})")
    else:
        print(f"✗ FAIL: Leader changed or multiple leaders")
        return False
    
    print("\n" + "="*60)
    print("✓ ALL ELECTION TESTS PASSED")
    print("="*60)
    return True

if __name__ == '__main__':
    success = test_election()
    sys.exit(0 if success else 1)
