"""
Automated Leader Failover Test
Programmatically kills the leader and verifies re-election.
"""
import sys
import os
import time
import subprocess
import psutil

# Add src directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, src_dir)

from infrastructure.node import Node
from infrastructure.comms import Communicator

def get_cluster_state():
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    for i in range(1, 6):
        peer = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        try:
            res = comm.get_state(peer)
            if res:
                states[i] = {'state': res.state, 'term': res.term}
            else:
                states[i] = {'state': 'OFFLINE'}
        except:
            states[i] = {'state': 'ERROR'}
    return states

def find_leader():
    states = get_cluster_state()
    for node_id, info in states.items():
        if info['state'] == 'Leader':
            return node_id, info['term']
    return None, None

def kill_node(node_id):
    print(f"  Killing Node {node_id}...")
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in cmd[0].lower() and 'src/main.py' in ' '.join(cmd) and f'--id {node_id}' in ' '.join(cmd):
                proc.kill()
                return True
        except:
            pass
    return False

def test_automated_failover():
    print("="*60)
    print("AUTOMATED LEADER FAILOVER TEST")
    print("="*60)

    # 1. Find leader
    leader_id, term = find_leader()
    if not leader_id:
        print("✗ FAIL: No initial leader found")
        return False
    print(f"✓ Initial leader: Node {leader_id} (term {term})")

    # 2. Kill leader
    if not kill_node(leader_id):
        print(f"✗ FAIL: Could not kill leader Node {leader_id}")
        return False

    # 3. Wait for re-election
    print("  Waiting for re-election (1.5s)...")
    time.sleep(1.5)

    # 4. Verify new leader
    new_leader_id, new_term = find_leader()
    if not new_leader_id:
        print("✗ FAIL: No new leader elected after failure")
        return False
    
    print(f"✓ New leader elected: Node {new_leader_id} (term {new_term})")
    
    if new_leader_id == leader_id:
        print("✗ FAIL: Old leader still reported as leader?")
        return False
        
    if new_term <= term:
        print(f"✗ FAIL: Term did not increment (old: {term}, new: {new_term})")
        return False

    print("\n✓ FAILOVER TEST PASSED")
    print("="*60)
    return True

if __name__ == '__main__':
    success = test_automated_failover()
    sys.exit(0 if success else 1)
