import sys
import os
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))

from infrastructure.node import Node
from infrastructure.comms import Communicator

def get_cluster_state():
    """Query all running nodes via RPC and return their current consensus states."""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    for pid in range(1, 6):
        peer = {'id': pid, 'ip': '127.0.0.1', 'port': 5000 + pid}
        try:
            res = comm.get_state(peer)
            if res:
                states[pid] = {'state': res.state, 'term': res.term}
            else:
                states[pid] = {'state': 'OFFLINE', 'term': -1}
        except:
            states[pid] = {'state': 'ERROR', 'term': -1}
    return states

def test_election():
    """Verify that a single leader is elected and maintained in a stable cluster."""
    print("[INFO] Waiting for election to complete...")
    time.sleep(1.0)
    
    states = get_cluster_state()
    leaders = [nid for nid, s in states.items() if s['state'] == 'Leader']
    
    print("\nCluster State:")
    for nid, s in sorted(states.items()):
        print(f"  Node {nid}: {s['state']:10s} (term {s['term']})")
    
    if len(leaders) != 1:
        print(f"[FAIL] Expected exactly 1 leader, but found {len(leaders)}")
        return False
    
    l_id = leaders[0]
    print(f"[PASS] Leader elected: Node {l_id}")
    
    print("[INFO] Verifying leader stability (2 seconds)...")
    time.sleep(2.0)
    
    states2 = get_cluster_state()
    leaders2 = [nid for nid, s in states2.items() if s['state'] == 'Leader']
    
    if len(leaders2) == 1 and leaders2[0] == l_id:
        print(f"[PASS] Leader stable: Node {l_id}")
        return True
    
    print(f"[FAIL] Leader changed or split (New leaders: {leaders2})")
    return False

if __name__ == '__main__':
    sys.exit(0 if test_election() else 1)
