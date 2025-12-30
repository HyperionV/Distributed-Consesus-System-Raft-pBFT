import sys
import os
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))

from infrastructure.node import Node
from infrastructure.comms import Communicator

def get_cluster():
    """Returns state and peer information for all nodes."""
    node = Node(1)
    comm = Communicator(node)
    cluster = {}
    for i in range(1, 6):
        p = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        s = comm.get_state(p)
        if s: cluster[i] = {'info': p, 'state': s.state, 'term': s.term, 'log': s.log_length}
    return node, comm, cluster

def test_partition():
    """Verify Raft safety during a 3-2 network partition (split-brain)."""
    node, comm, cluster = get_cluster()
    lid = next((nid for nid, c in cluster.items() if c['state'] == 'Leader'), None)
    if not lid:
        print("[FAIL] Initial leader not found")
        return False
    print(f"[INFO] Initial leader: Node {lid}")
    
    # Majority: any 3 nodes NOT including the old leader
    # Minority: old leader + 1 other node
    minority = [lid, 1 if lid != 1 else 2]
    majority = [i for i in range(1, 6) if i not in minority]
    print(f"[ACTION] Creating partition: Majority {majority} VS Minority {minority}")
    
    for i in majority:
        p = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        comm.set_partition(p, blocked_node_ids=minority)
    for i in minority:
        p = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        comm.set_partition(p, blocked_node_ids=majority)
    
    print("[INFO] Waiting for re-election in majority partition (4 seconds)...")
    time.sleep(4)
    
    _, _, cluster = get_cluster()
    new_lid = next((nid for nid in majority if cluster.get(nid, {}).get('state') == 'Leader'), None)
    if not new_lid:
        print("[FAIL] No new leader elected in majority partition")
        return False
    print(f"[PASS] New leader elected: Node {new_lid}")
    
    # Prove majority can commit
    print("[ACTION] Submitting command to new leader...")
    comm.submit_command(cluster[new_lid]['info'], "SET PartitionTest=Success")
    
    print("[ACTION] Healing network partition...")
    for i in range(1, 6):
        comm.set_partition({'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}, blocked_node_ids=[])
    
    print("[INFO] Waiting for log convergence (4 seconds)...")
    time.sleep(4)
    
    _, _, cluster = get_cluster()
    logs = [c['log'] for c in cluster.values()]
    print(f"  Log lengths: {logs}")
    if len(set(logs)) == 1:
        print("[PASS] All node logs converged after partition healed")
        return True
    
    print("[FAIL] Cluster logs divergent after healing")
    return False

if __name__ == '__main__':
    sys.exit(0 if test_partition() else 1)
