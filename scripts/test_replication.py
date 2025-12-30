import sys
import os
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))

from infrastructure.node import Node
from infrastructure.comms import Communicator

def get_cluster():
    """Returns a communicator and the current state of all nodes in the cluster."""
    node = Node(1)
    comm = Communicator(node)
    cluster = {}
    for pid in range(1, 6):
        p = {'id': pid, 'ip': '127.0.0.1', 'port': 5000 + pid}
        s = comm.get_state(p)
        if s: cluster[pid] = {'info': p, 'state': s.state, 'log': s.log_length}
    return node, comm, cluster

def test_replication():
    """Verify that commands submitted to the leader are replicated to all followers."""
    print("[INFO] Identifying cluster leader...")
    node, comm, cluster = get_cluster()
    leader = next((c for c in cluster.values() if c['state'] == 'Leader'), None)
    
    if not leader:
        print("[FAIL] No leader found in cluster")
        return False
    
    print(f"[INFO] Leader is Node {leader['info']['id']}. Submitting commands...")
    commands = ["SET X=100", "SET Y=200", "SET Z=300"]
    for cmd in commands:
        res = comm.submit_command(leader['info'], cmd)
        if not res or not res.success:
            print(f"[FAIL] Failed to submit command '{cmd}'")
            return False
        print(f"  Command accepted: {cmd}")
    
    print("[INFO] Waiting for log replication (2 seconds)...")
    time.sleep(2.0)
    
    _, _, cluster = get_cluster()
    print("\nReplication Status:")
    for nid, c in sorted(cluster.items()):
        print(f"  Node {nid}: {c['state']:10s} | Log Length: {c['log']}")
        if c['log'] < 3:
            print(f"[FAIL] Node {nid} has incomplete log (expected >= 3)")
            return False
            
    print("\n[INFO] Verifying data consistency across cluster...")
    for nid, c in sorted(cluster.items()):
        data = comm.get_data(c['info'], 'Y')
        val = data.value if data and data.success else "NOT FOUND"
        print(f"  Node {nid}: Y = {val}")
        if val != '200':
            print(f"[FAIL] Data inconsistency on Node {nid}")
            return False
            
    print("[PASS] Log replication and data consistency verified")
    return True

if __name__ == '__main__':
    sys.exit(0 if test_replication() else 1)
