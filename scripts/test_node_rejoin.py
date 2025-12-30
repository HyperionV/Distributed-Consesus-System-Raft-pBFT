import sys
import os
import time
import subprocess
import psutil

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))

from infrastructure.node import Node
from infrastructure.comms import Communicator

def get_node_state(nid):
    """Retrieve the current state of a node via gRPC."""
    node = Node(1)
    comm = Communicator(node)
    p = {'id': nid, 'ip': '127.0.0.1', 'port': 5000 + nid}
    res = comm.get_state(p)
    return (res.state, res.term) if res else ("OFFLINE", -1)

def kill_node(nid):
    """Terminate the process of a specific cluster node."""
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'src/main.py' in ' '.join(cmd) and f'--id {nid}' in ' '.join(cmd):
                proc.kill()
                return True
        except: pass
    return False

def test_node_rejoin():
    """Test that a failed node successfully synchronizes with the new leader upon restarting."""
    print("[INFO] Locating leader...")
    l_id = next((i for i in range(1, 6) if get_node_state(i)[0] == 'Leader'), None)
    if not l_id:
        print("[FAIL] Cluster has no leader")
        return False
    print(f"  Leader is Node {l_id}")
    
    print(f"[ACTION] Killing leader Node {l_id}...")
    kill_node(l_id)
    print("  Waiting for re-election...")
    time.sleep(2.0)
    
    new_info = [(i, get_node_state(i)) for i in range(1, 6)]
    new_l_id = next((i for i, s in new_info if s[0] == 'Leader'), None)
    if not new_l_id:
        print("[FAIL] Re-election failed")
        return False
    new_term = next(s[1] for i, s in new_info if i == new_l_id)
    print(f"  New leader is Node {new_l_id} (term {new_term})")
    
    print(f"[ACTION] Restarting former leader Node {l_id}...")
    subprocess.Popen([sys.executable, 'src/main.py', '--id', str(l_id)], cwd=project_root)
    time.sleep(3.0)
    
    state, term = get_node_state(l_id)
    print(f"  Node {l_id} state after restart: {state} (term {term})")
    
    if state == 'Follower' and term == new_term:
        print("[PASS] Node rejoined as Follower and synchronized term")
        return True
        
    print(f"[FAIL] Node {l_id} failed to sync (expected Follower, term {new_term})")
    return False

if __name__ == '__main__':
    sys.exit(0 if test_node_rejoin() else 1)
