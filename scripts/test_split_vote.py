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

def get_cluster_state():
    """Returns the current state of all nodes that are reachable."""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    for i in range(1, 6):
        peer = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        res = comm.get_state(peer)
        if res: states[i] = res.state
    return states

def kill_all():
    """Terminate all Raft node processes."""
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'src/main.py' in ' '.join(cmd): proc.kill()
        except: pass

def test_split_vote():
    """Stress test election resolution by restarting the entire cluster multiple times."""
    print("[INFO] Starting split vote recovery stress test...")
    for iter_idx in range(1, 3):
        print(f"\n--- Iteration {iter_idx}: Rapid Cluster Restart ---")
        kill_all()
        time.sleep(1)
        
        print("  Launching all nodes simultaneously...")
        for i in range(1, 6):
            subprocess.Popen([sys.executable, 'src/main.py', '--id', str(i)], cwd=project_root)
        
        print("  Waiting for election resolution (3 seconds)...")
        time.sleep(3.0)
        
        states = get_cluster_state()
        leaders = [nid for nid, st in states.items() if st == 'Leader']
        print(f"  Nodes Online: {len(states)}, Leaders: {len(leaders)} {leaders}")
        
        if len(leaders) != 1:
            print("[FAIL] Cluster failed to resolve to a single leader")
            return False
            
    print("\n[PASS] Cluster resolved split votes correctly in all iterations")
    return True

if __name__ == '__main__':
    sys.exit(0 if test_split_vote() else 1)
