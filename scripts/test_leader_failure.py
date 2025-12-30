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

def get_leader():
    """Find and return the current leader's info and term."""
    node = Node(1)
    comm = Communicator(node)
    for i in range(1, 6):
        p = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        s = comm.get_state(p)
        if s and s.state == 'Leader': return i, p, s.term
    return None, None, 0

def test_leader_failure():
    """Verify that a new leader is elected after the current leader crashes."""
    lid, lp, term = get_leader()
    if not lid:
        print("[FAIL] Initial leader not found")
        return False
    
    print(f"[INFO] Initial leader: Node {lid} (term {term})")
    comm = Communicator(Node(1))
    comm.submit_command(lp, "SET status=alive")
    time.sleep(1)
    
    print(f"[ACTION] Killing leader Node {lid}...")
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'src/main.py' in ' '.join(cmd) and f'--id {lid}' in ' '.join(cmd):
                proc.kill()
                print(f"  Process {proc.pid} terminated.")
        except: pass
        
    print("[INFO] Waiting for re-election (3 seconds)...")
    time.sleep(3)
    
    new_lid, new_lp, new_term = get_leader()
    if not new_lid or new_lid == lid:
        print("[FAIL] No new leader elected after failure")
        return False
    
    print(f"[PASS] New leader elected: Node {new_lid} (term {new_term})")
    
    data = comm.get_data(new_lp, "status")
    print(f"  Data recovery check: status = {data.value if data else 'None'}")
    if not data or data.value != 'alive':
        print("[FAIL] Committed data lost during failover")
        return False
        
    print(f"[ACTION] Restarting Node {lid}...")
    subprocess.Popen([sys.executable, 'src/main.py', '--id', str(lid)], cwd=project_root)
    time.sleep(2)
    
    p_old = {'id': lid, 'ip': '127.0.0.1', 'port': 5000 + lid}
    s_old = comm.get_state(p_old)
    if s_old and s_old.state == 'Follower':
        print(f"[PASS] Node {lid} successfully rejoined as Follower")
        return True
    
    print(f"[FAIL] Node {lid} state after restart: {s_old.state if s_old else 'OFFLINE'}")
    return False

if __name__ == '__main__':
    sys.exit(0 if test_leader_failure() else 1)
