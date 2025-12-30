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

def get_node_info(nid):
    """Retrieve state and term for a specific node."""
    node = Node(1)
    comm = Communicator(node)
    p = {'id': nid, 'ip': '127.0.0.1', 'port': 5000 + nid}
    s = comm.get_state(p)
    return p, s

def test_persistence():
    """Verify that a node restores its term and log content from the WAL after a restart."""
    print("[INFO] Submitting data to be persisted...")
    comm = Communicator(Node(1))
    # Find leader
    lid = None
    for i in range(1, 6):
        _, s = get_node_info(i)
        if s and s.state == 'Leader':
            lid = i
            break
            
    if not lid:
        print("[FAIL] No leader to submit data to")
        return False
        
    lp, ls = get_node_info(lid)
    for i in range(3):
        comm.submit_command(lp, f"SET PersistentKey{i}=Value{i}")
    
    time.sleep(2)
    _, s_before = get_node_info(lid)
    print(f"  State before crash: term={s_before.term}, log_len={s_before.log_length}")
    
    print(f"[ACTION] Killing leader Node {lid} to test persistence...")
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'src/main.py' in ' '.join(cmd) and f'--id {lid}' in ' '.join(cmd):
                proc.kill()
        except: pass
        
    time.sleep(1)
    print(f"[ACTION] Restarting Node {lid}...")
    subprocess.Popen([sys.executable, 'src/main.py', '--id', str(lid)], cwd=project_root)
    time.sleep(3)
    
    _, s_after = get_node_info(lid)
    if not s_after:
        print(f"[FAIL] Node {lid} did not recover after restart")
        return False
        
    print(f"  State after restart: term={s_after.term}, log_len={s_after.log_length}")
    
    if s_after.log_length >= s_before.log_length and s_after.term >= s_before.term:
        print("[PASS] State successfully recovered from WAL")
        return True
    
    print("[FAIL] State regression detected after recovery")
    return False

if __name__ == '__main__':
    sys.exit(0 if test_persistence() else 1)
