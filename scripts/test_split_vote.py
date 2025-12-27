"""
Split Vote Recovery Stress Test
Restarts multiple nodes rapidly to check if they can resolve split votes via randomized timeouts.
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
            if res: states[i] = res.state
        except: pass
    return states

def kill_all_nodes():
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in cmd[0].lower() and 'src/main.py' in ' '.join(cmd):
                proc.kill()
        except: pass

def start_node(i):
    return subprocess.Popen([sys.executable, 'src/main.py', '--id', str(i)], 
                            cwd=project_root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def test_split_vote():
    print("="*60)
    print("SPLIT VOTE RECOVERY STRESS TEST")
    print("="*60)

    # Stress loop: Kill and restart cluster 3 times
    for iteration in range(1, 4):
        print(f"\n--- Iteration {iteration}: Rapid Cluster Restart ---")
        kill_all_nodes()
        time.sleep(1)
        
        print("  Starting all nodes simultaneously...")
        for i in range(1, 6):
            start_node(i)
        
        print("  Waiting for election resolution (2s)...")
        time.sleep(2.0)
        
        states = get_cluster_state()
        leaders = [nid for nid, st in states.items() if st == 'Leader']
        
        print(f"  Nodes Online: {len(states)}")
        print(f"  Leaders Found: {len(leaders)} ({leaders})")
        
        if len(leaders) == 1:
            print(f"✓ PASS: Cluster resolved to 1 leader in term stability")
        elif len(leaders) == 0:
            print(f"⚠ Candidate phase persisting, waiting 2 more seconds...")
            time.sleep(2.0)
            states = get_cluster_state()
            leaders = [nid for nid, st in states.items() if st == 'Leader']
            if len(leaders) == 1:
                print(f"✓ PASS: Cluster resolved after extra wait")
            else:
                print(f"✗ FAIL: No leader elected after 4 seconds")
                return False
        else:
            print(f"✗ FAIL: Multiple leaders found: {leaders}")
            return False

    print("\n✓ SPLIT VOTE TEST PASSED")
    print("="*60)
    return True

if __name__ == '__main__':
    success = test_split_vote()
    sys.exit(0 if success else 1)
