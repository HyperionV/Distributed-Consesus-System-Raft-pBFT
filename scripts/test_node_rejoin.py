"""
Node Rejoin Test
Kills a leader, let a new one be elected, then restarts the old leader and verifies it joins as a follower.
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

def get_node_state(node_id):
    temp_node = Node(1)
    comm = Communicator(temp_node)
    peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
    try:
        res = comm.get_state(peer)
        if res:
            return res.state, res.term
    except:
        pass
    return "OFFLINE", -1

def kill_node(node_id):
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in cmd[0].lower() and 'src/main.py' in ' '.join(cmd) and f'--id {node_id}' in ' '.join(cmd):
                proc.kill()
                return True
        except:
            pass
    return False

def test_node_rejoin():
    print("="*60)
    print("NODE REJOIN TEST")
    print("="*60)

    # 1. Find leader
    leader_id = None
    for i in range(1, 6):
        state, term = get_node_state(i)
        if state == 'Leader':
            leader_id = i
            break
    
    if not leader_id:
        print("✗ FAIL: No initial leader found")
        return False
    print(f"✓ Initial leader: Node {leader_id}")

    # 2. Kill leader
    print(f"  Killing leader Node {leader_id}...")
    kill_node(leader_id)
    
    # Wait and monitor
    print("  Waiting for re-election (up to 3 seconds)...")
    new_leader_id = None
    new_term = -1
    
    for _ in range(3):
        time.sleep(1.0)
        print("  Checking cluster state...")
        for i in range(1, 6):
            if i == leader_id: continue
            state, term = get_node_state(i)
            print(f"    Node {i}: {state} (term {term})")
            if state == 'Leader':
                new_leader_id = i
                new_term = term
        
        if new_leader_id:
            break
    
    if not new_leader_id:
        print("✗ FAIL: No new leader elected after failure")
        return False
    print(f"✓ New leader elected: Node {new_leader_id} (term {new_term})")


    # 4. Restart old leader
    print(f"  Restarting old leader Node {leader_id}...")
    subprocess.Popen([sys.executable, 'src/main.py', '--id', str(leader_id)], 
                     cwd=project_root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)

    # 5. Verify old leader rejoins as Follower and updates term
    # Wait a bit more for heartbeat
    time.sleep(1.0)
    state, term = get_node_state(leader_id)
    
    # Get current leader term to be sure
    _, current_leader_term = get_node_state(new_leader_id)
    
    print(f"  Old leader Node {leader_id} state after restart: {state} (term {term})")
    print(f"  Current leader Node {new_leader_id} term: {current_leader_term}")
    
    if state == 'Follower' and term >= current_leader_term:
        print(f"✓ PASS: Old leader joined as Follower and synced to term {term}")
    else:
        print(f"✗ FAIL: Expected Follower with term >= {current_leader_term}, got {state} (term {term})")
        return False


    print("\n✓ REJOIN TEST PASSED")
    print("="*60)
    return True

if __name__ == '__main__':
    success = test_node_rejoin()
    sys.exit(0 if success else 1)
