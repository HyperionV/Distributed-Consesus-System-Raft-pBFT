"""
Test Persistence (WAL)
Verifies that nodes restore state correctly after restart
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


def find_leader():
    temp_node = Node(1)
    comm = Communicator(temp_node)
    for node_id in range(1, 6):
        peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
        state = comm.get_state(peer)
        if state and state.state == 'Leader':
            return node_id, peer, state.term, state.log_length
    return None, None, 0, 0


def get_node_state(node_id):
    temp_node = Node(1)
    comm = Communicator(temp_node)
    peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
    return comm.get_state(peer)


def kill_node(node_id):
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in cmd[0].lower() and 'src/main.py' in ' '.join(cmd) and f'--id {node_id}' in ' '.join(cmd):
                proc.kill()
                return True
        except:
            pass
    return False


def start_node(node_id):
    return subprocess.Popen(
        [sys.executable, 'src/main.py', '--id', str(node_id)],
        cwd=project_root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


def test_persistence():
    print("="*60)
    print("PERSISTENCE (WAL) TEST")
    print("="*60)
    
    # 1. Find leader and submit commands
    leader_id, leader_peer, initial_term, initial_log_len = find_leader()
    if not leader_id:
        print("✗ FAIL: No leader found")
        return False
    print(f"✓ Leader: Node {leader_id} (term={initial_term}, log={initial_log_len})")
    
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    # Submit some commands
    for i in range(3):
        cmd = f"SET key{i}=value{i}"
        response = comm.submit_command(leader_peer, cmd)
        if not response or not response.success:
            print(f"✗ FAIL: Could not submit {cmd}")
            return False
        print(f"✓ Submitted: {cmd}")
    
    time.sleep(2.0)  # Wait for replication
    
    # 2. Record state before kill
    state_before = get_node_state(leader_id)
    print(f"\nBefore kill: term={state_before.term}, log_length={state_before.log_length}")
    
    # 3. Kill and restart leader
    print(f"\n  Killing Node {leader_id}...")
    kill_node(leader_id)
    time.sleep(1.0)
    
    print(f"  Restarting Node {leader_id}...")
    start_node(leader_id)
    time.sleep(2.0)  # Wait for startup and potential election
    
    # 4. Check state after restart
    state_after = get_node_state(leader_id)
    if not state_after:
        print(f"✗ FAIL: Node {leader_id} not responding after restart")
        return False
    
    print(f"\nAfter restart: term={state_after.term}, log_length={state_after.log_length}")
    
    # Verify log was restored
    if state_after.log_length >= state_before.log_length:
        print(f"✓ PASS: Log restored ({state_after.log_length} entries)")
    else:
        print(f"✗ FAIL: Log not fully restored (before={state_before.log_length}, after={state_after.log_length})")
        return False
    
    # Verify term was restored (may be higher if new election happened)
    if state_after.term >= state_before.term:
        print(f"✓ PASS: Term preserved/advanced ({state_after.term})")
    else:
        print(f"✗ FAIL: Term regression (before={state_before.term}, after={state_after.term})")
        return False
    
    print("\n" + "="*60)
    print("✓ PERSISTENCE TEST PASSED")
    print("="*60)
    return True


if __name__ == '__main__':
    success = test_persistence()
    sys.exit(0 if success else 1)
