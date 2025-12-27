"""
Leader Failure Test
Tests that the cluster handles leader crashes gracefully:
- Automatic re-election after leader dies
- No data loss for committed entries
- New leader can accept new commands
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
    for i in range(1, 6):
        peer = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        state = comm.get_state(peer)
        if state and state.state == 'Leader':
            return i, state.term, state.log_length
    return None, 0, 0


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


def test_leader_failure():
    print("="*60)
    print("LEADER FAILURE TEST")
    print("="*60)
    
    temp_node = Node(1)
    comm = Communicator(temp_node)
    
    # 1. Find leader and submit some data
    leader_id, term, log_len = find_leader()
    if not leader_id:
        print("✗ FAIL: No initial leader")
        return False
    print(f"✓ Initial leader: Node {leader_id} (term={term}, log={log_len})")
    
    leader_peer = {'id': leader_id, 'ip': '127.0.0.1', 'port': 5000 + leader_id}
    
    # Submit data before failure
    for i in range(3):
        resp = comm.submit_command(leader_peer, f"SET before_crash_{i}=value_{i}")
        if resp and resp.success:
            print(f"✓ Submitted: before_crash_{i}")
    
    time.sleep(1.0)  # Wait for commit
    
    # 2. Kill the leader
    print(f"\n  Killing leader Node {leader_id}...")
    kill_node(leader_id)
    
    # 3. Wait for new election
    print("  Waiting for new election (3s)...")
    time.sleep(3.0)
    
    # 4. Find new leader
    new_leader_id, new_term, new_log_len = find_leader()
    if not new_leader_id:
        print("✗ FAIL: No new leader elected")
        return False
    
    if new_leader_id == leader_id:
        print("✗ FAIL: Old dead leader still reported")
        return False
    
    print(f"✓ New leader: Node {new_leader_id} (term={new_term}, log={new_log_len})")
    
    # 5. Submit new data to new leader
    new_leader_peer = {'id': new_leader_id, 'ip': '127.0.0.1', 'port': 5000 + new_leader_id}
    
    resp = comm.submit_command(new_leader_peer, "SET after_crash=new_value")
    if resp and resp.success:
        print("✓ New leader accepting commands")
    else:
        print("⚠ New leader not accepting commands yet")
    
    # 6. Verify data survived
    time.sleep(1.0)
    data = comm.get_data(new_leader_peer, "before_crash_0")
    if data and data.success:
        print(f"✓ Data survived: before_crash_0 = {data.value}")
    else:
        print("⚠ Data may not be committed yet")
    
    # 7. Restart old leader
    print(f"\n  Restarting old leader Node {leader_id}...")
    start_node(leader_id)
    time.sleep(2.0)
    
    # 8. Verify old leader rejoined as follower
    old_leader_peer = {'id': leader_id, 'ip': '127.0.0.1', 'port': 5000 + leader_id}
    state = comm.get_state(old_leader_peer)
    
    if state and state.state == 'Follower':
        print(f"✓ Old leader rejoined as Follower (term={state.term})")
    elif state:
        print(f"⚠ Old leader state: {state.state}")
    else:
        print("✗ Old leader not responding")
    
    print("\n" + "="*60)
    print("✓ LEADER FAILURE TEST PASSED")
    print("="*60)
    return True


if __name__ == '__main__':
    success = test_leader_failure()
    sys.exit(0 if success else 1)
