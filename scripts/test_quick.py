"""
Quick System Verification Test
Tests basic Raft functionality to verify the system works
"""
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


def kill_all_nodes():
    """Kill all running nodes"""
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in str(cmd).lower() and 'main.py' in str(cmd):
                proc.kill()
        except:
            pass
    time.sleep(1)


def start_nodes(count=3):
    """Start N nodes"""
    processes = []
    for i in range(1, count + 1):
        proc = subprocess.Popen(
            [sys.executable, 'src/main.py', '--id', str(i)],
            cwd=project_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        processes.append(proc)
    return processes


def get_states():
    """Get state from nodes 1-3"""
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    
    for i in range(1, 4):
        peer = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i}
        try:
            state = comm.get_state(peer)
            if state:
                states[i] = state.state
        except:
            states[i] = 'OFFLINE'
    
    return states


def main():
    print("="*60)
    print("QUICK SYSTEM VERIFICATION TEST")
    print("="*60)
    
    # Clean start
    print("\n1. Cleaning up...")
    kill_all_nodes()
    
    # Start 3 nodes
    print("2. Starting 3 nodes...")
    procs = start_nodes(3)
    
    # Wait for election
    print("3. Waiting for election (5s)...")
    time.sleep(5)
    
    # Check states
    print("4. Checking states...")
    states = get_states()
    
    leaders = [i for i, s in states.items() if s == 'Leader']
    followers = [i for i, s in states.items() if s == 'Follower']
    
    print(f"\nResults:")
    for node_id, state in sorted(states.items()):
        print(f"  Node {node_id}: {state}")
    
    # Verify
    success = len(leaders) == 1 and len(followers) == 2
    
    if success:
        print(f"\n✓ PASS: 1 leader (Node {leaders[0]}), 2 followers")
    else:
        print(f"\n✗ FAIL: Expected 1 leader, 2 followers")
        print(f"  Got: {len(leaders)} leaders, {len(followers)} followers")
    
    # Cleanup
    kill_all_nodes()
    
    print("="*60)
    return 0 if success else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Error: {e}")
        kill_all_nodes()
        sys.exit(1)
