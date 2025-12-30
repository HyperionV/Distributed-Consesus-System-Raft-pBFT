import subprocess
import time
import sys
import os

def run_cluster(num_nodes=5):
    """
    Launch a local cluster of Raft nodes.
    Sets up the PYTHONPATH and starts each node as a separate subprocess.
    """
    print(f"[INFO] Starting {num_nodes} Raft Nodes...")
    procs = []
    root = os.path.join(os.path.dirname(__file__), '..')
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.join(root, 'src')
    
    for i in range(1, num_nodes + 1):
        # Allow stdout/stderr to inherit from parent for tracing as requested
        p = subprocess.Popen([sys.executable, 'src/main.py', '--id', str(i)], 
                             cwd=root, env=env)
        procs.append((i, p))
        time.sleep(0.5)
    
    print("[INFO] Raft Cluster running. Press Ctrl+C to stop.")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Stopping Raft cluster...")
        for _, p in procs: p.terminate()

if __name__ == '__main__':
    run_cluster()
