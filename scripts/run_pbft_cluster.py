import subprocess
import sys
import os
import time
import psutil

# Define project root for consistent pathing
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def kill_pbft_nodes():
    """Find and terminate all running pBFT node processes."""
    for f in psutil.process_iter(['pid', 'cmdline']):
        try:
            if f.info['cmdline'] and 'pbft_main.py' in ' '.join(f.info['cmdline']): f.kill()
        except: pass

def start_pbft_cluster(num=4, mal=None):
    """
    Launch a local cluster of pBFT nodes.
    
    Args:
        num: Number of nodes to start.
        mal: List of node IDs that should run in malicious (Byzantine) mode.
    """
    mal = mal or []
    kill_pbft_nodes()
    time.sleep(1)
    
    procs = []
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.join(root, 'src')
    
    for i in range(1, num + 1):
        cmd = [sys.executable, 'src/pbft_main.py', '--id', str(i)]
        if i in mal: cmd.append('--malicious')
        # Allow stdout/stderr to inherit from parent for tracing
        p = subprocess.Popen(cmd, cwd=root, env=env)
        procs.append(p)
    return procs

if __name__ == '__main__':
    start_pbft_cluster()
    print("[INFO] pBFT Cluster started. Press Ctrl+C to stop.")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        kill_pbft_nodes()
        print("\n[INFO] pBFT Cluster stopped.")
