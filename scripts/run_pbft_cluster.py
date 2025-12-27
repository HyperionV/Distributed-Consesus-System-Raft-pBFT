"""
pBFT Cluster Manager
Starts and manages a cluster of pBFT nodes for testing
"""
import subprocess
import sys
import os
import time
import psutil

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)


def kill_pbft_nodes():
    """Kill all running pBFT nodes"""
    killed = 0
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in cmd[0].lower() and 'pbft_main.py' in ' '.join(cmd):
                print(f"  Killing: {' '.join(cmd[-3:])}")
                proc.kill()
                killed += 1
        except:
            pass
    return killed


def start_pbft_cluster(num_nodes=4, malicious_nodes=None):
    """
    Start a pBFT cluster
    
    Args:
        num_nodes: Number of nodes to start (default 4 for f=1)
        malicious_nodes: List of node IDs to run in malicious mode
    """
    malicious_nodes = malicious_nodes or []
    
    print("Stopping existing pBFT nodes...")
    killed = kill_pbft_nodes()
    if killed:
        print(f"Killed {killed} node(s), waiting for ports to free...")
        time.sleep(1.0)
    
    print(f"\nStarting {num_nodes} pBFT nodes...")
    processes = []
    
    for node_id in range(1, num_nodes + 1):
        cmd = [sys.executable, 'src/pbft_main.py', '--id', str(node_id)]
        if node_id in malicious_nodes:
            cmd.append('--malicious')
            print(f"  Starting Node {node_id} (MALICIOUS)...")
        else:
            print(f"  Starting Node {node_id}...")
        
        proc = subprocess.Popen(
            cmd,
            cwd=project_root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        processes.append(proc)
    
    print(f"\n✓ All {num_nodes} pBFT nodes started")
    print("Waiting for initialization (2s)...")
    time.sleep(2.0)
    
    return processes


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Manage pBFT cluster')
    parser.add_argument('--nodes', type=int, default=4, help='Number of nodes')
    parser.add_argument('--malicious', type=int, nargs='*', default=[], help='Malicious node IDs')
    parser.add_argument('--stop', action='store_true', help='Just stop nodes')
    args = parser.parse_args()
    
    if args.stop:
        killed = kill_pbft_nodes()
        print(f"Stopped {killed} pBFT node(s)")
        return
    
    processes = start_pbft_cluster(args.nodes, args.malicious)
    
    print("\npBFT cluster running. Press Ctrl+C to stop...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping all nodes...")
        kill_pbft_nodes()
        print("Done.")


if __name__ == '__main__':
    main()
