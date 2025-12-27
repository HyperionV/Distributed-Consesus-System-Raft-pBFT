"""
Helper script to restart the cluster
Stops all running nodes and starts fresh ones
"""
import subprocess
import time
import sys
import os
import psutil

def kill_existing_nodes():
    """Kill any existing node processes"""
    print("Stopping existing nodes...")
    killed = 0
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and 'python' in cmdline[0].lower() and 'src/main.py' in ' '.join(cmdline):
                print(f"  Killing process {proc.info['pid']}: {' '.join(cmdline)}")
                proc.kill()
                killed += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
    if killed > 0:
        print(f"Killed {killed} node(s), waiting for ports to free...")
        time.sleep(2)
    else:
        print("No existing nodes found")

def start_nodes():
    """Start all 5 nodes"""
    print("\nStarting 5 nodes...")
    processes = []
    
    for i in range(1, 6):
        cmd = [sys.executable, 'src/main.py', '--id', str(i)]
        print(f"  Starting Node {i}...")
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)) + '/..',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append((i, proc))
        time.sleep(0.3)
    
    print(f"\n✓ All 5 nodes started")
    print("Waiting for election...")
    time.sleep(1.0)
    
    return processes

if __name__ == '__main__':
    try:
        kill_existing_nodes()
        processes = start_nodes()
        
        print("\nPress Ctrl+C to stop all nodes\n")
        
        # Monitor output
        while True:
            for node_id, proc in processes:
                line = proc.stdout.readline()
                if line:
                    print(f"[Node {node_id}] {line.strip()}")
            time.sleep(0.01)
            
    except KeyboardInterrupt:
        print("\n\nStopping all nodes...")
        for node_id, proc in processes:
            proc.terminate()
        print("Done.")
