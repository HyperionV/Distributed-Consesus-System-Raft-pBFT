import subprocess
import time
import sys
import os

def run_cluster(num_nodes=5):
    print(f"=== Starting {num_nodes} Nodes ===\n")
    
    processes = []
    
    for i in range(1, num_nodes + 1):
        cmd = [sys.executable, 'src/main.py', '--id', str(i)]
        print(f"Starting Node {i}...")
        proc = subprocess.Popen(
            cmd,
            cwd=os.path.join(os.path.dirname(__file__), '..'),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        processes.append((i, proc))
        time.sleep(0.5)
    
    print(f"\n✓ All {num_nodes} nodes started")
    print("Press Ctrl+C to stop all nodes\n")
    
    try:
        while True:
            for node_id, proc in processes:
                line = proc.stdout.readline()
                if line:
                    print(f"[Node {node_id}] {line.strip()}")
    except KeyboardInterrupt:
        print("\n\n=== Stopping All Nodes ===")
        for node_id, proc in processes:
            proc.terminate()
            print(f"Stopped Node {node_id}")
        print("Done.")

if __name__ == '__main__':
    run_cluster()
