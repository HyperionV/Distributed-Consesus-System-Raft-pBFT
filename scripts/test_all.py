import sys
import os
import time
import subprocess
import glob
import psutil

class TestRunner:
    """
    Automated test runner for the Raft and pBFT consensus cluster.
    """
    def __init__(self, python_path=sys.executable):
        self.python = python_path
        self.root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.results = []

    def cleanup(self):
        """Clean up stale WAL data files and stop running node processes."""
        print("[INFO] Cleaning up environment...")
        
        # Remove WAL files
        for f in glob.glob("wal_data_*.json") + glob.glob("wal_data_*.json.tmp"):
            try: os.remove(f)
            except: pass
            
        # Kill only consensus node processes, not the test runner itself
        current_pid = os.getpid()
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmd = proc.info['cmdline']
                if not cmd: continue
                cmd_str = ' '.join(cmd)
                # Check for our consensus scripts but don't kill the current script
                if proc.info['pid'] != current_pid:
                    if 'src/main.py' in cmd_str or 'src/pbft_main.py' in cmd_str or 'scripts/run_cluster.py' in cmd_str:
                        proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied): pass
        time.sleep(1)

    def run_script(self, name, script_path, timeout=60):
        """Execute a single test script and record its result."""
        print(f"\n[TEST] {name}")
        env = os.environ.copy()
        env['PYTHONPATH'] = os.path.join(self.root, 'src')
        try:
            res = subprocess.run([self.python, script_path], cwd=self.root, env=env, timeout=timeout)
            passed = res.returncode == 0
            if not passed: print(f"[FAIL] {name}")
            self.results.append((name, "[PASS]" if passed else "[FAIL]"))

            return passed
        except Exception as e:
            print(f"[ERROR] {name}: {e}")
            self.results.append((name, "[ERROR]"))
            return False

def main():
    """Main execution entry for running all consensus tests."""
    runner = TestRunner()
    runner.cleanup()

    print("\n[PHASE 1] RAFT CLUSTER")
    # Allow node logs to reach console for tracing as requested
    cluster = subprocess.Popen([sys.executable, 'scripts/run_cluster.py'], cwd=runner.root)
    time.sleep(3)


    raft_tests = [
        ("Leader Election", "scripts/test_election.py"),
        ("Leader Failure", "scripts/test_leader_failure.py"),
        ("Log Replication", "scripts/test_replication.py"),
        ("Network Partition", "scripts/test_network_partition.py"),
        ("Persistence", "scripts/test_persistence.py"),
        ("Node Rejoin", "scripts/test_node_rejoin.py"),
        ("Split Vote Recovery", "scripts/test_split_vote.py")
    ]

    all_passed = True
    for name, path in raft_tests:
        print("\n=============")
        if not runner.run_script(name, path): all_passed = False
    
    cluster.terminate()
    runner.cleanup()

    print("\n=============\n\n[PHASE 2] PBFT CLUSTER")
    if not runner.run_script("pBFT Byzantine Test", "scripts/test_pbft.py", timeout=120): all_passed = False

    print("\n=============\n\nTEST SUMMARY")
    for name, res in runner.results: print(f"{res.ljust(8)} {name}")
    print("="*60)
    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
