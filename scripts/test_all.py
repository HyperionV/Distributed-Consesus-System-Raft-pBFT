"""
Comprehensive End-to-End Test Suite for Distributed Consensus System
Tests both Raft and pBFT implementations across all scenarios
"""
import sys
import os
import time
import subprocess

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))


class TestRunner:
    def __init__(self):
        self.results = {}
        self.total = 0
        self.passed = 0
        self.failed = 0
    
    def run_test(self, name, script_path):
        """Run a test script and record result"""
        self.total += 1
        print(f"\n{'='*70}")
        print(f"Running: {name}")
        print('='*70)
        
        try:
            result = subprocess.run(
                [sys.executable, script_path],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            success = result.returncode == 0
            self.results[name] = {
                'success': success,
                'output': result.stdout[-500:] if result.stdout else '',
                'error': result.stderr[-500:] if result.stderr else ''
            }
            
            if success:
                self.passed += 1
                print(f"✓ PASS: {name}")
            else:
                self.failed += 1
                print(f"✗ FAIL: {name}")
                if result.stderr:
                    print(f"Error: {result.stderr[-200:]}")
            
            return success
            
        except subprocess.TimeoutExpired:
            self.failed += 1
            self.results[name] = {'success': False, 'output': '', 'error': 'Timeout'}
            print(f"✗ TIMEOUT: {name}")
            return False
        except Exception as e:
            self.failed += 1
            self.results[name] = {'success': False, 'output': '', 'error': str(e)}
            print(f"✗ ERROR: {name} - {e}")
            return False
    
    def print_summary(self):
        """Print final test summary"""
        print(f"\n{'='*70}")
        print("FINAL TEST SUMMARY")
        print('='*70)
        print(f"Total Tests: {self.total}")
        print(f"Passed: {self.passed} ({100*self.passed//self.total if self.total else 0}%)")
        print(f"Failed: {self.failed}")
        print()
        
        if self.failed > 0:
            print("Failed Tests:")
            for name, result in self.results.items():
                if not result['success']:
                    print(f"  ✗ {name}")
                    if result['error']:
                        print(f"    Error: {result['error'][:100]}")
        
        print('='*70)


def main():
    runner = TestRunner()
    
    print("="*70)
    print("DISTRIBUTED CONSENSUS SYSTEM - END-TO-END TEST SUITE")
    print("="*70)
    print("Testing: Raft (CFT) and pBFT (BFT) implementations")
    print()
    
    # === RAFT TESTS ===
    print("\n" + "="*70)
    print("RAFT CONSENSUS TESTS (Crash Fault Tolerance)")
    print("="*70)
    
    # Election & Failover
    runner.run_test(
        "Raft: Leader Election",
        os.path.join(script_dir, 'test_election.py')
    )
    
    runner.run_test(
        "Raft: Automated Failover",
        os.path.join(script_dir, 'test_automated_failover.py')
    )
    
    runner.run_test(
        "Raft: Node Rejoin",
        os.path.join(script_dir, 'test_node_rejoin.py')
    )
    
    runner.run_test(
        "Raft: Split Vote Recovery",
        os.path.join(script_dir, 'test_split_vote.py')
    )
    
    # Log Replication & Persistence
    runner.run_test(
        "Raft: Log Replication",
        os.path.join(script_dir, 'test_replication.py')
    )
    
    runner.run_test(
        "Raft: WAL Persistence",
        os.path.join(script_dir, 'test_persistence.py')
    )
    
    # Chaos Engineering
    runner.run_test(
        "Raft: Leader Failure",
        os.path.join(script_dir, 'test_leader_failure.py')
    )
    
    runner.run_test(
        "Raft: Network Partition (3-2 Split)",
        os.path.join(script_dir, 'test_network_partition.py')
    )
    
    # === pBFT TESTS ===
    print("\n" + "="*70)
    print("pBFT CONSENSUS TESTS (Byzantine Fault Tolerance)")
    print("="*70)
    
    runner.run_test(
        "pBFT: Byzantine Tolerance",
        os.path.join(script_dir, 'test_pbft.py')
    )
    
    # Print final summary
    runner.print_summary()
    
    return 0 if runner.failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
