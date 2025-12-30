import sys
import os
import time
import grpc

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))
sys.path.insert(0, os.path.join(project_root, 'src', 'generated'))

from generated import pbft_pb2
from generated import pbft_pb2_grpc
from run_pbft_cluster import start_pbft_cluster, kill_pbft_nodes

def get_status():
    """Retrieve current view and primary info from all pBFT nodes."""
    stats = {}
    for i in range(1, 5):
        try:
            with grpc.insecure_channel(f'127.0.0.1:{6000 + i}') as chan:
                stub = pbft_pb2_grpc.PBFTServiceStub(chan)
                s = stub.GetStatus(pbft_pb2.StatusRequest(), timeout=1.0)
                stats[i] = {'view': s.view, 'primary': s.primary_id, 'is_primary': s.is_primary}
        except: stats[i] = {'state': 'OFFLINE'}
    return stats

def submit_pbft(nid, op, timeout=8.0):
    """Send client request to the specified pBFT primary node."""
    try:
        with grpc.insecure_channel(f'127.0.0.1:{6000 + nid}') as chan:
            stub = pbft_pb2_grpc.PBFTServiceStub(chan)
            req = pbft_pb2.ClientRequest(operation=op, timestamp=int(time.time()*1000), client_id=999)
            return stub.Request(req, timeout=timeout)
    except Exception as e:
        print(f"  RPC Error: {e}")
        return None

def test_pbft():
    """Verify pBFT Byzantine Fault Tolerance by testing honest and malicious scenarios."""
    print("[PHASE 1] Consensus with 4 Honest Nodes")
    kill_pbft_nodes()
    time.sleep(1)
    start_pbft_cluster(4, mal=[])
    time.sleep(3)
    
    stats = get_status()
    pid = next((nid for nid, s in stats.items() if s.get('is_primary')), None)
    if not pid:
        print("[FAIL] Initial primary node not detected")
        return False
        
    print(f"  Leader is Node {pid}. Submitting request...")
    res = submit_pbft(pid, "SET key1=val1")
    if res and res.success:
        print("[PASS] Normal consensus achieved")
    else:
        print("[FAIL] Failed to achieve consensus in honest cluster")
        return False
    
    print("\n[PHASE 2] Consensus with 1 Byzantine Node")
    kill_pbft_nodes()
    time.sleep(1)
    # Node 4 is malicious
    start_pbft_cluster(4, mal=[4])
    time.sleep(3)
    
    stats = get_status()
    pid = next((nid for nid, s in stats.items() if s.get('is_primary')), None)
    if not pid:
        print("[FAIL] Primary node not detected after restart")
        return False
        
    print(f"  Primary: Node {pid}, Malicious: Node 4. Submitting requests...")
    successes = 0
    for i in range(3):
        res = submit_pbft(pid, f"SET byz_key_{i}=byz_val_{i}")
        if res and res.success:
            successes += 1
            print(f"    Request {i} committed")
        else:
            print(f"    Request {i} failed or timed out")
        time.sleep(0.5)
            
    print(f"  Summary: {successes}/3 requests committed")
    if successes >= 2:
        print("[PASS] pBFT tolerated the Byzantine fault")
        return True
    
    print("[FAIL] Byzantine node prevented sufficient quorum")
    return False

if __name__ == '__main__':
    try:
        sys.exit(0 if test_pbft() else 1)
    finally:
        kill_pbft_nodes()
        time.sleep(1)
