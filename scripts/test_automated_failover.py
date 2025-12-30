import sys, os, time, psutil
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, os.path.join(project_root, 'src'))
from infrastructure.node import Node
from infrastructure.comms import Communicator

def get_cluster_state():
    temp_node = Node(1)
    comm = Communicator(temp_node)
    states = {}
    for i in range(1, 6):
        peer = {'id': i, 'ip': '127.0.0.1', 'port': 5000 + i if i != 1 else 5001}
        res = comm.get_state(peer)
        states[i] = {'state': res.state, 'term': res.term} if res else {'state': 'OFFLINE'}
    return states

def find_leader():
    states = get_cluster_state()
    for nid, s in states.items():
        if s['state'] == 'Leader': return nid, s['term']
    return None, None

def kill_node(nid):
    print(f"  Killing Node {nid}...")
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmd = proc.info['cmdline']
            if cmd and 'python' in cmd[0].lower() and 'src/main.py' in ' '.join(cmd) and f'--id {nid}' in ' '.join(cmd):
                proc.kill()
                return True
        except: pass
    return False

def test_automated_failover():
    print("RAFT AUTOMATED FAILOVER TEST")
    l_id, term = find_leader()
    if not l_id:
        print("[FAIL] No initial leader")
        return False
    print(f"Initial leader: Node {l_id} (term {term})")
    
    if not kill_node(l_id):
        print(f"[FAIL] Could not kill Node {l_id}")
        return False
    
    print("Waiting for re-election (2s)...")
    time.sleep(2.0)
    
    new_l_id, new_term = find_leader()
    if not new_l_id:
        print("[FAIL] No new leader elected")
        return False
    print(f"New leader: Node {new_l_id} (term {new_term})")
    
    if new_l_id == l_id:
        print("[FAIL] Dead node still reported as leader")
        return False
    if new_term <= term:
        print(f"[FAIL] Term not incremented ({term} -> {new_term})")
        return False
    print("[PASS] Failover successful")
    return True

if __name__ == '__main__':
    sys.exit(0 if test_automated_failover() else 1)
