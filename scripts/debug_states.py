"""
Simple debug script to check node states
"""
import sys
import os
import time

# Add src directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, src_dir)

from infrastructure.node import Node
from infrastructure.comms import Communicator

print("Checking running node states via RPC...")
print("="*60)

temp_node = Node(1)
comm = Communicator(temp_node)

for node_id in range(1, 6):
    peer = {'id': node_id, 'ip': '127.0.0.1', 'port': 5000 + node_id}
    try:
        res = comm.get_state(peer)
        if res:
            print(f"Node {node_id}: {res.state:10s} | Term: {res.term}")
        else:
            print(f"Node {node_id}: OFFLINE")
    except Exception as e:
        print(f"Node {node_id}: ERROR - {e}")


print("="*60)
