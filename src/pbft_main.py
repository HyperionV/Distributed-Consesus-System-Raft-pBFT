"""
pBFT Node - Separate node implementation for Byzantine Fault Tolerant consensus
Usage: python src/pbft_main.py --id <node_id> [--malicious]
"""
import sys
import os
import argparse
import json
import grpc
from concurrent import futures
import logging

from generated import pbft_pb2, pbft_pb2_grpc
from consensus.pbft import PBFTConsensus

logging.basicConfig(level=logging.INFO, format='[pBFT Node %(node_id)s] %(message)s')


class PBFTNode:
    """Node running pBFT consensus instead of Raft"""
    
    def __init__(self, node_id, config_path='pbft_nodes_config.json', malicious=False):
        self.node_id = node_id
        self.config = self._load_config(config_path)
        self.node_info = self._get_node_info()
        self.peers = [n for n in self.config if n['id'] != node_id]
        self.blocked_ips = set()
        self.blocked_node_ids = set()
        self.server = None
        self.logger = logging.LoggerAdapter(logging.getLogger(), {'node_id': node_id})
        
        # Initialize pBFT consensus (f=1 for 4 nodes)
        # For 5 nodes we'd need f=1 as well (3f+1=4 is minimum, 5 nodes can still tolerate 1 fault)
        self.pbft = PBFTConsensus(self, f=1)
        
        if malicious:
            self.pbft.set_malicious(True)
    
    def _load_config(self, path):
        config_file = os.path.join(os.path.dirname(__file__), '..', path)
        with open(config_file, 'r') as f:
            return json.load(f)
    
    def _get_node_info(self):
        for node in self.config:
            if node['id'] == self.node_id:
                return node
        raise ValueError(f"Node {self.node_id} not found in config")
    
    def serve(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        
        # Register pBFT service
        pbft_pb2_grpc.add_PBFTServiceServicer_to_server(PBFTServicer(self), self.server)
        
        address = f"{self.node_info['ip']}:{self.node_info['port']}"
        self.server.add_insecure_port(address)
        self.server.start()
        
        self.logger.info(f"pBFT Node started on {address}")
        self.logger.info(f"Primary: Node {self.pbft.primary_id}, Is Primary: {self.pbft.is_primary}")
        
        # Start pBFT consensus
        self.pbft.start()
        
        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            self.pbft.stop()
            self.server.stop(0)


class PBFTServicer(pbft_pb2_grpc.PBFTServiceServicer):
    """gRPC servicer for pBFT"""
    
    def __init__(self, node):
        self.node = node
    
    def Request(self, request, context):
        return self.node.pbft.handle_client_request(request)
    
    def PrePrepare(self, request, context):
        return self.node.pbft.handle_pre_prepare(request)
    
    def Prepare(self, request, context):
        return self.node.pbft.handle_prepare(request)
    
    def Commit(self, request, context):
        return self.node.pbft.handle_commit(request)
    
    def ViewChange(self, request, context):
        return self.node.pbft.handle_view_change(request)
    
    def NewView(self, request, context):
        # Simplified - just accept
        return pbft_pb2.NewViewReply(accepted=True)
    
    def GetStatus(self, request, context):
        status = self.node.pbft.get_status()
        return pbft_pb2.StatusReply(
            view=status['view'],
            last_sequence=status['sequence'],
            primary_id=status['primary_id'],
            replica_id=self.node.node_id,
            is_primary=status['is_primary'],
            state=status['state']
        )


def main():
    parser = argparse.ArgumentParser(description='Start a pBFT node')
    parser.add_argument('--id', type=int, required=True, help='Node ID')
    parser.add_argument('--malicious', action='store_true', help='Run in malicious mode')
    args = parser.parse_args()
    
    node = PBFTNode(args.id, malicious=args.malicious)
    node.serve()


if __name__ == '__main__':
    main()
