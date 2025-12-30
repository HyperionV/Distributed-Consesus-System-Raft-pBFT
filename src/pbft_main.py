import sys
import os
import argparse
import json
import grpc
from concurrent import futures
from generated import pbft_pb2, pbft_pb2_grpc
from consensus.pbft import PBFTConsensus

class PBFTNode:
    """
    pBFT node wrapper handling configuration, gRPC service, and consensus logic.
    """
    def __init__(self, node_id, config_path='pbft_nodes_config.json', malicious=False):
        self.node_id = node_id
        config_file = os.path.join(os.path.dirname(__file__), '..', config_path)
        with open(config_file, 'r') as f: self.config = json.load(f)
        self.node_info = next(n for n in self.config if n['id'] == node_id)
        self.peers = [n for n in self.config if n['id'] != node_id]
        self.pbft = PBFTConsensus(self, f=1)
        if malicious: self.pbft.set_malicious(True)
    
    def log(self, message, level="INFO"):
        """Professional plain-text logging to stdout."""
        print(f"[{level}][pBFT Node {self.node_id}] {message}")
    
    def serve(self):
        """Start the gRPC server and pBFT consensus."""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        pbft_pb2_grpc.add_PBFTServiceServicer_to_server(PBFTServicer(self), self.server)
        addr = f"{self.node_info['ip']}:{self.node_info['port']}"
        self.server.add_insecure_port(addr)
        self.server.start()
        
        self.log(f"Started on {addr}")
        self.pbft.start()
        
        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            self.pbft.stop()
            self.server.stop(0)

class PBFTServicer(pbft_pb2_grpc.PBFTServiceServicer):
    """gRPC Servicer for pBFT phase messages."""
    def __init__(self, node): self.node = node
    def Request(self, req, ctx): return self.node.pbft.handle_client_request(req)
    def PrePrepare(self, req, ctx): return self.node.pbft.handle_pre_prepare(req)
    def Prepare(self, req, ctx): return self.node.pbft.handle_prepare(req)
    def Commit(self, req, ctx): return self.node.pbft.handle_commit(req)
    def ViewChange(self, req, ctx): return self.node.pbft.handle_view_change(req)
    def NewView(self, req, ctx): return pbft_pb2.NewViewReply(accepted=True)
    def GetStatus(self, req, ctx):
        s = self.node.pbft.get_status()
        return pbft_pb2.StatusReply(view=s['view'], last_sequence=0, primary_id=s['primary_id'], 
                                   replica_id=self.node.node_id, is_primary=s['is_primary'], state=s['state'])

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--id', type=int, required=True)
    p.add_argument('--malicious', action='store_true')
    args = p.parse_args()
    PBFTNode(args.id, malicious=args.malicious).serve()

if __name__ == '__main__':
    main()
