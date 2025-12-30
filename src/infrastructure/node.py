import sys
import os
import json
import grpc
from concurrent import futures
from generated import raft_pb2, raft_pb2_grpc, control_pb2, control_pb2_grpc

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from consensus.raft import RaftConsensus

class Node:
    """
    Main cluster node supporting Raft and administrative Control services.
    """
    
    def __init__(self, node_id, config_path='nodes_config.json'):
        self.node_id = node_id
        self.config = self._load_config(config_path)
        self.node_info = next(n for n in self.config if n['id'] == node_id)
        self.peers = [n for n in self.config if n['id'] != node_id]
        self.blocked_ips, self.blocked_node_ids = set(), set()
        self.raft = RaftConsensus(self)
        
    def _load_config(self, path):
        """Load cluster configuration from JSON file."""
        config_file = os.path.join(os.path.dirname(__file__), '..', '..', path)
        with open(config_file, 'r') as f: return json.load(f)
    
    def log(self, message, level="INFO"):
        """Professional plain-text logging to stdout."""
        print(f"[{level}][Node {self.node_id}] {message}")
    
    def serve(self):
        """Start the gRPC server and consensus logic."""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(self), self.server)
        control_pb2_grpc.add_ControlServiceServicer_to_server(ControlServicer(self), self.server)
        
        addr = f"{self.node_info['ip']}:{self.node_info['port']}"
        self.server.add_insecure_port(addr)
        self.server.start()
        
        self.log(f"Started on {addr}")
        self.raft.start()
        
        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            self.raft.stop()
            self.server.stop(0)

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    """gRPC Servicer for standard Raft operations."""
    def __init__(self, node): self.node = node
    def Ping(self, req, ctx): return raft_pb2.PingReply(receiver_id=self.node.node_id, message=f"Pong from {self.node.node_id}")
    def RequestVote(self, req, ctx): return self.node.raft.handle_request_vote(req)
    def AppendEntries(self, req, ctx): return self.node.raft.handle_append_entries(req)
    def GetState(self, req, ctx):
        s = self.node.raft.get_state()
        return raft_pb2.GetStateReply(state=s['state'], term=s['term'], node_id=self.node.node_id, 
                                     log_length=s['log_length'], commit_index=s['commit_index'])
    def SubmitCommand(self, req, ctx):
        success, msg = self.node.raft.submit_command(req.command)
        return raft_pb2.SubmitCommandReply(success=success, message=msg, leader_id=0 if success else -1)
    def GetData(self, req, ctx):
        v = self.node.raft.state_machine.get(req.key)
        return raft_pb2.GetDataReply(success=v is not None, value=v or "", message="OK" if v is not None else "Not found")

class ControlServicer(control_pb2_grpc.ControlServiceServicer):
    """gRPC Servicer for administrative/test cluster control."""
    def __init__(self, node): self.node = node
    def SetPartition(self, req, ctx):
        self.node.blocked_ips, self.node.blocked_node_ids = set(req.blocked_ips), set(req.blocked_node_ids)
        self.node.log(f"Partition set: blocking nodes {list(self.node.blocked_node_ids)}")
        return control_pb2.PartitionReply(success=True, message="OK")
    def GetPartitionStatus(self, req, ctx):
        return control_pb2.PartitionStatusReply(blocked_node_ids=list(self.node.blocked_node_ids), 
                                               blocked_ips=list(self.node.blocked_ips))
