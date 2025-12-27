import sys
import os
import json
import grpc
from concurrent import futures
import logging

from generated import raft_pb2, raft_pb2_grpc, control_pb2, control_pb2_grpc

# Import Raft consensus
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from consensus.raft import RaftConsensus

logging.basicConfig(level=logging.INFO, format='[Node %(node_id)s] %(message)s')

class Node:
    def __init__(self, node_id, config_path='nodes_config.json'):
        self.node_id = node_id
        self.config = self._load_config(config_path)
        self.node_info = self._get_node_info()
        self.peers = [n for n in self.config if n['id'] != node_id]
        self.blocked_ips = set()
        self.blocked_node_ids = set()  # For partition simulation
        self.server = None
        self.logger = logging.LoggerAdapter(logging.getLogger(), {'node_id': node_id})
        
        # Initialize Raft consensus
        self.raft = RaftConsensus(self)
        
    def _load_config(self, path):
        config_file = os.path.join(os.path.dirname(__file__), '..', '..', path)
        with open(config_file, 'r') as f:
            return json.load(f)
    
    def _get_node_info(self):
        for node in self.config:
            if node['id'] == self.node_id:
                return node
        raise ValueError(f"Node {self.node_id} not found in config")
    
    def serve(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        
        # Register services
        raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(self), self.server)
        control_pb2_grpc.add_ControlServiceServicer_to_server(ControlServicer(self), self.server)
        
        address = f"{self.node_info['ip']}:{self.node_info['port']}"
        self.server.add_insecure_port(address)
        self.server.start()
        
        self.logger.info(f"Started on {address}")
        self.logger.info(f"Peers: {[f"{p['id']}@{p['ip']}:{p['port']}" for p in self.peers]}")
        
        # Start Raft consensus
        self.raft.start()
        
        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            self.raft.stop()
            self.server.stop(0)

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node):
        self.node = node
    
    def Ping(self, request, context):
        self.node.logger.info(f"Received Ping from Node {request.sender_id}")
        return raft_pb2.PingReply(
            receiver_id=self.node.node_id,
            message=f"Pong from Node {self.node.node_id}"
        )
    
    def RequestVote(self, request, context):
        return self.node.raft.handle_request_vote(request)
    
    def AppendEntries(self, request, context):
        return self.node.raft.handle_append_entries(request)
    
    def GetState(self, request, context):
        state_info = self.node.raft.get_state()
        return raft_pb2.GetStateReply(
            state=state_info['state'],
            term=state_info['term'],
            node_id=self.node.node_id,
            log_length=state_info['log_length'],
            commit_index=state_info['commit_index']
        )
    
    def SubmitCommand(self, request, context):
        success, message = self.node.raft.submit_command(request.command)
        leader_id = 0 if success else -1  # TODO: track actual leader ID
        return raft_pb2.SubmitCommandReply(
            success=success,
            message=message,
            leader_id=leader_id
        )
    
    def GetData(self, request, context):
        value = self.node.raft.state_machine.get(request.key)
        if value is not None:
            return raft_pb2.GetDataReply(success=True, value=value, message="OK")
        return raft_pb2.GetDataReply(success=False, value="", message=f"Key '{request.key}' not found")

class ControlServicer(control_pb2_grpc.ControlServiceServicer):
    def __init__(self, node):
        self.node = node
    
    def SetPartition(self, request, context):
        self.node.blocked_ips = set(request.blocked_ips)
        self.node.blocked_node_ids = set(request.blocked_node_ids)
        
        blocked = list(self.node.blocked_node_ids) or list(self.node.blocked_ips)
        self.node.logger.info(f"Partition set: blocking nodes {list(self.node.blocked_node_ids)}, IPs {list(self.node.blocked_ips)}")
        
        return control_pb2.PartitionReply(
            success=True,
            message=f"Blocked {len(blocked)} targets"
        )
    
    def GetPartitionStatus(self, request, context):
        return control_pb2.PartitionStatusReply(
            blocked_node_ids=list(self.node.blocked_node_ids),
            blocked_ips=list(self.node.blocked_ips)
        )

