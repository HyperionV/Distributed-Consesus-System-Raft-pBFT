import sys
import os
import grpc
import logging

from generated import raft_pb2, raft_pb2_grpc, control_pb2, control_pb2_grpc, pbft_pb2, pbft_pb2_grpc

class Communicator:
    def __init__(self, node):
        self.node = node
        self.logger = node.logger
    
    def _get_channel(self, peer):
        address = f"{peer['ip']}:{peer['port']}"
        return grpc.insecure_channel(address)
    
    def _is_blocked(self, peer):
        """Check if peer is blocked by partition simulation"""
        peer_id = peer.get('id')
        peer_ip = peer.get('ip')
        
        if peer_id and peer_id in self.node.blocked_node_ids:
            self.logger.debug(f"Blocked: Node {peer_id} is partitioned")
            return True
        if peer_ip and peer_ip in self.node.blocked_ips:
            self.logger.debug(f"Blocked: IP {peer_ip} is partitioned")
            return True
        return False
    
    def ping(self, peer, timeout=1.0):
        try:
            with self._get_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                response = stub.Ping(
                    raft_pb2.PingRequest(sender_id=self.node.node_id),
                    timeout=timeout
                )
                self.logger.info(f"Ping to Node {peer['id']}: {response.message}")
                return response
        except grpc.RpcError as e:
            self.logger.error(f"Ping to Node {peer['id']} failed: {e.code()}")
            return None
    
    def request_vote(self, peer, args, timeout=1.0):
        if self._is_blocked(peer):
            return None
        try:
            with self._get_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                return stub.RequestVote(args, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def append_entries(self, peer, args, timeout=1.0):
        if self._is_blocked(peer):
            return None
        try:
            with self._get_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                return stub.AppendEntries(args, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def set_partition(self, peer, blocked_ips=None, blocked_node_ids=None):
        """Set partition on a node - block specific IPs or node IDs"""
        try:
            with self._get_channel(peer) as channel:
                stub = control_pb2_grpc.ControlServiceStub(channel)
                return stub.SetPartition(
                    control_pb2.PartitionRequest(
                        blocked_ips=blocked_ips or [],
                        blocked_node_ids=blocked_node_ids or []
                    ),
                    timeout=1.0
                )
        except grpc.RpcError as e:
            self.logger.error(f"SetPartition to Node {peer['id']} failed: {e.code()}")
            return None
    
    def get_state(self, peer):
        """Get Raft state from a peer (for testing)"""
        try:
            with self._get_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                return stub.GetState(raft_pb2.GetStateRequest(), timeout=1.0)
        except grpc.RpcError:
            return None
    
    def submit_command(self, peer, command, timeout=2.0):
        """Submit a command to a node (should be leader)"""
        try:
            with self._get_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                return stub.SubmitCommand(
                    raft_pb2.SubmitCommandRequest(command=command),
                    timeout=timeout
                )
        except grpc.RpcError as e:
            self.logger.error(f"SubmitCommand to Node {peer['id']} failed: {e.code()}")
            return None
    
    def get_data(self, peer, key, timeout=1.0):
        """Get data from a node's state machine"""
        try:
            with self._get_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                return stub.GetData(
                    raft_pb2.GetDataRequest(key=key),
                    timeout=timeout
                )
        except grpc.RpcError:
            return None

    # ========== pBFT Methods ==========
    
    def pbft_pre_prepare(self, peer, request, timeout=1.0):
        """Send PrePrepare to peer"""
        try:
            with self._get_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                return stub.PrePrepare(request, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def pbft_prepare(self, peer, request, timeout=1.0):
        """Send Prepare to peer"""
        try:
            with self._get_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                return stub.Prepare(request, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def pbft_commit(self, peer, request, timeout=1.0):
        """Send Commit to peer"""
        try:
            with self._get_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                return stub.Commit(request, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def pbft_view_change(self, peer, request, timeout=1.0):
        """Send ViewChange to peer"""
        try:
            with self._get_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                return stub.ViewChange(request, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def pbft_request(self, peer, request, timeout=5.0):
        """Send client Request to peer"""
        try:
            with self._get_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                return stub.Request(request, timeout=timeout)
        except grpc.RpcError:
            return None
    
    def pbft_get_status(self, peer, timeout=1.0):
        """Get pBFT status from peer"""
        try:
            with self._get_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                return stub.GetStatus(pbft_pb2.StatusRequest(), timeout=timeout)
        except grpc.RpcError:
            return None


