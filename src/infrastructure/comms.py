import grpc
from generated import raft_pb2, raft_pb2_grpc, control_pb2, control_pb2_grpc, pbft_pb2, pbft_pb2_grpc

class Communicator:
    """
    Client-side communication wrapper for both Raft and pBFT services.
    Handles RPC timeouts and network partition simulation.
    """
    
    def __init__(self, node):
        self.node = node
    
    def _get_channel(self, peer):
        return grpc.insecure_channel(f"{peer['ip']}:{peer['port']}")
    
    def _is_blocked(self, peer):
        """Check if communication with the peer should be blocked (partition simulation)."""
        return peer.get('id') in self.node.blocked_node_ids or peer.get('ip') in self.node.blocked_ips
    
    def ping(self, peer, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return raft_pb2_grpc.RaftServiceStub(chan).Ping(
                    raft_pb2.PingRequest(sender_id=self.node.node_id), timeout=timeout)
        except Exception: return None
    
    def request_vote(self, peer, args, timeout=1.0):
        if self._is_blocked(peer): return None
        try:
            with self._get_channel(peer) as chan:
                return raft_pb2_grpc.RaftServiceStub(chan).RequestVote(args, timeout=timeout)
        except: return None
    
    def append_entries(self, peer, args, timeout=1.0):
        if self._is_blocked(peer): return None
        try:
            with self._get_channel(peer) as chan:
                return raft_pb2_grpc.RaftServiceStub(chan).AppendEntries(args, timeout=timeout)
        except: return None
    
    def set_partition(self, peer, blocked_ips=None, blocked_node_ids=None):
        try:
            with self._get_channel(peer) as chan:
                return control_pb2_grpc.ControlServiceStub(chan).SetPartition(
                    control_pb2.PartitionRequest(blocked_ips=blocked_ips or [], 
                    blocked_node_ids=blocked_node_ids or []), timeout=1.0)
        except: return None
    
    def get_state(self, peer):
        try:
            with self._get_channel(peer) as chan:
                return raft_pb2_grpc.RaftServiceStub(chan).GetState(
                    raft_pb2.GetStateRequest(), timeout=1.0)
        except: return None
    
    def submit_command(self, peer, cmd, timeout=2.0):
        try:
            with self._get_channel(peer) as chan:
                return raft_pb2_grpc.RaftServiceStub(chan).SubmitCommand(
                    raft_pb2.SubmitCommandRequest(command=cmd), timeout=timeout)
        except: return None
    
    def get_data(self, peer, key, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return raft_pb2_grpc.RaftServiceStub(chan).GetData(
                    raft_pb2.GetDataRequest(key=key), timeout=timeout)
        except: return None

    # pBFT Methods
    def pbft_pre_prepare(self, peer, req, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return pbft_pb2_grpc.PBFTServiceStub(chan).PrePrepare(req, timeout=timeout)
        except: return None
    
    def pbft_prepare(self, peer, req, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return pbft_pb2_grpc.PBFTServiceStub(chan).Prepare(req, timeout=timeout)
        except: return None
    
    def pbft_commit(self, peer, req, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return pbft_pb2_grpc.PBFTServiceStub(chan).Commit(req, timeout=timeout)
        except: return None
    
    def pbft_view_change(self, peer, req, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return pbft_pb2_grpc.PBFTServiceStub(chan).ViewChange(req, timeout=timeout)
        except: return None
    
    def pbft_request(self, peer, req, timeout=5.0):
        try:
            with self._get_channel(peer) as chan:
                return pbft_pb2_grpc.PBFTServiceStub(chan).Request(req, timeout=timeout)
        except: return None
    
    def pbft_get_status(self, peer, timeout=1.0):
        try:
            with self._get_channel(peer) as chan:
                return pbft_pb2_grpc.PBFTServiceStub(chan).GetStatus(
                    pbft_pb2.StatusRequest(), timeout=timeout)
        except: return None
