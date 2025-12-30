import threading
import time
import hashlib
from collections import defaultdict
from generated import pbft_pb2
from consensus.state_machine import StateMachine

class PBFTConsensus:
    """
    Practical Byzantine Fault Tolerance (pBFT) implementation.
    Tolerates up to f Byzantine nodes in a cluster of 3f+1 total nodes.
    """
    
    def __init__(self, node, f=1):
        self.node = node
        self.f = f
        self.quorum = 2 * f + 1
        self.view = 0
        self.sequence = 0
        self.state_machine = StateMachine(node=node)
        
        self.pre_prepares = {}
        self.prepares = defaultdict(set)
        self.commits = defaultdict(set)
        self.executed = set()
        self.pending_requests = {}
        self.view_change_votes = defaultdict(set)
        
        self.state = "normal"
        self.view_change_timeout = 10.0
        self.last_activity = time.time()
        self.lock = threading.RLock()
        self.running = False
        self.malicious = False
    
    @property
    def primary_id(self):
        return (self.view % (len(self.node.peers) + 1)) + 1
    
    @property
    def is_primary(self):
        return self.node.node_id == self.primary_id
    
    def _digest(self, msg):
        """SHA-256 digest. Malicious nodes return garbage."""
        if self.malicious: return hashlib.sha256(b"malicious").hexdigest()
        return hashlib.sha256(msg.encode()).hexdigest()
    
    def start(self):
        self.running = True
        threading.Thread(target=self._timeout_loop, daemon=True).start()
        self.node.log(f"pBFT started: view={self.view}, primary=Node {self.primary_id}")
    
    def stop(self):
        self.running = False
    
    def _timeout_loop(self):
        while self.running:
            time.sleep(1.0)
            if self.state == "normal" and not self.is_primary:
                if time.time() - self.last_activity > self.view_change_timeout:
                    self.node.log("Primary timeout, starting view change", level="WARN")
                    self._initiate_view_change()
    
    def _initiate_view_change(self):
        with self.lock:
            self.state = "view-change"
            new_v = self.view + 1
            self.view_change_votes[new_v].add(self.node.node_id)
        req = pbft_pb2.ViewChangeRequest(new_view=new_v, last_sequence=self.sequence, 
                                          replica_id=self.node.node_id)
        self._broadcast('view_change', req)
    
    def _broadcast(self, msg_type, req):
        """Broadcast message to all peers."""
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        for peer in self.node.peers:
            try:
                if msg_type == 'pre_prepare': comm.pbft_pre_prepare(peer, req, timeout=0.5)
                elif msg_type == 'prepare': comm.pbft_prepare(peer, req, timeout=0.5)
                elif msg_type == 'commit': comm.pbft_commit(peer, req, timeout=0.5)
                elif msg_type == 'view_change': comm.pbft_view_change(peer, req, timeout=0.5)
            except: pass
    
    def handle_client_request(self, req):
        """Primary: assign sequence number and initiate consensus."""
        with self.lock:
            self.last_activity = time.time()
            if not self.is_primary:
                return pbft_pb2.ClientReply(view=self.view, success=False, 
                    result=f"Redirect to Node {self.primary_id}")
            
            self.sequence += 1
            seq = self.sequence
            digest = self._digest(req.operation)
            self.pending_requests[digest] = req
            
            pp = pbft_pb2.PrePrepareRequest(view=self.view, sequence=seq, digest=digest, 
                                           request=req, primary_id=self.node.node_id)
            self.pre_prepares[(self.view, seq)] = pp
            # Primary counts itself
            self.prepares[(self.view, seq, digest)].add(self.node.node_id)
        
        # Broadcast PrePrepare (outside lock)
        self._broadcast('pre_prepare', pp)
        
        # Primary also broadcasts its Prepare (so replicas can count it)
        self._broadcast('prepare', pbft_pb2.PrepareRequest(
            view=self.view, sequence=seq, digest=digest, replica_id=self.node.node_id))
        
        # Wait for execution
        for _ in range(80):
            time.sleep(0.1)
            with self.lock:
                if (self.view, seq) in self.executed:
                    return pbft_pb2.ClientReply(view=self.view, success=True, 
                        result=f"Committed seq {seq}")
        
        return pbft_pb2.ClientReply(view=self.view, success=False, result="Timeout")
    
    def handle_pre_prepare(self, req):
        """Validate PrePrepare from primary and broadcast Prepare."""
        v, n, d = req.view, req.sequence, req.digest
        
        with self.lock:
            self.last_activity = time.time()
            
            if v != self.view or req.primary_id != self.primary_id:
                return pbft_pb2.PrePrepareReply(accepted=False)
            
            expected = self._digest(req.request.operation)
            if d != expected:
                self.node.log(f"Digest mismatch seq {n}", level="WARN")
                return pbft_pb2.PrePrepareReply(accepted=False)
            
            self.pre_prepares[(v, n)] = req
            self.pending_requests[d] = req.request
            self.prepares[(v, n, d)].add(self.node.node_id)
        
        # Broadcast Prepare (outside lock)
        self._broadcast('prepare', pbft_pb2.PrepareRequest(
            view=v, sequence=n, digest=d, replica_id=self.node.node_id))
        
        return pbft_pb2.PrePrepareReply(accepted=True)
    
    def handle_prepare(self, req):
        """Collect Prepare votes. Start Commit phase when quorum reached."""
        v, n, d = req.view, req.sequence, req.digest
        should_commit = False
        
        with self.lock:
            self.last_activity = time.time()
            if v != self.view:
                return pbft_pb2.PrepareReply(accepted=False)
            
            self.prepares[(v, n, d)].add(req.replica_id)
            count = len(self.prepares[(v, n, d)])
            
            if count >= self.quorum and self.node.node_id not in self.commits[(v, n, d)]:
                self.commits[(v, n, d)].add(self.node.node_id)
                should_commit = True
        
        if should_commit:
            self._broadcast('commit', pbft_pb2.CommitRequest(
                view=v, sequence=n, digest=d, replica_id=self.node.node_id))
        
        return pbft_pb2.PrepareReply(accepted=True)
    
    def handle_commit(self, req):
        """Collect Commit votes. Execute when quorum reached."""
        v, n, d = req.view, req.sequence, req.digest
        
        with self.lock:
            self.last_activity = time.time()
            if v != self.view:
                return pbft_pb2.CommitReply(accepted=False)
            
            self.commits[(v, n, d)].add(req.replica_id)
            
            if len(self.commits[(v, n, d)]) >= self.quorum and (v, n) not in self.executed:
                self.executed.add((v, n))
                request = self.pending_requests.get(d)
                if request:
                    _, res = self.state_machine.apply(request.operation)
                    self.node.log(f"COMMIT seq={n}: {request.operation} -> {res}")
        
        return pbft_pb2.CommitReply(accepted=True)
    
    def handle_view_change(self, req):
        with self.lock:
            if req.new_view <= self.view:
                return pbft_pb2.ViewChangeReply(accepted=False)
            
            self.view_change_votes[req.new_view].add(req.replica_id)
            
            if len(self.view_change_votes[req.new_view]) >= self.quorum:
                self.view = req.new_view
                self.state = "normal"
                self.last_activity = time.time()
                self.node.log(f"View change complete: view={self.view}")
        
        return pbft_pb2.ViewChangeReply(accepted=True)
    
    def get_status(self):
        with self.lock:
            return {
                'view': self.view, 
                'sequence': self.sequence, 
                'primary_id': self.primary_id,
                'is_primary': self.is_primary, 
                'state': self.state
            }
    
    def set_malicious(self, val):
        self.malicious = val
        self.node.log(f"Malicious mode: {val}", level="WARN")
