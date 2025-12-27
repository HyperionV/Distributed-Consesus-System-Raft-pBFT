"""
pBFT (Practical Byzantine Fault Tolerance) Consensus Implementation
Tolerates f Byzantine faults with 3f+1 nodes (f=1 means 4 nodes)
"""
import threading
import time
import hashlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from generated import pbft_pb2
from consensus.state_machine import StateMachine


class PBFTConsensus:
    """
    Practical Byzantine Fault Tolerance consensus.
    Requires 3f+1 nodes to tolerate f Byzantine faults.
    Uses three-phase protocol: PrePrepare -> Prepare -> Commit
    """
    
    def __init__(self, node, f=1):
        self.node = node
        self.logger = node.logger
        self.f = f  # Number of Byzantine faults to tolerate
        self.quorum = 2 * f + 1  # 2f+1 for consensus
        
        # State
        self.view = 0  # Current view (determines primary)
        self.sequence = 0  # Last assigned sequence number
        self.state_machine = StateMachine(logger=self.logger)
        
        # Message logs per (view, sequence)
        self.pre_prepares = {}  # (v, n) -> PrePrepareRequest
        self.prepares = defaultdict(set)  # (v, n, digest) -> set of replica_ids
        self.commits = defaultdict(set)  # (v, n, digest) -> set of replica_ids
        
        # Execution tracking
        self.executed = set()  # Set of executed (v, n) pairs
        self.pending_requests = {}  # digest -> ClientRequest
        
        # View change
        self.view_change_votes = defaultdict(set)  # new_view -> set of replica_ids
        self.state = "normal"  # "normal" or "view-change"
        self.view_change_timeout = 5.0  # seconds
        self.last_activity = time.time()
        
        # Thread control
        self.lock = threading.Lock()
        self.running = False
        self.timeout_thread = None
        
        # Malicious mode for testing
        self.malicious = False
    
    @property
    def primary_id(self):
        """Current primary (leader) determined by view number"""
        total_nodes = len(self.node.peers) + 1
        return (self.view % total_nodes) + 1  # 1-indexed node IDs
    
    @property
    def is_primary(self):
        """Check if this node is the current primary"""
        return self.node.node_id == self.primary_id
    
    def _digest(self, message):
        """Compute SHA-256 digest of a message"""
        if self.malicious:
            # Malicious node sends garbage digest
            return hashlib.sha256(b"garbage").hexdigest()
        return hashlib.sha256(message.encode()).hexdigest()
    
    def start(self):
        """Start pBFT consensus"""
        self.running = True
        self.timeout_thread = threading.Thread(target=self._timeout_loop, daemon=True)
        self.timeout_thread.start()
        self.logger.info(f"pBFT started: view={self.view}, primary=Node {self.primary_id}, is_primary={self.is_primary}")
    
    def stop(self):
        """Stop pBFT consensus"""
        self.running = False
        if self.timeout_thread:
            self.timeout_thread.join(timeout=1.0)
    
    def _timeout_loop(self):
        """Monitor for primary failure and trigger view change"""
        while self.running:
            time.sleep(0.5)
            
            if self.state == "normal" and not self.is_primary:
                elapsed = time.time() - self.last_activity
                if elapsed > self.view_change_timeout:
                    self.logger.warning(f"Primary timeout ({elapsed:.1f}s), initiating view change")
                    self._initiate_view_change()
    
    def _initiate_view_change(self):
        """Start view change protocol"""
        with self.lock:
            self.state = "view-change"
            new_view = self.view + 1
            self.view_change_votes[new_view].add(self.node.node_id)
        
        # Broadcast ViewChange to all
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        
        request = pbft_pb2.ViewChangeRequest(
            new_view=new_view,
            last_sequence=self.sequence,
            replica_id=self.node.node_id
        )
        
        for peer in self.node.peers:
            try:
                comm.pbft_view_change(peer, request)
            except:
                pass
    
    def handle_client_request(self, request):
        """Handle incoming client request (primary only processes)"""
        with self.lock:
            self.last_activity = time.time()
            
            if not self.is_primary:
                # Forward to primary or reject
                return pbft_pb2.ClientReply(
                    view=self.view,
                    timestamp=request.timestamp,
                    replica_id=self.node.node_id,
                    success=False,
                    result=f"Not primary. Try Node {self.primary_id}"
                )
            
            # Assign sequence number
            self.sequence += 1
            seq = self.sequence
            digest = self._digest(request.operation)
            
            # Store request
            self.pending_requests[digest] = request
            
            # Create PrePrepare
            pre_prepare = pbft_pb2.PrePrepareRequest(
                view=self.view,
                sequence=seq,
                digest=digest,
                request=request,
                primary_id=self.node.node_id
            )
            
            # Store locally
            self.pre_prepares[(self.view, seq)] = pre_prepare
        
        # Broadcast PrePrepare to all replicas
        self._broadcast_pre_prepare(pre_prepare)
        
        # Wait for execution (simplified - real impl would use callbacks)
        for _ in range(50):  # 5 second timeout
            time.sleep(0.1)
            with self.lock:
                if (self.view, seq) in self.executed:
                    return pbft_pb2.ClientReply(
                        view=self.view,
                        timestamp=request.timestamp,
                        replica_id=self.node.node_id,
                        success=True,
                        result=f"Committed at sequence {seq}"
                    )
        
        return pbft_pb2.ClientReply(
            view=self.view,
            timestamp=request.timestamp,
            replica_id=self.node.node_id,
            success=False,
            result="Timeout waiting for commit"
        )
    
    def _broadcast_pre_prepare(self, pre_prepare):
        """Primary broadcasts PrePrepare to all replicas"""
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        
        for peer in self.node.peers:
            try:
                comm.pbft_pre_prepare(peer, pre_prepare)
            except:
                pass
    
    def handle_pre_prepare(self, request):
        """Handle PrePrepare from primary"""
        with self.lock:
            self.last_activity = time.time()
            v, n, d = request.view, request.sequence, request.digest
            
            # Validate
            if request.view != self.view:
                return pbft_pb2.PrePrepareReply(accepted=False)
            
            if request.primary_id != self.primary_id:
                return pbft_pb2.PrePrepareReply(accepted=False)
            
            # Check digest
            expected_digest = self._digest(request.request.operation)
            if d != expected_digest:
                self.logger.warning(f"Digest mismatch from primary (Byzantine?)")
                return pbft_pb2.PrePrepareReply(accepted=False)
            
            # Accept PrePrepare
            self.pre_prepares[(v, n)] = request
            self.pending_requests[d] = request.request
            
            # Add our own Prepare vote
            self.prepares[(v, n, d)].add(self.node.node_id)
        
        # Broadcast Prepare to all
        self._broadcast_prepare(v, n, d)
        
        return pbft_pb2.PrePrepareReply(accepted=True)
    
    def _broadcast_prepare(self, view, seq, digest):
        """Broadcast Prepare to all replicas"""
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        
        prepare = pbft_pb2.PrepareRequest(
            view=view,
            sequence=seq,
            digest=digest,
            replica_id=self.node.node_id
        )
        
        for peer in self.node.peers:
            try:
                comm.pbft_prepare(peer, prepare)
            except:
                pass
    
    def handle_prepare(self, request):
        """Handle Prepare from replica"""
        v, n, d = request.view, request.sequence, request.digest
        
        with self.lock:
            self.last_activity = time.time()
            
            if v != self.view:
                return pbft_pb2.PrepareReply(accepted=False)
            
            # Record Prepare
            self.prepares[(v, n, d)].add(request.replica_id)
            
            # Check if we have quorum
            if len(self.prepares[(v, n, d)]) >= self.quorum:
                # Add our commit vote
                self.commits[(v, n, d)].add(self.node.node_id)
        
        # If quorum reached, broadcast Commit
        if len(self.prepares[(v, n, d)]) >= self.quorum:
            self._broadcast_commit(v, n, d)
        
        return pbft_pb2.PrepareReply(accepted=True)
    
    def _broadcast_commit(self, view, seq, digest):
        """Broadcast Commit to all replicas"""
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        
        commit = pbft_pb2.CommitRequest(
            view=view,
            sequence=seq,
            digest=digest,
            replica_id=self.node.node_id
        )
        
        for peer in self.node.peers:
            try:
                comm.pbft_commit(peer, commit)
            except:
                pass
    
    def handle_commit(self, request):
        """Handle Commit from replica"""
        v, n, d = request.view, request.sequence, request.digest
        
        with self.lock:
            self.last_activity = time.time()
            
            if v != self.view:
                return pbft_pb2.CommitReply(accepted=False)
            
            # Record Commit
            self.commits[(v, n, d)].add(request.replica_id)
            
            # Check if we have quorum and haven't executed yet
            if len(self.commits[(v, n, d)]) >= self.quorum and (v, n) not in self.executed:
                # Execute!
                self._execute(v, n, d)
        
        return pbft_pb2.CommitReply(accepted=True)
    
    def _execute(self, view, seq, digest):
        """Execute a committed request"""
        if (view, seq) in self.executed:
            return
        
        self.executed.add((view, seq))
        
        request = self.pending_requests.get(digest)
        if request:
            success, result = self.state_machine.apply(request.operation)
            self.logger.info(f"pBFT executed seq={seq}: {request.operation} -> {result}")
    
    def handle_view_change(self, request):
        """Handle ViewChange from replica"""
        with self.lock:
            new_view = request.new_view
            
            if new_view <= self.view:
                return pbft_pb2.ViewChangeReply(accepted=False)
            
            self.view_change_votes[new_view].add(request.replica_id)
            
            # Check if we have quorum for view change
            if len(self.view_change_votes[new_view]) >= self.quorum:
                self._complete_view_change(new_view)
        
        return pbft_pb2.ViewChangeReply(accepted=True)
    
    def _complete_view_change(self, new_view):
        """Complete view change to new view"""
        self.view = new_view
        self.state = "normal"
        self.last_activity = time.time()
        self.logger.info(f"View change complete: view={new_view}, new primary=Node {self.primary_id}")
    
    def get_status(self):
        """Get current status"""
        with self.lock:
            return {
                'view': self.view,
                'sequence': self.sequence,
                'primary_id': self.primary_id,
                'is_primary': self.is_primary,
                'state': self.state,
                'executed_count': len(self.executed)
            }
    
    def set_malicious(self, enabled):
        """Enable/disable malicious mode for testing"""
        self.malicious = enabled
        self.logger.warning(f"Malicious mode: {enabled}")
