import threading
import time
import random
import logging

from generated import raft_pb2

from consensus.state_machine import StateMachine
from storage.wal import WAL


class RaftConsensus:
    """
    Raft consensus algorithm implementation.
    Handles leader election, log replication, and state machine application.
    """
    
    def __init__(self, node):
        self.node = node
        self.logger = node.logger
        
        # Initialize WAL and state machine
        self.wal = WAL(node.node_id, logger=self.logger)
        self.state_machine = StateMachine(logger=self.logger)
        
        # Load persisted state
        saved_term, saved_voted_for, saved_log = self.wal.load()
        
        # Persistent state (saved to WAL)
        self.current_term = saved_term
        self.voted_for = saved_voted_for
        self.log = saved_log  # List of {term, command}
        
        # Volatile state - all servers
        self.state = 'Follower'  # Follower, Candidate, Leader
        self.commit_index = 0    # Index of highest log entry known to be committed
        self.last_applied = 0    # Index of highest log entry applied to state machine
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_timeout()
        
        # Volatile state - leader only (reinitialized after election)
        self.next_index = {}   # For each peer: next log index to send
        self.match_index = {}  # For each peer: highest log index known to be replicated
        
        # Thread control
        self.lock = threading.Lock()
        self.running = False
        self.election_thread = None
        self.heartbeat_thread = None
        self.apply_thread = None
        
    def _random_timeout(self):
        """Generate random election timeout between 300-600ms"""
        return random.uniform(0.3, 0.6)
    
    def _persist(self):
        """Persist current state to WAL (must hold lock)"""
        self.wal.save(self.current_term, self.voted_for, self.log)
    
    def _last_log_index(self):
        """Get index of last log entry (1-indexed, 0 if empty)"""
        return len(self.log)
    
    def _last_log_term(self):
        """Get term of last log entry (0 if empty)"""
        if not self.log:
            return 0
        return self.log[-1]['term']
    
    def start(self):
        """Start the Raft consensus threads"""
        self.running = True
        
        # Apply any committed but not applied entries from WAL
        self._apply_committed_entries()
        
        self.election_thread = threading.Thread(target=self._run_election_timer, daemon=True)
        self.election_thread.start()
        
        self.apply_thread = threading.Thread(target=self._run_apply_loop, daemon=True)
        self.apply_thread.start()
        
        self.logger.info(f"Raft started: term={self.current_term}, log_len={len(self.log)}")
    
    def stop(self):
        """Stop all Raft threads"""
        self.running = False
        if self.election_thread:
            self.election_thread.join(timeout=1.0)
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=1.0)
        if self.apply_thread:
            self.apply_thread.join(timeout=1.0)
    
    def _run_election_timer(self):
        """Background thread that checks for election timeout"""
        while self.running:
            time.sleep(0.01)  # Check every 10ms
            
            with self.lock:
                if self.state == 'Leader':
                    continue  # Leaders don't timeout
                
                elapsed = time.time() - self.last_heartbeat
                if elapsed >= self.election_timeout:
                    self._start_election()
    
    def _run_apply_loop(self):
        """Background thread that applies committed entries to state machine"""
        while self.running:
            time.sleep(0.01)  # Check every 10ms
            self._apply_committed_entries()
    
    def _apply_committed_entries(self):
        """Apply any committed but not yet applied entries"""
        with self.lock:
            while self.last_applied < self.commit_index and self.last_applied < len(self.log):
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                command = entry['command']
        
        # Apply outside lock to avoid blocking
        if 'command' in dir():
            success, result = self.state_machine.apply(command)
            self.logger.info(f"Applied log[{self.last_applied}]: {command} -> {result}")

    def _start_election(self):
        """Transition to Candidate and start election. Must be called with lock held."""
        self.state = 'Candidate'
        self.current_term += 1
        self.voted_for = self.node.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_timeout()
        
        term = self.current_term
        candidate_id = self.node.node_id
        last_log_index = self._last_log_index()
        last_log_term = self._last_log_term()
        
        self._persist()  # Save vote for self
        
        self.logger.info(f"Starting election for term {term}")
        
        # Release lock before making RPC calls
        self.lock.release()
        
        votes_received = 1  # Vote for self
        votes_needed = (len(self.node.peers) + 1) // 2 + 1
        
        from infrastructure.comms import Communicator
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        comm = Communicator(self.node)
        request = raft_pb2.RequestVoteArgs(
            term=term,
            candidate_id=candidate_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term
        )

        with ThreadPoolExecutor(max_workers=len(self.node.peers)) as executor:
            futures = {executor.submit(comm.request_vote, peer, request, timeout=0.1): peer for peer in self.node.peers}
            
            for future in as_completed(futures):
                response = future.result()
                
                if response and response.vote_granted:
                    votes_received += 1
                    self.logger.info(f"Received vote (total: {votes_received})")
                    
                    if votes_received >= votes_needed:
                        self._become_leader(term)
                        break
                elif response and response.term > term:
                    self._step_down(response.term)
                    break
        
        self.lock.acquire()
        if self.state == 'Leader':
            return
            
        if self.state == 'Candidate' and self.current_term == term:
            self.logger.info(f"Election failed/split: only got {votes_received} votes")

    def _become_leader(self, term):
        """Transition to Leader state"""
        with self.lock:
            if self.current_term == term and self.state == 'Candidate':
                self.state = 'Leader'
                self.logger.info(f"🎉 WON ELECTION! Became Leader for term {term}")
                
                # Initialize leader state for each peer
                for peer in self.node.peers:
                    self.next_index[peer['id']] = len(self.log) + 1
                    self.match_index[peer['id']] = 0
                
                # Start heartbeat thread
                if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
                    self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
                    self.heartbeat_thread.start()
    
    def _send_heartbeats(self):
        """Leader sends periodic heartbeats/log replication to followers"""
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        
        while self.running:
            with self.lock:
                if self.state != 'Leader':
                    return
                term = self.current_term
                commit_index = self.commit_index
                log_snapshot = list(self.log)  # Copy for thread safety
                next_index_snapshot = dict(self.next_index)
            
            # Send AppendEntries to all peers
            for peer in self.node.peers:
                peer_id = peer['id']
                next_idx = next_index_snapshot.get(peer_id, 1)
                
                # Calculate entries to send
                prev_log_index = next_idx - 1
                prev_log_term = 0
                if prev_log_index > 0 and prev_log_index <= len(log_snapshot):
                    prev_log_term = log_snapshot[prev_log_index - 1]['term']
                
                # Get entries to send (from next_index to end)
                entries = []
                for i in range(next_idx - 1, len(log_snapshot)):
                    entry = log_snapshot[i]
                    entries.append(raft_pb2.Entry(term=entry['term'], command=entry['command']))
                
                request = raft_pb2.AppendEntriesArgs(
                    term=term,
                    leader_id=self.node.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=commit_index
                )
                
                response = comm.append_entries(peer, request, timeout=0.1)
                
                if response:
                    if response.term > term:
                        self._step_down(response.term)
                        return
                    
                    with self.lock:
                        if response.success:
                            # Update next_index and match_index
                            self.next_index[peer_id] = len(log_snapshot) + 1
                            self.match_index[peer_id] = len(log_snapshot)
                        else:
                            # Decrement next_index and retry
                            if self.next_index[peer_id] > 1:
                                self.next_index[peer_id] -= 1
            
            # Update commit_index based on match_index
            self._update_commit_index()
            
            time.sleep(0.05)  # 50ms heartbeat interval
    
    def _update_commit_index(self):
        """Leader: advance commit_index if majority have replicated"""
        with self.lock:
            if self.state != 'Leader':
                return
            
            # For each N > commit_index, if majority have match_index >= N
            # and log[N].term == currentTerm, set commit_index = N
            for n in range(len(self.log), self.commit_index, -1):
                if self.log[n - 1]['term'] != self.current_term:
                    continue
                
                # Count how many peers have replicated this entry
                count = 1  # Leader has it
                for peer in self.node.peers:
                    if self.match_index.get(peer['id'], 0) >= n:
                        count += 1
                
                if count > (len(self.node.peers) + 1) // 2:
                    self.commit_index = n
                    self.logger.info(f"Committed up to index {n}")
                    break
    
    def _step_down(self, term):
        """Revert to Follower state when discovering higher term"""
        with self.lock:
            if term > self.current_term:
                self.logger.info(f"Stepping down: discovered term {term} > {self.current_term}")
                self.current_term = term
                self.voted_for = None
                self.state = 'Follower'
                self.last_heartbeat = time.time()
                self.election_timeout = self._random_timeout()
                self._persist()
    
    def handle_request_vote(self, request):
        """Handle RequestVote RPC"""
        with self.lock:
            # Rule 1: Reply false if term < currentTerm
            if request.term < self.current_term:
                return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)
            
            # Update term if we see a higher one
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = 'Follower'
                self._persist()
            
            # Check log freshness: candidate's log must be at least as up-to-date
            last_log_term = self._last_log_term()
            last_log_index = self._last_log_index()
            
            log_ok = (request.last_log_term > last_log_term or
                     (request.last_log_term == last_log_term and request.last_log_index >= last_log_index))
            
            vote_granted = False
            if (self.voted_for is None or self.voted_for == request.candidate_id) and log_ok:
                vote_granted = True
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()
                self.election_timeout = self._random_timeout()
                self._persist()
                self.logger.info(f"Granted vote to Node {request.candidate_id} for term {request.term}")
            
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=vote_granted)
    
    def handle_append_entries(self, request):
        """Handle AppendEntries RPC (heartbeat or log replication)"""
        with self.lock:
            # Rule 1: Reply false if term < currentTerm
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
            
            # Update term and step down if necessary
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self._persist()
            
            # Become follower if we're a candidate
            if self.state != 'Follower':
                self.logger.info(f"Received AppendEntries from Leader (Node {request.leader_id}), stepping down")
                self.state = 'Follower'
            
            # Reset election timer
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_timeout()
            
            # Rule 2: Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
            if request.prev_log_index > 0:
                if len(self.log) < request.prev_log_index:
                    return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
                if self.log[request.prev_log_index - 1]['term'] != request.prev_log_term:
                    # Delete conflicting entry and all that follow
                    self.log = self.log[:request.prev_log_index - 1]
                    self._persist()
                    return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
            
            # Rule 3 & 4: Append new entries (if any)
            log_modified = False
            for i, entry in enumerate(request.entries):
                index = request.prev_log_index + i + 1  # 1-indexed
                
                if index <= len(self.log):
                    # Entry exists - check for conflict
                    if self.log[index - 1]['term'] != entry.term:
                        # Delete this and all following entries
                        self.log = self.log[:index - 1]
                        self.log.append({'term': entry.term, 'command': entry.command})
                        log_modified = True
                else:
                    # Append new entry
                    self.log.append({'term': entry.term, 'command': entry.command})
                    log_modified = True
            
            if log_modified:
                self._persist()
                self.logger.debug(f"Log updated: {len(self.log)} entries")
            
            # Rule 5: Update commitIndex
            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, len(self.log))
            
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=True)
    
    def submit_command(self, command):
        """
        Submit a command to be replicated (called on leader only).
        Returns: (success, message)
        """
        with self.lock:
            if self.state != 'Leader':
                return False, "Not the leader"
            
            # Append to local log
            entry = {'term': self.current_term, 'command': command}
            self.log.append(entry)
            self._persist()
            
            index = len(self.log)
            self.logger.info(f"Appended command at index {index}: {command}")
            
            return True, f"Command appended at index {index}"
    
    def get_state(self):
        """Get current state for debugging/testing"""
        with self.lock:
            return {
                'state': self.state,
                'term': self.current_term,
                'voted_for': self.voted_for,
                'log_length': len(self.log),
                'commit_index': self.commit_index,
                'last_applied': self.last_applied
            }
