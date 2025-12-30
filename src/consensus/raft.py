import threading
import time
import random
from generated import raft_pb2
from consensus.state_machine import StateMachine
from storage.wal import WAL

class RaftConsensus:
    """
    Raft consensus algorithm implementation handling leader election and log replication.
    """
    
    def __init__(self, node):
        self.node = node
        self.wal = WAL(node.node_id)
        self.state_machine = StateMachine(node=node)
        
        saved_term, saved_voted_for, saved_log = self.wal.load()
        self.current_term = saved_term
        self.voted_for = saved_voted_for
        self.log = saved_log
        
        self.state = 'Follower'
        self.commit_index = 0
        self.last_applied = 0
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_timeout()
        
        self.next_index = {}
        self.match_index = {}
        
        self.lock = threading.Lock()
        self.running = False
        self.election_thread = None
        self.heartbeat_thread = None
        self.apply_thread = None
        
    def _random_timeout(self):
        """Generate a random election timeout between 300ms and 600ms."""
        return random.uniform(0.3, 0.6)
    
    def _persist(self):
        """Persist current state to WAL."""
        self.wal.save(self.current_term, self.voted_for, self.log)
    
    def _last_log_index(self):
        return len(self.log)
    
    def _last_log_term(self):
        return self.log[-1]['term'] if self.log else 0
    
    def start(self):
        """Start Raft consensus background threads."""
        self.running = True
        self._apply_committed_entries()
        
        self.election_thread = threading.Thread(target=self._run_election_timer, daemon=True)
        self.election_thread.start()
        
        self.apply_thread = threading.Thread(target=self._run_apply_loop, daemon=True)
        self.apply_thread.start()
        
        self.node.log(f"Raft started: term={self.current_term}, log_len={len(self.log)}")
    
    def stop(self):
        """Stop all background threads."""
        self.running = False
        for t in [self.election_thread, self.heartbeat_thread, self.apply_thread]:
            if t: t.join(timeout=1.0)
    
    def _run_election_timer(self):
        """Monitor for election timeouts."""
        while self.running:
            time.sleep(0.01)
            with self.lock:
                if self.state == 'Leader': continue
                if time.time() - self.last_heartbeat >= self.election_timeout:
                    self._start_election()
    
    def _run_apply_loop(self):
        """Periodically apply committed entries to the state machine."""
        while self.running:
            time.sleep(0.01)
            self._apply_committed_entries()
    
    def _apply_committed_entries(self):
        """Apply entries that are committed but not yet applied."""
        with self.lock:
            while self.last_applied < self.commit_index and self.last_applied < len(self.log):
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                command = entry['command']
                success, result = self.state_machine.apply(command)
                self.node.log(f"Applied log[{self.last_applied}]: {command} -> {result}")

    def _start_election(self):
        """Transition to Candidate and broadcast RequestVote RPCs."""
        self.state = 'Candidate'
        self.current_term += 1
        self.voted_for = self.node.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_timeout()
        
        term = self.current_term
        candidate_id = self.node.node_id
        last_index = self._last_log_index()
        last_term = self._last_log_term()
        
        self._persist()
        self.node.log(f"Starting election for term {term}")
        
        self.lock.release()
        
        votes = 1
        needed = (len(self.node.peers) + 1) // 2 + 1
        
        from infrastructure.comms import Communicator
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        comm = Communicator(self.node)
        req = raft_pb2.RequestVoteArgs(
            term=term, 
            candidate_id=candidate_id, 
            last_log_index=last_index, 
            last_log_term=last_term
        )

        with ThreadPoolExecutor(max_workers=len(self.node.peers)) as executor:
            futures = {executor.submit(comm.request_vote, p, req, timeout=0.1): p for p in self.node.peers}
            for fut in as_completed(futures):
                res = fut.result()
                if res and res.vote_granted:
                    votes += 1
                    if votes >= needed:
                        self._become_leader(term)
                        break
                elif res and res.term > term:
                    self._step_down(res.term)
                    break
        
        self.lock.acquire()
        if self.state == 'Candidate' and self.current_term == term:
            self.node.log(f"Election failed/split for term {term}")

    def _become_leader(self, term):
        """Transition to Leader state and start heartbeats."""
        with self.lock:
            if self.current_term == term and self.state == 'Candidate':
                self.state = 'Leader'
                self.node.log(f"WON ELECTION! Became Leader for term {term}")
                for p in self.node.peers:
                    self.next_index[p['id']] = len(self.log) + 1
                    self.match_index[p['id']] = 0
                if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
                    self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
                    self.heartbeat_thread.start()
    
    def _send_heartbeats(self):
        """Background thread for the leader to send heartbeats and synchronize logs."""
        from infrastructure.comms import Communicator
        comm = Communicator(self.node)
        while self.running:
            with self.lock:
                if self.state != 'Leader': return
                term, commit_index = self.current_term, self.commit_index
                log_snap, next_snap = list(self.log), dict(self.next_index)
            
            for p in self.node.peers:
                p_id = p['id']
                nxt = next_snap.get(p_id, 1)
                prev_idx = nxt - 1
                prev_term = log_snap[prev_idx-1]['term'] if prev_idx > 0 and prev_idx <= len(log_snap) else 0
                entries = [raft_pb2.Entry(term=e['term'], command=e['command']) for e in log_snap[nxt-1:]]
                
                req = raft_pb2.AppendEntriesArgs(
                    term=term, 
                    leader_id=self.node.node_id, 
                    prev_log_index=prev_idx, 
                    prev_log_term=prev_term, 
                    entries=entries, 
                    leader_commit=commit_index
                )
                res = comm.append_entries(p, req, timeout=0.1)
                
                if res:
                    if res.term > term:
                        self._step_down(res.term)
                        return
                    with self.lock:
                        if res.success:
                            self.next_index[p_id] = len(log_snap) + 1
                            self.match_index[p_id] = len(log_snap)
                        elif self.next_index[p_id] > 1:
                            self.next_index[p_id] -= 1
            
            self._update_commit_index()
            time.sleep(0.05)
    
    def _update_commit_index(self):
        """Advance commit index if a majority of nodes have replicated an entry."""
        with self.lock:
            if self.state != 'Leader': return
            for n in range(len(self.log), self.commit_index, -1):
                if self.log[n-1]['term'] != self.current_term: continue
                count = 1 + sum(1 for p in self.node.peers if self.match_index.get(p['id'], 0) >= n)
                if count > (len(self.node.peers) + 1) // 2:
                    self.commit_index = n
                    self.node.log(f"Committed up to index {n}")
                    break
    
    def _step_down(self, term):
        """Revert to Follower state upon discovering a higher term."""
        with self.lock:
            if term > self.current_term:
                self.node.log(f"Stepping down to term {term}")
                self.current_term, self.voted_for, self.state = term, None, 'Follower'
                self.last_heartbeat = time.time()
                self.election_timeout = self._random_timeout()
                self._persist()
    
    def handle_request_vote(self, req):
        """Process RequestVote RPC from a Candidate."""
        with self.lock:
            if req.term < self.current_term:
                return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)
            if req.term > self.current_term:
                self.current_term, self.voted_for, self.state = req.term, None, 'Follower'
                self._persist()
            
            l_term, l_idx = self._last_log_term(), self._last_log_index()
            log_ok = (req.last_log_term > l_term or (req.last_log_term == l_term and req.last_log_index >= l_idx))
            
            granted = False
            if (self.voted_for is None or self.voted_for == req.candidate_id) and log_ok:
                granted = True
                self.voted_for, self.last_heartbeat = req.candidate_id, time.time()
                self.election_timeout = self._random_timeout()
                self._persist()
                self.node.log(f"Voted for Node {req.candidate_id} (term {req.term})")
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=granted)
    
    def handle_append_entries(self, req):
        """Process AppendEntries RPC (heartbeat or replication) from a Leader."""
        with self.lock:
            if req.term < self.current_term:
                return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
            if req.term > self.current_term:
                self.current_term, self.voted_for = req.term, None
                self._persist()
            
            if self.state != 'Follower': self.state = 'Follower'
            self.last_heartbeat, self.election_timeout = time.time(), self._random_timeout()
            
            if req.prev_log_index > 0:
                if len(self.log) < req.prev_log_index or self.log[req.prev_log_index-1]['term'] != req.prev_log_term:
                    if len(self.log) >= req.prev_log_index:
                        self.log = self.log[:req.prev_log_index-1]
                        self._persist()
                    return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
            
            modified = False
            for i, ent in enumerate(req.entries):
                idx = req.prev_log_index + i + 1
                if idx <= len(self.log):
                    if self.log[idx-1]['term'] != ent.term:
                        self.log = self.log[:idx-1]
                        self.log.append({'term': ent.term, 'command': ent.command})
                        modified = True
                else:
                    self.log.append({'term': ent.term, 'command': ent.command})
                    modified = True
            
            if modified: self._persist()
            if req.leader_commit > self.commit_index:
                self.commit_index = min(req.leader_commit, len(self.log))
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=True)
    
    def submit_command(self, command):
        """Submit a command to the leader for replication."""
        with self.lock:
            if self.state != 'Leader': return False, "Not leader"
            self.log.append({'term': self.current_term, 'command': command})
            self._persist()
            self.node.log(f"Appended command at index {len(self.log)}: {command}")
            return True, f"Appended at {len(self.log)}"
    
    def get_state(self):
        """Get the current consensus state for monitoring."""
        with self.lock:
            return {
                'state': self.state, 
                'term': self.current_term, 
                'voted_for': self.voted_for, 
                'log_length': len(self.log), 
                'commit_index': self.commit_index, 
                'last_applied': self.last_applied
            }
