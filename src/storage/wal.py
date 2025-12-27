"""
Write-Ahead Log (WAL) - Persistence Layer
Saves Raft state to disk for crash recovery
"""
import json
import os
import logging


class WAL:
    """
    Write-Ahead Log for Raft state persistence.
    Uses atomic writes (temp file + rename) for safety.
    """
    
    def __init__(self, node_id, data_dir="data", logger=None):
        self.node_id = node_id
        self.data_dir = data_dir
        self.filepath = os.path.join(data_dir, f"node_{node_id}_wal.json")
        self.logger = logger or logging.getLogger(__name__)
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
    
    def save(self, current_term, voted_for, log):
        """
        Persist Raft state to disk atomically.
        
        Args:
            current_term: Current term number
            voted_for: Node ID voted for in current term (or None)
            log: List of log entries [{term, command}, ...]
        """
        state = {
            'current_term': current_term,
            'voted_for': voted_for,
            'log': log
        }
        
        # Atomic write: write to temp file, then rename
        temp_path = self.filepath + '.tmp'
        try:
            with open(temp_path, 'w') as f:
                json.dump(state, f, indent=2)
            os.replace(temp_path, self.filepath)
            self.logger.debug(f"WAL saved: term={current_term}, log_len={len(log)}")
        except Exception as e:
            self.logger.error(f"WAL save failed: {e}")
            raise
    
    def load(self):
        """
        Load persisted state from disk.
        
        Returns:
            tuple: (current_term, voted_for, log) - defaults to (0, None, []) if no file
        """
        if not os.path.exists(self.filepath):
            self.logger.info("No WAL file found, starting fresh")
            return 0, None, []
        
        try:
            with open(self.filepath, 'r') as f:
                state = json.load(f)
            
            current_term = state.get('current_term', 0)
            voted_for = state.get('voted_for')
            log = state.get('log', [])
            
            self.logger.info(f"WAL loaded: term={current_term}, voted_for={voted_for}, log_len={len(log)}")
            return current_term, voted_for, log
            
        except json.JSONDecodeError as e:
            self.logger.error(f"WAL file corrupted: {e}")
            return 0, None, []
        except Exception as e:
            self.logger.error(f"WAL load failed: {e}")
            return 0, None, []
    
    def clear(self):
        """Delete WAL file (for testing)"""
        if os.path.exists(self.filepath):
            os.remove(self.filepath)
            self.logger.info("WAL cleared")
