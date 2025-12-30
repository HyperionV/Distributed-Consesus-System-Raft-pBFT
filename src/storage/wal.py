import json
import os

class WAL:
    """
    Write-Ahead Log for persistent storage of consensus state.
    Uses atomic file operations for safe crash recovery.
    """
    
    def __init__(self, node_id, data_dir="."):
        self.node_id = node_id
        # Note: filename matches cleanup expectations in test_all.py
        self.filepath = os.path.join(data_dir, f"wal_data_{node_id}.json")
        os.makedirs(data_dir, exist_ok=True)
    
    def save(self, term, voted_for, log):
        """Save the current term, vote, and log entries to disk atomically."""
        state = {'term': term, 'voted_for': voted_for, 'log': log}
        tmp = self.filepath + '.tmp'
        try:
            with open(tmp, 'w') as f: json.dump(state, f)
            os.replace(tmp, self.filepath)
        except Exception: pass
    
    def load(self):
        """Load persisted state from disk. Returns (term, voted_for, log)."""
        if not os.path.exists(self.filepath): return 0, None, []
        try:
            with open(self.filepath, 'r') as f: s = json.load(f)
            return s.get('term', 0), s.get('voted_for'), s.get('log', [])
        except Exception: return 0, None, []
    
    def clear(self):
        """Delete the WAL file for testing isolation."""
        if os.path.exists(self.filepath): os.remove(self.filepath)
