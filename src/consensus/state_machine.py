"""
State Machine - Key-Value Store
Applies committed log entries from the Raft consensus
"""
import threading
import logging


class StateMachine:
    """
    Simple key-value store that applies committed Raft log entries.
    Thread-safe for concurrent access.
    """
    
    def __init__(self, logger=None):
        self.data = {}
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger(__name__)
    
    def apply(self, command):
        """
        Apply a command to the state machine.
        
        Supported commands:
        - "SET key=value" - Set a key to a value
        - "DELETE key" - Delete a key
        
        Returns: (success: bool, result: str)
        """
        with self.lock:
            try:
                parts = command.strip().split(maxsplit=1)
                if not parts:
                    return False, "Empty command"
                
                op = parts[0].upper()
                
                if op == 'SET' and len(parts) == 2:
                    if '=' not in parts[1]:
                        return False, "SET requires key=value format"
                    key, value = parts[1].split('=', 1)
                    self.data[key.strip()] = value.strip()
                    self.logger.info(f"Applied: SET {key}={value}")
                    return True, f"OK: {key}={value}"
                
                elif op == 'DELETE' and len(parts) == 2:
                    key = parts[1].strip()
                    if key in self.data:
                        del self.data[key]
                        self.logger.info(f"Applied: DELETE {key}")
                        return True, f"OK: deleted {key}"
                    return False, f"Key '{key}' not found"
                
                elif op == 'GET' and len(parts) == 2:
                    # GET is read-only, doesn't need to go through log
                    key = parts[1].strip()
                    if key in self.data:
                        return True, self.data[key]
                    return False, f"Key '{key}' not found"
                
                else:
                    return False, f"Unknown command: {command}"
                    
            except Exception as e:
                return False, f"Error: {str(e)}"
    
    def get(self, key):
        """Get a value by key (read-only)"""
        with self.lock:
            return self.data.get(key)
    
    def get_all(self):
        """Get all key-value pairs (snapshot)"""
        with self.lock:
            return dict(self.data)
    
    def __len__(self):
        with self.lock:
            return len(self.data)
