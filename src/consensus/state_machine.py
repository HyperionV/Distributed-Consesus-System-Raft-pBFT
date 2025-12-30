import threading

class StateMachine:
    """
    Simple in-memory key-value store that applies validated consensus commands.
    """
    
    def __init__(self, node=None):
        self.data = {}
        self.lock = threading.Lock()
        self.node = node
    
    def log(self, message):
        if self.node:
            self.node.log(message)
        else:
            print(f"[INFO][StateMachine] {message}")

    def apply(self, cmd):
        """
        Apply a command (SET or DELETE) to the state machine.
        Returns: (success: bool, result: str)
        """
        with self.lock:
            try:
                p = cmd.strip().split(maxsplit=1)
                if not p: return False, "Empty"
                op = p[0].upper()
                if op == 'SET' and len(p) == 2 and '=' in p[1]:
                    k, v = p[1].split('=', 1)
                    self.data[k.strip()] = v.strip()
                    self.log(f"State updated: {k.strip()}={v.strip()}")
                    return True, "OK"
                if op == 'DELETE' and len(p) == 2:
                    k = p[1].strip()
                    if k in self.data:
                        del self.data[k]
                        self.log(f"State deleted: {k}")
                        return True, "OK"
                    return False, "Not found"
                return False, f"Unknown: {cmd}"
            except Exception as e: return False, str(e)
    
    def get(self, k):
        """Read-only access to the state machine data."""
        with self.lock: return self.data.get(k)

    def snapshot(self):
        """Return a copy of the current state machine data."""
        with self.lock: return dict(self.data)
