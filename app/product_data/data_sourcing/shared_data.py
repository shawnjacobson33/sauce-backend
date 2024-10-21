import threading
from collections import defaultdict

"""
Applies the Singleton Pattern to allow a single shared instance of the data that each plug will update in separate
coroutines.

_prop_lines: dict[set]
    - Group by Bookmaker to Reduce Filtering Operations Later
    - Store in Sets because duplicates are unwanted.
"""
class PropLines:
    _prop_lines = defaultdict(set)
    _lock = threading.Lock()

    @classmethod
    def get(cls):
        return cls._prop_lines

    @classmethod
    def update(cls, key, value):
        # Acquire the lock to ensure thread safety
        with cls._lock:
            cls._prop_lines[key].add(value)
