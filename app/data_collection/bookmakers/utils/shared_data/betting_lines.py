import threading
from collections import defaultdict

"""
Applies the Singleton Pattern to allow a single shared instance of the data that each plug will update in separate
coroutines.

_prop_lines: dict[set]
    - Group by Bookmaker to Reduce Filtering Operations Later
    - Store in Lists and handle duplicates later on.
"""
class BettingLines:
    _betting_lines = defaultdict(list)
    _lock = threading.Lock()

    @classmethod
    def get(cls):
        return cls._betting_lines

    @classmethod
    def update(cls, key, value):
        # Acquire the lock to ensure thread safety
        with cls._lock:
            cls._betting_lines[key].append(value)

    @classmethod
    def size(cls):
        # gets the total amount of betting lines stored
        return sum(len(value) for value in cls._betting_lines.values())