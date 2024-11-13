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
    def size(cls, bookmaker_name: str = None) -> int:
        # if bookmaker is inputted
        if bookmaker_name:
            # get the lines associated with that bookmaker
            lines = cls._betting_lines.get(bookmaker_name, "")
            # return the number of lines they have
            return len(lines)

        # gets the total amount of betting lines stored
        return sum(len(value) for value in cls._betting_lines.values())