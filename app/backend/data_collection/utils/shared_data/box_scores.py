import threading
from collections import defaultdict
from typing import Optional


class BoxScores:
    _box_scores: defaultdict = defaultdict(lambda: defaultdict(dict))
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_box_scores(cls, league: Optional[str] = None):
        return cls._box_scores.get(league) if league else cls._box_scores

    @classmethod
    def update_box_scores(cls, league: str, game_id: str, subject_id: str, box_score: dict):
        with cls._lock1:
            cls._box_scores[league][game_id][subject_id] = box_score
