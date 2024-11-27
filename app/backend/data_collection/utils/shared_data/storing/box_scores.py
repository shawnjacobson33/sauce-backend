import threading
from collections import defaultdict
from typing import Optional


class BoxScores:
    _box_scores: defaultdict = defaultdict(dict)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_box_scores(cls, league: Optional[str] = None):
        return cls._box_scores

    @classmethod
    def update_box_scores(cls, league: str, game_id: str, subject: dict, box_score: dict, stat_type: str) -> None:
        with cls._lock1:
            # gets all box scores associated with the game
            cls._box_scores[(league, game_id, subject['id'])] = {
                'name': subject['name'],  # TODO: just here for logging purposes?
                'stats': {
                    stat_type: box_score
                }
            }

    @classmethod
    def size(cls) -> int:
        return len(cls._box_scores)
