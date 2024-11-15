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
    def update_box_scores(cls, league: str, game_id: str, subject: dict, box_score: dict, stat_type: str) -> None:
        with cls._lock1:
            # gets all box scores associated with the game
            filtered_box_scores = cls._box_scores[league][game_id]
            # gets the box scores associated with the subject if they exist
            if subject_box_scores := filtered_box_scores.get(subject['id']):
                # updates or sets the stats for the given stat type to the fresh box score
                subject_box_scores['stats'][stat_type] = box_score

            # if the subject doesn't have a box score yet, create one
            filtered_box_scores[subject['id']] = {
                'subject': subject['name'],
                'stats': {
                    stat_type: box_score
                }
            }

    @classmethod
    def size(cls):
        return sum([len(game) for game in cls._box_scores.values()])
