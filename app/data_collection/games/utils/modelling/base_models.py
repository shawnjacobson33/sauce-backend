from collections import defaultdict
from dataclasses import dataclass

from app.data_collection.utils.shared_data import Games


@dataclass
class Source:
    name: str  # ex: BasketballReference
    league: str


class ScheduleCollector:
    def __init__(self, source_info: Source):
        self.source_info = source_info
        self.games_collected = defaultdict(int)

    def update_games(self, game: dict) -> None:
        # add the game to the shared data structure
        Games.update_games(self.source_info.league, game)
        # keep track of the number of games found per league
        self.games_collected[self.source_info.league] += 1