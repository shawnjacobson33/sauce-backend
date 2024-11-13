from typing import Optional

from app.backend.data_collection.utils.shared_data import Games
from app.backend.data_collection.utils.modelling import Source, Retriever


class ScheduleSource(Source):
    def __init__(self, name: str, league: str, league_specific: Optional[str]):
        super().__init__(name, league)
        self.league_specific = league if not league_specific else league_specific  # For 'NCAA'


class ScheduleRetriever(Retriever):
    def __init__(self, source: ScheduleSource):
        super().__init__(source)

    def update_games(self, game: dict) -> None:
        # add the game to the shared data structure
        Games.update_games(game)
        # keep track of the number of games found per league
        self.data_collected += 1

    def __str__(self):
        return f'{str(self.data_collected)} ({self.source.league_specific}) games'