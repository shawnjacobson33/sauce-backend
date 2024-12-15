from app import Games
from app import Retriever
from app import GameSource


class ScheduleRetriever(Retriever):
    def __init__(self, source: GameSource):
        super().__init__(source)
        self.league = source.league
        self.league_spec = source.league_spec

    def __str__(self):
        return f'{str(Games.counts(self.league_spec))} ({self.league_spec}) new games'