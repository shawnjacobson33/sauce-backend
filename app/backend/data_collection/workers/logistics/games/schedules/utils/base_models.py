from app.backend.data_collection.workers.utils import Games
from app.backend.data_collection.workers.utils.modelling import Retriever
from app.backend.data_collection.workers.logistics.games.utils import GameSource


class ScheduleRetriever(Retriever):
    def __init__(self, source: GameSource):
        super().__init__(source)
        self.league = source.league
        self.league_spec = source.league_spec

    def __str__(self):
        return f'{str(Games.size(self.league_spec))} ({self.league_spec}) new games'