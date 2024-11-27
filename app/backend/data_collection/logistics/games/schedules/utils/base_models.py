from app.backend.data_collection.utils.shared_data import Games
from app.backend.data_collection.utils.modelling import Retriever
from app.backend.data_collection.logistics.games.utils import GameSource


class ScheduleRetriever(Retriever):
    def __init__(self, source: GameSource):
        super().__init__(source)

    def __str__(self):
        return f'{str(Games.size(self.source.league_specific))} ({self.source.league_specific}) new games'