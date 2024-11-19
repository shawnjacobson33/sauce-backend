from app.backend.data_collection.utils.shared_data import Games
from app.backend.data_collection.utils.modelling import Retriever
from app.backend.data_collection.logistics.games.utils import GameSource


class ScheduleRetriever(Retriever):
    def __init__(self, source: GameSource):
        super().__init__(source)

    def update_games(self, game: dict) -> None:
        # add the game to the shared data structure and keep track of the number of games found per league
        self.data_collected += Games.update_games(game)

    def __str__(self):
        return f'{str(self.data_collected)} ({self.source.league_specific}) new/updated games'