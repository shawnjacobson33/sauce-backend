from app.data_collection.utils.shared_data import Games
from app.data_collection.utils.modelling import Source, Retriever


class ScheduleSource(Source):
    def __init__(self, name: str, league: str):
        super().__init__(name, league)


class ScheduleRetriever(Retriever):
    def __init__(self, source: ScheduleSource):
        super().__init__(source)

    def update_games(self, game: dict) -> None:
        # add the game to the shared data structure
        Games.update_games(self.source.league, game)
        # keep track of the number of games found per league
        self.data_collected += 1

    def __str__(self):
        return f'{str(self.data_collected)} ({self.source.league}) games'