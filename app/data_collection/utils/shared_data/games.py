import threading
from collections import defaultdict

from app.database import db, GAMES_COLLECTION_NAME


class Games:
    _games: defaultdict = defaultdict(list)
    _lock1: threading.Lock = threading.Lock()

    @classmethod
    def get_games(cls):
        return cls._games

    @classmethod
    def update_games(cls, league: str, game: dict):
        with cls._lock1:
            cls._games[league].append(game)

    @classmethod
    def size(cls, source_name: str = None) -> int:
        # if bookmaker is inputted
        if source_name:
            # get the lines associated with that bookmaker
            lines = cls._games.get(source_name, "")
            # return the number of lines they have
            return len(lines)

        # gets the total amount of betting lines stored
        return sum(len(value) for value in cls._games.values())

    @classmethod
    def store_games(cls) -> None:
        list_of_games_objs = [game for games in cls._games.values() for game in games]
        db.MongoDB.fetch_collection(GAMES_COLLECTION_NAME).insert_many(list_of_games_objs)


