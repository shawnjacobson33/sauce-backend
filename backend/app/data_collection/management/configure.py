from typing import Type

from backend.app.database import MongoDB
from backend.app.data_collection.workers import lines as lns
from backend.app.data_collection.workers.logistics import games as gms
from backend.app.data_collection.workers.utils.modelling import Retriever


def configure_game_retriever(source_name: str, game_retriever: Type[Retriever]) -> Retriever:
    # get some source info
    if source := MongoDB.fetch_source(source_name):
        # create a source object
        game_source = gms.GameSource(source['name'], source['league'], source.get('league_specific'))
        # return a new schedule retriever instance
        return game_retriever(game_source)


def configure_lines_retriever(batch_id: str, lines_retriever: Type[lns.LinesRetriever]) -> lns.LinesRetriever:
    # get some source info
    if source := MongoDB.fetch_source(lines_retriever.__name__):
        # get some attributes of the bookmaker from the database
        lines_source = lns.LinesSource(source)
        # create an instance of the bookmaker plug
        return lines_retriever(batch_id, lines_source)