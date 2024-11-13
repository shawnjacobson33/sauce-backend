from typing import Type

from app.backend import database as db
from app.backend.data_collection import games as gms
from app.backend.data_collection import bookmakers as bkm


def configure_schedule_retriever(source_name: str, schedule_retriever: Type[gms.ScheduleRetriever]) -> gms.ScheduleRetriever:
    # get some source info
    if source := db.MongoDB.fetch_source(source_name):
        # create a source object
        schedule_source = gms.ScheduleSource(source['name'], source['league'], source.get('league-specific'))
        # return a new schedule retriever instance
        return schedule_retriever(schedule_source)


def configure_lines_retriever(lines_retriever: Type[bkm.LinesRetriever]) -> bkm.LinesRetriever:
    # get some source info
    if source := db.MongoDB.fetch_source(lines_retriever.__name__):
        # get some attributes of the bookmaker from the database
        lines_source = bkm.LinesSource(source)
        # create an instance of the bookmaker plug
        return lines_retriever(lines_source)