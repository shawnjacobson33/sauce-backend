from typing import Type

from app import database as db
from app.data_collection import games as gms
from app.data_collection import bookmakers as bkm


SESSION = db.MongoDB.get_session()


def configure_schedule_retriever(source_name: str, schedule_retriever: Type[gms.ScheduleRetriever]) -> gms.ScheduleRetriever:
    # get some source info
    if source := db.get_source(SESSION, source_name):
        # create a source object
        schedule_source = gms.ScheduleSource(source['name'], source['league'])
        # return a new schedule retriever instance
        return schedule_retriever(schedule_source)


def configure_lines_retriever(lines_retriever: Type[bkm.LinesRetriever]) -> bkm.LinesRetriever:
    # get some source info
    if source := db.get_source(SESSION, lines_retriever.__name__):
        # get some attributes of the bookmaker from the database
        lines_source = bkm.LinesSource(source)
        # create an instance of the bookmaker plug
        return lines_retriever(lines_source)