import uuid
from dataclasses import dataclass
from typing import Optional


# ***************************** DATABASE MODELS *********************************

@dataclass
class Source:
    name: str  # ex: BasketballReference, BoomFantasy
    league: str = ""  # Used for everything but LinesSource child class


@dataclass
class Team:
    league: Optional[str] = None
    abbr_name: Optional[str] = None
    full_name: Optional[str] = None


class Retriever:
    def __init__(self, source: Source):
        self.name = source.name
        self.data_collected = 0

    async def retrieve(self):
        pass
