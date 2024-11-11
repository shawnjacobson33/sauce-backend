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
    league: str
    abbr_name: Optional[str] = None
    full_name: Optional[str] = None


class Retriever:
    def __init__(self, source: Source):
        self.batch_id = uuid.uuid4()
        self.data_collected = 0
        self.source = source

    async def retrieve(self):
        pass
