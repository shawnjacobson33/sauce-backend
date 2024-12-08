from dataclasses import dataclass
from typing import Optional

from app import LEAGUE_SPORT_MAP


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


class Market:
    def __init__(self, name: str, league: Optional[str] = None, sport: Optional[str] = None):
        self.name = name
        self.sport = LEAGUE_SPORT_MAP.get(league) if not sport else sport

    def __str__(self):
        return f"Market(name: {self.name}, sport: {self.sport})"


class Subject:
    def __init__(self, name: str, league: str, team: Optional[str] = None, position: Optional[dict] = None, jersey_number: Optional[dict] = None):
        self.name = name
        self.league = league
        self.team = team
        self.position = position
        self.jersey_number = jersey_number


class Retriever:
    def __init__(self, source: Source):
        self.name = source.name
        self.data_collected = 0

    async def retrieve(self):
        pass
