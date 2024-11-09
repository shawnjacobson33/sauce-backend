from dataclasses import dataclass
from typing import Optional

from app.data_collection.utils.definitions import LEAGUE_SPORT_MAP

# ***************************** DATABASE MODELS *********************************


@dataclass
class Team:
    league: str
    abbr_name: Optional[str] = None
    full_name: Optional[str] = None


class Market:
    def __init__(self, name: str, league: Optional[str] = None, sport: Optional[str] = None):
        self.name = name
        self.sport = LEAGUE_SPORT_MAP.get(league) if not sport else sport

    def __str__(self):
        return f"Market(name: {self.name}, sport: {self.sport})"


@dataclass
class Subject:
    name: str
    league: Optional[str] = None
    team: Optional[dict] = None
    position: Optional[str] = None
    jersey_number: Optional[str] = None
