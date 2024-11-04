from dataclasses import dataclass
from typing import Optional

from app.product_data.data_collection.plugs.utils import LEAGUE_SPORT_MAP

# ***************************** DATABASE MODELS *********************************

@dataclass
class Subject:
    name: str
    league: Optional[str] = None
    team: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[str] = None


class Market:
    def __init__(self, name: str, league: Optional[str] = None, sport: Optional[str] = None):
        self.name = name
        self.sport = LEAGUE_SPORT_MAP.get(league) if not sport else sport

    def __str__(self):
        return f"Market(name: {self.name}, sport: {self.sport})"


@dataclass
class Team:
    name: str
    league: Optional[str] = None
