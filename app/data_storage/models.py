from dataclasses import dataclass
from datetime import datetime
from typing import Union


@dataclass
class Entity:
    name: str
    domain: str


@dataclass
class Market(Entity):
    std_name: str


@dataclass
class Position(Entity):
    std_name: str


@dataclass
class Team(Entity):
    std_name: str
    full_name: str = None


@dataclass
class Subject(Entity):
    std_name: str
    position: str
    team: str


@dataclass
class Game:
    domain: str
    info: str
    game_time: Union[datetime, str]

    def __post_init__(self):
        try:
            self.game_time = datetime.strptime(self.game_time, "%Y-%m-%d %H:%M") \
                if isinstance(self.game_time, str) else self.game_time
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM")


@dataclass
class Bookmaker:
    name: str
    dflt_odds: str


@dataclass
class SportsDataProvider:
    league: str
