from dataclasses import dataclass
from datetime import datetime
from typing import Union


@dataclass
class Entity:
    name: str


@dataclass
class Bookmaker(Entity):
    dflt_odds: float


@dataclass
class SportsDataProvider(Entity):
    league: str


@dataclass
class Market(Entity):
    domain: str
    std_name: str


@dataclass
class Position(Entity):
    domain: str
    std_name: str


@dataclass
class Team(Entity):
    domain: str
    std_name: str
    full_name: str = None


@dataclass
class Subject(Entity):
    domain: str
    std_name: str
    position: str
    team: str


@dataclass
class Game(Entity):
    info: str
    game_time: Union[datetime, int]
