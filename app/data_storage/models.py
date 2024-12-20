from dataclasses import dataclass


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
    home_team: str
    away_team: str
    start_time: str = None
    end_time: str = None