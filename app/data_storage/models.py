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
class AttrEntity(Entity):
    domain: str
    std_name: str = None


@dataclass
class Market(AttrEntity):
    pass


@dataclass
class Team(AttrEntity):
    full_name: str = None


@dataclass
class Position(AttrEntity):
    pass


@dataclass
class Subject(AttrEntity):
    position: Position
    team: Team


@dataclass
class Game(AttrEntity):
    home_team: Team
    away_team: Team
    start_time: str = None
    end_time: str = None