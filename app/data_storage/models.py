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
    std_name: str


@dataclass
class Team(AttrEntity):
    full_name: str


@dataclass
class Position(AttrEntity):
    pass