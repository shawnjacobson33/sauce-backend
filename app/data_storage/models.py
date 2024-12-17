from dataclasses import dataclass


@dataclass
class Entity:
    domain: str
    name: str
    std_name: str


@dataclass
class Team(Entity):
    full_name: str


@dataclass
class Position(Entity):
    pass