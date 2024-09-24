from dataclasses import dataclass
from typing import Optional


@dataclass
class Subject:
    name: str
    league: Optional[str] = None
    team: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[str] = None


@dataclass
class Market:
    name: str
    league: Optional[str] = None
