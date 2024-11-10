from dataclasses import dataclass
from typing import Optional


# ***************************** DATABASE MODELS *********************************


@dataclass
class Team:
    league: str
    abbr_name: Optional[str] = None
    full_name: Optional[str] = None



