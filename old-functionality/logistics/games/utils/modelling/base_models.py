from typing import Optional

from app import Source


class GameSource(Source):
    def __init__(self, name: str, league: str, league_specific: str | None):
        super().__init__(name, league)
        self.league_spec = league if not league_specific else league_specific  # For 'NCAA'