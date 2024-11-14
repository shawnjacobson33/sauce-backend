from typing import Optional

from app.backend.data_collection.utils.modelling import Source


class GameSource(Source):
    def __init__(self, name: str, league: str, league_specific: Optional[str]):
        super().__init__(name, league)
        self.league_specific = league if not league_specific else league_specific  # For 'NCAA'