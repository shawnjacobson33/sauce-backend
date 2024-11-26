from dataclasses import dataclass
from typing import Optional

from app.backend.data_collection.utils.modelling import Source, Retriever
from app.backend.data_collection.utils.definitions import LEAGUE_SPORT_MAP
from app.backend.data_collection.bookmakers.utils.shared_data import BettingLines
from app.backend.data_collection.bookmakers.utils.requesting import RequestManager


# ***************************** EXTRA HELPERS *********************************

@dataclass
class Payout:
    legs: int
    is_insured: bool
    odds: float

# ***************************** DATABASE MODELS *********************************

class Market:
    def __init__(self, name: str, league: Optional[str] = None, sport: Optional[str] = None):
        self.name = name
        self.sport = LEAGUE_SPORT_MAP.get(league) if not sport else sport

    def __str__(self):
        return f"Market(name: {self.name}, sport: {self.sport})"


class Subject:
    def __init__(self, name: str, league: str, team: Optional[dict] = None, position: Optional[dict] = None, jersey_number: Optional[dict] = None):
        self.name = name
        self.league = league
        self.team_id = team['id'] if team else None
        self.team_name = team.get('abbr_name', team.get('full_name')) if team else None
        self.position = position
        self.jersey_number = jersey_number


class LinesSource(Source):
    def __init__(self, bookmaker_info: dict):
        super().__init__(bookmaker_info['name'])
        self.is_dfs: bool = bookmaker_info['is_dfs']

        self.default_payout, self.payouts = None, None
        # don't have to check for 'payouts' because these two will always be together.
        if 'default_odds' in bookmaker_info:
            d_data, p_data = bookmaker_info['default_odds'], bookmaker_info['payouts']
            self.default_payout: Payout = Payout(d_data['legs'], d_data['is_insured'], d_data['odds'])
            self.payouts: list[Payout] = [Payout(data['legs'], data['is_insured'], data['odds']) for data in p_data]

# ***************************** BASE MODELS *********************************

class LinesRetriever(Retriever):
    def __init__(self, lines_source: LinesSource):
        super().__init__(lines_source)
        self.req_mngr = RequestManager(use_requests=(lines_source.name == 'BetOnline'))  # BetOnline doesn't work with 'cloudscraper'

    def update_betting_lines(self, betting_line: dict) -> None:
        # update shared data...formatting bookmaker name for OddsShopper's contrasting formats...OddShopper will use bookmaker param
        BettingLines.update(betting_line)
        # add one to the prop line count
        self.data_collected += 1

    def __str__(self):
        return f'{str(self.data_collected)} lines'
