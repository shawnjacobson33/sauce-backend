from dataclasses import dataclass
from typing import Optional

from app.product_data.data_sourcing.utils import LEAGUE_SPORT_MAP, Packager


@dataclass
class Payout:
    legs: int
    is_insured: bool
    odds: float


class Bookmaker:
    def __init__(self, info: dict):
        self.name: str = info['name']
        self.is_dfs: bool = info['is_dfs']

        self.default_payout, self.payouts = None, None
        # don't have to check for 'payouts' because these two will always be together.
        if 'default_odds' in info:
            d_data, p_data = info['default_odds'], info['payouts']
            self.default_payout: Payout = Payout(d_data['legs'], d_data['is_insured'], d_data['odds'])
            self.payouts: list[Payout] = [Payout(data['legs'], data['is_insured'], data['odds']) for data in p_data]


class Plug:
    def __init__(self, info: Bookmaker, batch_id: str, request_manager, data_standardizer):
        self.info = info
        self.batch_id = batch_id
        self.rm = request_manager
        self.ds = data_standardizer
        self.packager = Packager(info.name)


@dataclass
class Team:
    name: str
    league: Optional[str] = None


@dataclass
class Subject:
    name: str
    league: Optional[str] = None
    team: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[str] = None


class Market:
    def __init__(self, name: str, league: Optional[str] = None, sport: Optional[str] = None):
        self.name = name
        self.sport = LEAGUE_SPORT_MAP.get(league) if not sport else sport

    def __str__(self):
        return f"Market(name: {self.name}, sport: {self.sport})"
