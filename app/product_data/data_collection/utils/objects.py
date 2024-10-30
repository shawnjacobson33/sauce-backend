import json
import os
from dataclasses import dataclass
from typing import Optional

from app.product_data.data_collection.utils import LEAGUE_SPORT_MAP, Packager
from app.product_data.data_collection.shared_data import PropLines


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
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr):
        self.info = info
        self.batch_id = batch_id
        self.req_mngr = req_mngr
        self.req_packager = Packager(info.name)
        self.data_size = 0

    async def start(self):
        pass

    def add_and_update(self, prop_line: dict, bookmaker: str = None) -> None:
        # update shared data...formatting bookmaker name for OddsShopper's contrasting formats...OddShopper will use bookmaker param
        PropLines.update(''.join(self.info.name.split() if not bookmaker else bookmaker.split()).lower(), prop_line)
        # add one to the prop line count
        self.data_size += 1

    @staticmethod
    def save_to_file():
        file_path = os.path.join(os.path.dirname(__file__), 'logs/prop_lines.json')
        with open(file_path, 'w') as f:
            json.dump(PropLines.get(), f, indent=4)

    def __str__(self):
        return str(self.data_size)


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
