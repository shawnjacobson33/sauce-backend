from dataclasses import dataclass

from app.product_data.data_collection.plugs.data_hub import BettingLines
from app.product_data.data_collection.plugs.bookmakers import utils


# ***************************** EXTRA HELPERS *********************************

@dataclass
class Payout:
    legs: int
    is_insured: bool
    odds: float

# ***************************** DATABASE MODELS *********************************

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

# ***************************** BASE MODELS *********************************

class BookmakerPlug:
    def __init__(self, bookmaker_info: Bookmaker, batch_id: str):
        # Instantiated
        self.__reporter = utils.setup_reporter(bookmaker_info.name, content=['leagues', 'subjects', 'markets', 'metrics'])
        self.req_mngr = utils.RequestManager(use_requests=(bookmaker_info.name == 'BetOnline'))  # BetOnline doesn't work with 'cloudscraper'
        # Params
        self.bookmaker_info = bookmaker_info
        self.batch_id = batch_id
        # Metrics
        self.betting_lines_collected = 0
        self.metrics = utils.Metrics()


    async def collect(self):
        pass

    def log(self, message: str):
        self.__reporter.debug(message)

    def update_betting_lines(self, betting_line: dict, bookmaker: str = None) -> None:
        # update shared data...formatting bookmaker name for OddsShopper's contrasting formats...OddShopper will use bookmaker param
        BettingLines.update(''.join(self.bookmaker_info.name.split() if not bookmaker else bookmaker.split()).lower(), betting_line)
        # add one to the prop line count
        self.betting_lines_collected += 1

    def __str__(self):
        return str(self.betting_lines_collected)