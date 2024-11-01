from collections import defaultdict

from app.product_data.data_collection.shared_data import BettingLines
from app.product_data.data_collection.utils import Packager, Bookmaker
from app.product_data.data_collection.plugs.bookmakers.reports import reporting


class BookmakerPlug:
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr):
        # Params
        self.bookmaker_info = info
        self.batch_id = batch_id
        self.req_mngr = req_mngr
        # Instantiated
        self.req_packager = Packager(info.name)
        self.__reporter = reporting.setup_reporter(self.bookmaker_info.name)
        # Metrics
        self.betting_lines_collected = 0
        self.leagues_tracker = set()  # TODO: Extract into a separate class?
        self.markets_tracker = defaultdict(int)
        self.subjects_tracker = defaultdict(int)

    async def collect(self):
        pass

    def report(self, message: str):
        self.__reporter.debug(message)

    def update_betting_lines(self, betting_line: dict, bookmaker: str = None) -> None:
        # update shared data...formatting bookmaker name for OddsShopper's contrasting formats...OddShopper will use bookmaker param
        BettingLines.update(''.join(self.bookmaker_info.name.split() if not bookmaker else bookmaker.split()).lower(), betting_line)
        # add one to the prop line count
        self.betting_lines_collected += 1

    def __str__(self):
        return str(self.betting_lines_collected)
