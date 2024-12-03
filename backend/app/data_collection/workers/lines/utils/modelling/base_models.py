from dataclasses import dataclass

from app.backend.data_collection.workers import lines as lns
from app.backend.data_collection.workers import utils as wrk_utils


# ***************************** EXTRA HELPERS *********************************

@dataclass
class Payout:
    legs: int
    is_insured: bool
    odds: float

# ***************************** DATABASE MODELS *********************************

class LinesSource(wrk_utils.Source):
    def __init__(self, bookmaker_info: dict):
        super().__init__(bookmaker_info['name'])
        self.default_payout, self.payouts = None, None
        # don't have to check for 'payouts' because these two will always be together.
        if 'default_odds' in bookmaker_info:
            d_data, p_data = bookmaker_info['default_odds'], bookmaker_info['payouts']
            self.default_payout: Payout = Payout(d_data['legs'], d_data['is_insured'], d_data['odds'])
            self.payouts: list[Payout] = [Payout(data['legs'], data['is_insured'], data['odds']) for data in p_data]

# ***************************** BASE MODELS *********************************

class LinesRetriever(wrk_utils.Retriever):
    def __init__(self, batch_id: str, lines_source: LinesSource):
        super().__init__(lines_source)
        self.batch_id = batch_id
        self.dflt_odds, self.dflt_im_prb = None, None
        if dflt_payout := lines_source.__dict__.get('default_payout'):
            self.dflt_odds = dflt_payout.odds
            self.dflt_im_prb = round(1 / self.dflt_odds, 4)

        self.req_mngr = lns.RequestManager(use_requests=(lines_source.name == 'BetOnline'))  # BetOnline doesn't work with 'cloudscraper'

    def __str__(self):
        return f'{str(wrk_utils.BettingLines.counts(self.name))} lines'
