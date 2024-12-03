from typing import Optional

from backend.app.data_collection.workers.utils.modelling import Market
from backend.app.data_collection.workers.utils.cleaning import clean_market
from backend.app.data_collection.workers.utils.storing import Markets, ProblemData, RelevantData


def get_market(bookmaker_name: str, league: str, market_name: str, period_type: str = None) -> Optional[dict]:
    # create a market object
    market = Market(market_name, league=league).__dict__
    # clean the market name
    cleaned_market = clean_market(market['name'], market['sport'], period_classifier=period_type)
    # get the matched data if it exists
    if market_id := Markets.get_market(market['sport'], cleaned_market):
        # update the market object with new data
        market['id'], market['name'] = market_id, cleaned_market
        # update the shared dictionary of valid markets
        RelevantData.update_relevant_markets(market, bookmaker_name)
        # return the market id and cleaned name
        return market

    # update the shared dictionary of pending markets
    ProblemData.update_problem_markets(market, bookmaker_name)