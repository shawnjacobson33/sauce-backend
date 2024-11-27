from typing import Optional

from app.backend.data_collection.utils.cleaning import clean_market
from app.backend.data_collection.utils.shared_data import Markets, ProblemData, RelevantData
from app.backend.data_collection.bookmakers.utils.modelling import Market


def get_market(bookmaker_name: str, league: str, market_name: str, period_type: str = None) -> Optional[Market]:
    # create a market object
    market = Market(market_name, league=league)
    # clean the market name
    cleaned_market = clean_market(market.name, market.sport, period_classifier=period_type)
    # get the matched data if it exists
    if market_id := Markets.get_market(market.sport, market.name):
        # update the market object with new data
        market.id, market.name = market_id, cleaned_market
        # update the shared dictionary of valid markets
        RelevantData.update_relevant_markets(market.__dict__, bookmaker_name)
        # return the market id and cleaned name
        return market

    # update the shared dictionary of pending markets
    ProblemData.update_problem_markets(market.__dict__, bookmaker_name)