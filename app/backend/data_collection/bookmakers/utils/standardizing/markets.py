from typing import Optional

from app.backend.data_collection.bookmakers.utils.modelling import Market
from app.backend.data_collection.bookmakers.utils.shared_data import Markets
from app.backend.data_collection.bookmakers.utils.cleaning import clean_market


def filter_market_data(sport: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Markets.get_stored_data()
    # filter it by partition
    return structured_data_store[sport]


def update_market_object(market: Market, cleaned_market_name: str, market_id: str) -> None:
    # update the market object's name
    market.name = cleaned_market_name
    # set the market object's id
    market.id = str(market_id)


def get_market_id(bookmaker_name: str, league: str, market_name: str, period_type: str = None) -> Optional[dict[str, str]]:
    # create a market object
    market = Market(market_name, league=league)
    # clean the market name
    cleaned_market = clean_market(market.name, market.sport, period_classifier=period_type)
    # filter by sport partition
    filtered_data = filter_market_data(market.sport)
    # get the matched data if it exists
    if matched_id := filtered_data.get(cleaned_market):
        # cast the matched id to a string
        matched_id = str(matched_id)
        # update the market object attributes
        update_market_object(market, cleaned_market, matched_id)
        # update the shared dictionary of valid markets
        Markets.update_valid_data(bookmaker_name, tuple(market.__dict__.items()))
        # return the market id and cleaned name
        return {
            'id': matched_id,
            'name': cleaned_market
        }

    # update the shared dictionary of pending markets
    Markets.update_pending_data(bookmaker_name if not period_type else cleaned_market, tuple(market.__dict__.items()))