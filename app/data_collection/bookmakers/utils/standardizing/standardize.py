from typing import Optional

from bson import ObjectId

from app.data_collection.utils.modelling import Subject, Market
from app.data_collection.bookmakers.utils.shared_data import Subjects, Markets
from app.data_collection.bookmakers.utils.cleaning import clean_subject, clean_market


def filter_subject_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Subjects.get_stored_subjects()
    # filter it by partition
    return structured_data_store[league]


def filter_market_data(sport: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Markets.get_stored_markets()
    # filter it by partition
    return structured_data_store[sport]


def get_subject_id(bookmaker_name: str, subject: Subject) -> Optional[tuple[ObjectId, str]]:
    # clean the subject name
    if cleaned_subject := clean_subject(subject.name):
        # filter by league partition
        filtered_data = filter_subject_data(subject.league)
        # get the matched data if it exists
        if matched_data := filtered_data.get(cleaned_subject):
            # update the subject object's name
            subject.name = cleaned_subject
            # set the market object's id
            subject.id = matched_data['id']
            # update the shared dictionary of valid subjects
            Subjects.update_valid_subjects(bookmaker_name, tuple(subject.__dict__.items()))
            # return the matched subject id and the actual name of the subject stored in the database
            return matched_data['id'], matched_data['name']

    # update the shared dictionary of pending subjects
    Subjects.update_pending_subjects(bookmaker_name, tuple(subject.__dict__.items()))
    return None, None


def get_market_id(bookmaker_name: str, market: Market, period_type: str = None) -> Optional[tuple[ObjectId, str]]:
    # clean the market name
    if cleaned_market := clean_market(market.name, market.sport, period_classifier=period_type):
        # filter by sport partition
        filtered_data = filter_market_data(market.sport)
        # get the matched data if it exists
        if matched_id := filtered_data.get(cleaned_market):
            # update the market object's name
            market.name = cleaned_market
            # set the market object's id
            market.id = matched_id
            # update the shared dictionary of valid markets
            Markets.update_valid_markets(bookmaker_name, tuple(market.__dict__.items()))
            # return the id of the matched market if it exists
            return matched_id, cleaned_market

    # update the shared dictionary of pending markets
    Markets.update_pending_markets(bookmaker_name if not period_type else cleaned_market, tuple(market.__dict__.items()))
    return None, None