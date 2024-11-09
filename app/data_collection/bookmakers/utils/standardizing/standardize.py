from typing import Optional

from bson import ObjectId

from app.data_collection.utils import Subject, Market, Team
from app.data_collection.bookmakers.utils.shared_data import Subjects, Markets, Teams
from app.data_collection.bookmakers.utils.cleaning import clean_subject, clean_market, clean_team


def create_team_object(league: str, team_name: str) -> Team:
    # create a team object
    team_obj = Team(league)
    # update the object's attributes depending on if the bookmaker uses abbr_name or full_name
    setattr(team_obj, 'abbr_name' if len(team_name) < 5 else 'full_name', team_name)
    # return the updated team object
    return team_obj


def filter_subject_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Subjects.get_stored_data()
    # filter it by partition
    return structured_data_store[league]


def filter_market_data(sport: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Markets.get_stored_data()
    # filter it by partition
    return structured_data_store[sport]


def filter_team_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Teams.get_stored_data()
    # filter it by partition
    return structured_data_store[league]


def update_team_object(team: Team, cleaned_team_name: str, team_id: str) -> None:
    # update the abbreviated or full team name of the object
    setattr(team, 'abbr_name' if len(cleaned_team_name) < 5 else 'full_name', cleaned_team_name)
    # set the market object's id
    team.id = str(team_id)


def update_market_object(market: Market, cleaned_market_name: str, market_id: str) -> None:
    # update the market object's name
    market.name = cleaned_market_name
    # set the market object's id
    market.id = str(market_id)


def get_subject_id(bookmaker_name: str, league: str, subject_name: str, **kwargs) -> Optional[tuple[ObjectId, str]]:
    # create a subject object
    subject = Subject(subject_name, league, **kwargs)
    # clean the subject name
    if cleaned_subject := clean_subject(subject.name):
        # filter by league partition
        filtered_data = filter_subject_data(subject.league)
        # get the matched data if it exists
        if matched_data := filtered_data.get(cleaned_subject):
            # update the shared dictionary of valid subjects
            Subjects.update_valid_data(bookmaker_name, tuple(matched_data.items()))
            # return the matched subject id and the actual name of the subject stored in the database
            return matched_data['id'], matched_data['name']

    # update the shared dictionary of pending subjects
    Subjects.update_pending_data(bookmaker_name, tuple(subject.__dict__.items()))
    return None, None


def get_market_id(bookmaker_name: str, league: str, market_name: str, period_type: str = None) -> Optional[tuple[ObjectId, str]]:
    # create a market object
    market = Market(market_name, league=league)
    # clean the market name
    if cleaned_market := clean_market(market.name, market.sport, period_classifier=period_type):
        # filter by sport partition
        filtered_data = filter_market_data(market.sport)
        # get the matched data if it exists
        if matched_id := filtered_data.get(cleaned_market):
            # update the market object attributes
            update_market_object(market, cleaned_market, matched_id)
            # update the shared dictionary of valid markets
            Markets.update_valid_data(bookmaker_name, tuple(market.__dict__.items()))
            # return the id of the matched market if it exists
            return matched_id, cleaned_market

    # update the shared dictionary of pending markets
    Markets.update_pending_data(bookmaker_name if not period_type else cleaned_market, tuple(market.__dict__.items()))
    return None, None


def get_team_id(bookmaker_name: str, league: str, team_name: str) -> Optional[dict]:
    # create the team object
    team = create_team_object(league, team_name)
    # clean the team name
    if cleaned_team := clean_team(team.abbr_name, team.league):
        # filter by league partition
        filtered_data = filter_team_data(team.league)
        # get the matched data if it exists
        if matched_id := filtered_data.get(cleaned_team):
            # update the team object with new data
            update_team_object(team, cleaned_team, matched_id)
            # update the shared dictionary of valid teams
            Teams.update_valid_data(bookmaker_name, tuple(team.__dict__.items()))
            # return a dictionary representing the team object for existing values
            return {key: value for key, value in team.__dict__.items() if value is not None}

    # update the shared dictionary of pending teams
    Teams.update_pending_data(bookmaker_name, tuple(team.__dict__.items()))