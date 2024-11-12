from typing import Optional

from app.data_collection.utils.modelling import Team
from app.data_collection.utils.shared_data import Teams
from app.data_collection.utils.cleaning import clean_team


def create_team_object(league: str, team_name: str) -> Team:
    # create a team object
    team_obj = Team(league if 'NCAA' not in league else 'NCAA')  # College Teams stored under one 'NCAA' umbrella
    # update the object's attributes depending on if the bookmaker uses abbr_name or full_name
    setattr(team_obj, 'abbr_name' if len(team_name) < 5 else 'full_name', team_name)
    # return the updated team object
    return team_obj


def get_team_name(team: Team) -> str:
    # some logic to get team name that exists, defaulting to abbreviated name
    return team.full_name if not team.abbr_name else team.abbr_name

def filter_team_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Teams.get_stored_data()
    # filter it by partition
    return structured_data_store[league]


def update_team_object(team: Team, cleaned_team_name: str, team_id: str) -> None:
    # update the abbreviated or full team name of the object
    setattr(team, 'abbr_name' if len(cleaned_team_name) < 5 else 'full_name', cleaned_team_name)
    # set the market object's id
    team.id = team_id


def get_team_id(bookmaker_name: str, league: str, team_name: str) -> Optional[dict]:
    # create the team object
    team = create_team_object(league, team_name)
    # clean the team name
    cleaned_team = clean_team(get_team_name(team), team.league)
    # filter by league partition
    filtered_data = filter_team_data(team.league)
    # get the matched data if it exists
    if matched_id := filtered_data.get(cleaned_team):
        # cast to a string
        matched_id = str(matched_id)
        # update the team object with new data
        update_team_object(team, cleaned_team, matched_id)
        # update the shared dictionary of valid teams
        Teams.update_valid_data(bookmaker_name, tuple(team.__dict__.items()))
        # return a dictionary representing the team object for existing values...exclude league as it will be stored elsewhere in the games object
        return {key: value for key, value in team.__dict__.items() if value is not None}

    # update the shared dictionary of pending teams
    Teams.update_pending_data(bookmaker_name, tuple(team.__dict__.items()))