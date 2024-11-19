from typing import Optional

from app.backend.data_collection.utils.modelling import Team
from app.backend.data_collection.utils.shared_data import Teams, RelevantData, ProblemData
from app.backend.data_collection.utils.cleaning import clean_team


def update_team_object(team: Team, team_name: tuple[str, str], team_id: str) -> None:
    # update the abbreviated or full team name of the object
    setattr(team, team_name[0], team_name[1])
    # set the market object's id
    team.id = team_id


def get_team_id(source_name: str, league: str, abbr_team_name: str) -> Optional[dict]:
    # create the team object
    team = Team(league=league, abbr_name=abbr_team_name)
    # clean the team name
    cleaned_team_name = clean_team(abbr_team_name, league)
    # get the matched data if it exists
    if team_id := Teams.get_team(league, cleaned_team_name[1]):
        # update the team object with new data
        update_team_object(team, cleaned_team_name, team_id)
        # update the shared dictionary of valid teams
        RelevantData.update_relevant_teams(team.__dict__, source_name, league)
        # return a dictionary representing the team object for existing values...exclude league as it will be stored elsewhere in the games object
        return {key: value for key, value in team.__dict__.items() if value is not None}

    # update the shared dictionary of pending teams
    ProblemData.update_problem_teams(team.__dict__, source_name, league)