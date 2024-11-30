from typing import Optional

from app.backend.data_collection.workers.utils.modelling import Team
from app.backend.data_collection.workers.utils import Teams, RelevantData, ProblemData
from app.backend.data_collection.workers.utils.cleaning import clean_team


def get_team(source_name: str, league: str, abbr_team_name: str) -> Optional[dict]:
    # create the team object
    team = Team(league=league, abbr_name=abbr_team_name)
    # clean the team name if the source isn't cbssports
    cleaned_team_name = clean_team(abbr_team_name, league) if 'cbssports' not in source_name else abbr_team_name
    # get the matched data if it exists
    if team_id := Teams.get_team(league, cleaned_team_name, content='id'):
        # update the team object with new data
        team.id, team.abbr_name = team_id, cleaned_team_name
        # teams that come from schedule parsers are not necessarily relevant
        if 'cbssports' not in source_name:
            # update the shared dictionary of valid teams
            RelevantData.update_relevant_teams(team.__dict__, source_name)
        # return a dictionary representing the team object for existing values
        return {key: value for key, value in team.__dict__.items() if value is not None}

    # update the shared dictionary of pending teams
    ProblemData.update_problem_teams(team.__dict__, source_name)