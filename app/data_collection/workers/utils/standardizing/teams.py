from typing import Optional

from app import Teams, RelevantData, ProblemData
from app import clean_team


def get_team(source_name: str, league: str, team: str) -> Optional[dict]:
    # clean the team name if the source isn't cbssports
    c_team_name = clean_team(team, league)
    # DATA LOOKS LIKE --> ('NBA', 'BOS')
    if team_id := Teams.get_team(league, c_team_name):
        # teams that come from schedule parsers are not necessarily relevant
        if 'cbssports' not in source_name:
            # update the shared dictionary of valid teams
            RelevantData.update_relevant_teams(team_id, source_name)
        # return a dictionary representing the team object for existing values
        return team_id

    # update the shared dictionary of pending teams
    ProblemData.update_problem_teams(team_id, source_name)