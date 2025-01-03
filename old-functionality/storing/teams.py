from collections import defaultdict
from typing import Optional

from app import MongoDB, TEAMS_COLLECTION_NAME
from app import get_entities

# get the teams collection so we can structure that data in memory
teams_c = MongoDB.fetch_collection(TEAMS_COLLECTION_NAME)


class Teams:
    """
    {
        ('NBA', 'BOS'): 'Boston Celtics'  FULL NAME USED BY ROSTERS, LINE WORKERS ONLY USE FOR EXISTENCE CHECKING
        ...
    }
    """
    _teams: defaultdict[tuple[str, str], dict] = get_entities(teams_c)

    @classmethod
    def get_teams(cls, league: str = None) -> dict:
        """Used by Rosters to get all teams for a spec league"""
        return {key: val for key, val in cls._teams.items() if key[0] == league} if league else cls._teams

    @classmethod
    def get_team(cls, league: str, team: str) -> Optional[tuple[str, str]]:
        # get the team name using unique identifier
        if cls._teams.get((league, team)):
            # return the attribute predicated upon the content wanted
            return league, team