from collections import defaultdict
from typing import Optional

from backend.app.database import MongoDB, TEAMS_COLLECTION_NAME
from backend.app.data_collection.workers.utils.storing.utils import get_entities

# get the teams collection so we can structure that data in memory
teams_c = MongoDB.fetch_collection(TEAMS_COLLECTION_NAME)


class Teams:
    """
    {
        ('NBA', 'BOS'): {
            'id': 'asdooiuh34',
            'full_name': 'Boston Celtics',
        },
        ...
    }
    """
    _teams: defaultdict[tuple[str, str], dict] = get_entities(teams_c)

    @classmethod
    def get_teams(cls, league: str = None) -> dict:
        return cls._teams

    @classmethod
    def get_team(cls, league: str, abbr_name: str, content: str = None) -> Optional[str]:
        # get the team name using unique identifier
        if team := cls._teams.get((league, abbr_name)):
            # return the attribute predicated upon the content wanted
            return team['full_name'] if content == 'full_name' else team['id']