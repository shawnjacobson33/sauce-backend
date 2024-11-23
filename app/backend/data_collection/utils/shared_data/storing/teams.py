from collections import defaultdict
from typing import Optional

from app.backend import database as db
from app.backend.data_collection.utils.definitions import IN_SEASON_LEAGUES

# get league partitions
PARTITIONS = [league if 'NCAA' not in league else 'NCAA' for league in IN_SEASON_LEAGUES]  # Because all college team names are stored under 'NCAA' umbrella


def structure_pair(docs: list[dict]) -> dict:
    # use abbreviated names as keys
    structured_docs = dict()
    # for each team
    for doc in docs:
        # give each team name type its id as a pair and update the dictionary
        structured_docs[doc['abbr_name']] = {
            'full_name': doc['full_name'],
            'id': str(doc['_id']),
        }
        # structured_docs[doc['abbr_name']] = structured_docs[doc['full_name']] = str(doc['_id'])
        # TODO: remove the full name when done
        # TODO: think about for rosters you need full names

    # return the structured documents
    return structured_docs


def get_teams() -> dict:
    # get collection being used
    teams_cursor = db.MongoDB.fetch_collection(db.TEAMS_COLLECTION_NAME)
    # initialize a dictionary to hold all the data partitioned
    partitioned_teams = dict()
    # for each partition in the partitions predicated upon the cursor name
    for league in PARTITIONS:
        # filter by league or sport and don't include the batch_id
        filtered_teams = teams_cursor.find({'league': league})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_teams[league] = structure_pair(filtered_teams)

    # return the fully structured and partitioned data
    return partitioned_teams


class Teams:
    """
    {
        'NBA': {
            'BOS': '123090asd'
            ...
        },
        ...
    }
    """
    _teams: defaultdict[str, dict[str, dict]] = get_teams()

    @classmethod
    def get_teams(cls, league: str = None) -> dict[str, dict]:
        return cls._teams[league] if league else cls._teams

    @classmethod
    def get_team(cls, league: str, abbr_name: str, content: str = None) -> Optional[str]:
        # gets the teams filtered down by league
        if league_filtered_teams := cls._teams.get(league):
            # gets the id for the abbreviated team name
            if team := league_filtered_teams.get(abbr_name):
                # return the attribute predicated upon the content wanted
                return team['full_name'] if content == 'full_name' else team['id']