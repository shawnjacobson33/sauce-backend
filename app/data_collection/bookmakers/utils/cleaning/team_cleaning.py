


TEAMS_MAP = {
    'NBA': {

    },
    'NFL': {},
    'MLB': {},
    'NHL': {},
    'WNBA': {},
    'NCAA': {}
}


def clean_team(team: str, league: str):
    # because in the database...to reduce redundancy all college team names have one league 'NCAA'
    formatted_league = league[:4] if 'NCAA' in league else league
    # get the teams associated with the mapped league name
    if partitioned_team_map := TEAMS_MAP.get(formatted_league):
        # map the team name to a standardized version in the TEAM_MAP, if it doesn't exist
        if mapped_team := partitioned_team_map.get(team):
            # return the standardized team name
            return mapped_team

    # otherwise return original team name
    return team