import asyncio

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.logistics import utils as lg_utils
from app.backend.data_collection.logistics.games import utils as gm_utils


# TODO: DUPLICATES FOUND
# NCAAM : Jordan Williams

NCAA_TEAM_NAME_URL_MAP = {
    'NCAAM': {
        'abbr_name': {
            'APLST': 'APPST',
            'SAMST': 'SAMHOU',
            'TXAM': 'TEXAM',
            'GAS': 'GASOU',
            'SALA': 'SALAB',
            'TXSTSM': 'TEXST'
        },
        'full_name': {
            'utsa-roadrunners': 'texassan-antonio-roadrunners',
            'byu-cougars': 'brigham-young-cougars',
            'miami-fl-hurricanes': 'miami-fla-hurricanes',
            'michigan-state-spartans': 'michigan-st-spartans',
            'fiu-panthers': 'florida-international-panthers',
            'new-mexico-state-aggies': 'new-mexico-st-aggies',
            'hawaii-rainbow-warriors': 'hawaii-warriors',
            'utep-miners': 'texasel-paso-miners',
            'miami-oh-redhawks': 'miami-ohio-redhawks',
            'appalachian-state-mountaineers': 'app-state-mountaineers',
            'texas-state-bobcats': 'texas-statesan-marcos-bobcats',
        }
    },
    'NCAA': {
        'full_name': {
            'texas-a&m-aggies': 'texas-am-aggies',
            "louisiana-ragin'-cajuns": 'louisiana-ragin-cajuns',
            'louisiana-monroe-warhawks': 'louisianamonroe-warhawks'
        }
    }

}


def get_full_team_name_formatted(team: dict, source: dc_utils.Source) -> str:
    # get the full team name formatted for the url
    full_name_formatted = ('-'.join(team['full_name'].lower().split())
                           .replace('.', '').replace('(', '').replace(')', ''))
    # there are some discrepancies between the format for NCAA full team names in the URL and in the DB
    if source.league == 'NCAA':
        # to get discrepancies that apply to NCAAM but don't apply to NCAAF
        if league_specific_map := NCAA_TEAM_NAME_URL_MAP.get(source.league_specific):
            # checks and gets a mapped team name
            full_name_formatted = league_specific_map['full_name'].get(full_name_formatted, full_name_formatted)

        # get the full team name mapped if it is in the NCAA map
        full_name_formatted = NCAA_TEAM_NAME_URL_MAP[source.league]['full_name'].get(full_name_formatted, full_name_formatted)

    return full_name_formatted


def get_abbr_team_name_formatted(league: str, abbr_name: str) -> str:
    # get the abbr team name formatted for the url
    if ncaa_team_map := NCAA_TEAM_NAME_URL_MAP.get(league):
        return ncaa_team_map['abbr_name'].get(abbr_name, abbr_name)

    return abbr_name


class RosterRetriever(dc_utils.Retriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self) -> None:
        # get the url for retrieving subjects
        url_data = lg_utils.get_url(self.source, 'rosters')
        # make a network request to extract all teams for which we want subjects
        if teams := dc_utils.Teams.get_teams(self.source.league):
            # initialize a list to hold requests to make
            tasks = list()
            # for each abbreviated and full name for each team
            for abbr_name, team_data in teams.items():
                # get the full team name prepared
                f_full_team_name = get_full_team_name_formatted(team_data, self.source)
                # get the abbreviated team name prepared
                f_abbr_name = get_abbr_team_name_formatted(self.source.league_specific, abbr_name)
                # format the url with the names
                formatted_url = url_data['url'].format(url_data['league'], f_abbr_name, f_full_team_name)
                # update team with its abbreviated name for outputting purposes
                team_data['abbr_name'] = abbr_name
                # add the request to tasks
                tasks.append(lg_utils.fetch(formatted_url, self._parse_roster, team_data))

            await asyncio.gather(*tasks)

    def _parse_roster(self, html_content, team_id: str):
        pass

    def update_subjects(self, subject: dict) -> None:
        self.data_collected += dc_utils.Subjects.update_subjects(subject)

    def log_team(self, abbr_name: str) -> None:
        print(f"[{self.source.league_specific}]: {abbr_name}")

    def __str__(self):
        return f'{str(self.data_collected)} ({self.source.league_specific}) Updated/Added subjects'