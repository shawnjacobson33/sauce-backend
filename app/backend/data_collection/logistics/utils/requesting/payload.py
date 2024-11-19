from typing import Optional, Union

from app.backend.data_collection.utils import Source


CBSSPORTS_LEAGUE_MAP = {
    'NBA': 'nba',
    'NFL': 'nfl',
    'MLB': 'mlb',
    'NHL': 'nhl',
    'WNBA': 'wnba',
    'NCAAM': 'college-basketball',
    'NCAAF': 'college-football'
}


URL_MAP = {
    'cbssports': {
        'ALL': {
            'box_scores': 'https://www.cbssports.com/{}/gametracker/boxscore/{}/',  # NBA_20241113_BOS@BKN
            'rosters': 'https://www.cbssports.com/{}/teams/{}/{}/roster/',
            'teams': 'https://www.cbssports.com/{}/teams/'
        }, 'NBA': {
            'schedule': 'https://www.cbssports.com/nba/schedule/{}/'
        }, 'NCAAM': {
            'schedule': 'https://www.cbssports.com/college-basketball/schedule/ALL/{}/'  # 20241112
        }, 'NFL': {
            'schedule': 'https://www.cbssports.com/nfl/schedule/'
        }, 'NCAAF': {
            'schedule': 'https://www.cbssports.com/college-football/schedule/'
        }, 'NHL': {
            'schedule': 'https://www.cbssports.com/nhl/schedule/{}/'
        }
    }
}


def get_url(source: Source, content: str) -> Optional[Union[dict, str]]:
    # get the data specific to source
    if source_urls := URL_MAP.get(source.name.split('-')[0]):
        # check if a general url is mapped for the desired content
        if gen_url := source_urls.get('ALL').get(content):
            # format the league tailored for the url address
            formatted_league = CBSSPORTS_LEAGUE_MAP.get(source.league_specific, source.league_specific)
            # return data found
            return {
                'url': gen_url,
                'league': formatted_league
            }

        # get the all the urls specific to this league
        if league_urls := source_urls.get(source.league_specific):
            # get the desired content for the league specific urls
            if spec_url := league_urls.get(content):
                return spec_url
