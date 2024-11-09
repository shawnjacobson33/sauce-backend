from typing import Optional


URL_MAP = {
    'Reference': {
        'NBA': {
            'schedule': 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'  # format ex: (2025, november)
        }, 'NFL': {
            'schedule': 'https://www.pro-football-reference.com/years/{}/games.htm'  # format ex: (2024)
        }, 'NHL': {
            'schedule': 'https://www.hockey-reference.com/leagues/NHL_{}_games.html'  # format ex: (2025)
        }, 'NCAAB': {
            'schedule': 'https://www.sports-reference.com/cbb/boxscores/index.cgi?month={}&day={}&year={}'  # format ex: (11, 9, 2024) no zeroes in front of single digit numbers
        }
    }, 'CBSSPORTS': {
        'NCAAF': {
            'schedule': 'https://www.cbssports.com/college-football/schedule/'
        }
    }
}


def get_url(source: str, league: str, content: str) -> Optional[str]:
    # get the data specific to source
    if source_data := URL_MAP.get(source):
        # get the dictionary specific to league within the source
        if league_data := source_data.get(league):
            # get the actual URL for the desired content
            return league_data.get(content)