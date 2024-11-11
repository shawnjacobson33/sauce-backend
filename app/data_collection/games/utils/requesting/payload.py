from typing import Optional


URL_MAP = {
    'basketball-reference': {
        'schedule': 'https://www.basketball-reference.com/leagues/NBA_{}_games-{}.html'  # format ex: (2025, november)
    }, 'pro-football-reference': {
        'schedule': 'https://www.pro-football-reference.com/years/{}/games.htm'  # format ex: (2024)
    }, 'hockey-reference': {
        'schedule': 'https://www.hockey-reference.com/leagues/NHL_{}_games.html'  # format ex: (2025)
    }, 'sports-reference': {
        'schedule': 'https://www.sports-reference.com/cbb/boxscores/index.cgi?month={}&day={}&year={}'  # format ex: (11, 9, 2024) no zeroes in front of single digit numbers
    }, 'cbssports': {
        'schedule': 'https://www.cbssports.com/college-football/schedule/'
    }
}



def get_url(source: str, content: str) -> Optional[str]:
    # get the data specific to source
    if source_data := URL_MAP.get(source):
        # get the actual URL for the desired content
        return source_data.get(content)