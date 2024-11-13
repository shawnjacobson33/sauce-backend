from typing import Optional


URL_MAP = {
    'cbssports-nba': {
        'schedule': 'https://www.cbssports.com/nba/schedule/{}/'  # 20241112
    }, 'cbssports-nfl': {
        'schedule': 'https://www.cbssports.com/nfl/schedule/'
    }, 'cbssports-ncaam': {
        'schedule': 'https://www.cbssports.com/college-basketball/schedule/ALL/{}/'  # 20241113
    }, 'cbssports-ncaaf': {
        'schedule': 'https://www.cbssports.com/college-football/schedule/'
    }, 'cbssports-ncaaw': {
        'schedule': 'https://www.cbssports.com/womens-college-basketball/schedule/ALL/{}/'  # 20241113
    }, 'cbssports-nhl': {
        'schedule': 'https://www.cbssports.com/nhl/schedule/{}/'
    }
}



def get_url(source: str, content: str) -> Optional[str]:
    # get the data specific to source
    if source_data := URL_MAP.get(source):
        # get the actual URL for the desired content
        return source_data.get(content)