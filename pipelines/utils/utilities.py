from pipelines.utils.requesting import Requesting
from pipelines.utils.storing import Storing
from pipelines.utils.cleaning import Cleaning
from pipelines.utils.logging import Logger

logger = Logger()

requester = Requesting()

storer = Storing()

cleaner = Cleaning()


_SPORT_MAP = {
    'NFL': 'Football',
    'NCAAF': 'Football',
    'NBA': 'Basketball',
    'NCAAB': 'Basketball',
    'NCAAM': 'Basketball',
    'WNBA': 'Basketball',
    'MLB': 'Baseball',
    'NHL': 'Ice Hockey',
}


def get_sport(league: str) -> str:
    if sport := _SPORT_MAP.get(league):
        return sport

    raise ValueError(f"No sport mapping found for league: '{league}'")
