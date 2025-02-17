from app.services.utils.requesting import Requesting
from app.services.utils.storing import Storing
from app.services.utils.cleaning import Cleaning


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
