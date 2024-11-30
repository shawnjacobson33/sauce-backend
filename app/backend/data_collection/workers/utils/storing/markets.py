from collections import defaultdict
from typing import Optional

from app.backend.database import MongoDB, MARKETS_COLLECTION_NAME
from app.backend.data_collection.workers.utils.storing import get_entities

# get the teams collection so we can structure that data in memory
markets_c = MongoDB.fetch_collection(MARKETS_COLLECTION_NAME)


class Markets:
    """
    {
        ('Basketball', 'Fantasy Points'): 'a0sd9u1h2h' (market id),
        ...
    }
    """
    _markets: defaultdict[tuple[str, str], dict] = get_entities(markets_c)

    @classmethod
    def get_markets(cls, sport: str = None) -> dict:
        return cls._markets

    @classmethod
    def get_market(cls, sport: str, market: str) -> Optional[str]:
        return cls._markets.get((sport, market))