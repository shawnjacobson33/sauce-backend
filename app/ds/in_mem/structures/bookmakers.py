from typing import Optional

import redis


class Bookmakers:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def get(self, bookmaker: str) -> Optional[str]:
        return self.__r.hget(f'bookmakers', bookmaker)

    def store(self, bookmaker: str, default_odds: str) -> None:
        self.__r.hset(f'bookmakers', mapping={
            bookmaker: default_odds
        })