from typing import Optional

import redis


class Markets:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _set_unidentified(self, sport: str, market: str) -> None:
        self.__r.sadd('markets:noid', f'{sport}:{market}')

    def get(self, sport: str, market: str) -> Optional[str]:
        if std_market := self.__r.hget(f'markets:std:{sport}', market):
            return std_market

        self._set_unidentified(sport, market)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('markets:noid')

    def store(self, sport: str, market: str, std_market: str) -> None:
        if not self.__r.hsetnx(f'markets:std:{sport}', key=market, value=std_market):
            print(f"Market: '{sport}:{market}' already stored!")
