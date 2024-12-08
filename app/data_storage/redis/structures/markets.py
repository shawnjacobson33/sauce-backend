from typing import Optional

import redis


class Markets:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _set_unidentified(self, *args) -> None:
        self.__r.sadd('markets:noid', 'markets:{}:{}'.format(*args))

    def get(self, sport: str, market: str) -> Optional[str]:
        if market := self.__r.hget(f'markets:{sport}', market):
            return market

        self._set_unidentified(sport, market)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('markets:noid')

    def store(self, sport: str, market: str, std_market: str, override: bool = False) -> None:
        market_id = f'markets:{sport}'
        self.__r.hset(market_id, key=market, value=std_market) if not override \
            else self.__r.hsetnx(market_id, key=market, value=std_market)
