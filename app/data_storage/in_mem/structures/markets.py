from collections import namedtuple
from typing import Optional

import redis

from app.data_storage.in_mem.structures.utils import StaticDataStore


NAMESPACE = {
    'hstd': 'markets:std:{}',
    'snoid': 'markets:noid',
}


class Markets(StaticDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'markets')

    def getmarket(self, sport: str, market: str) -> Optional[str]:
        if std_market := self.__r.hget(self._hstd.format(sport), market):
            return std_market

        self._set_noid(sport, market)

    def _set_noid(self, sport: str, market: str) -> None:
        self.__r.sadd(self._snoid, f'{sport}:{market}')

    def store(self, market: namedtuple) -> None:
        try:
            new_updates = 0
            new_m_id = self.__aid.generate()
            for market_name in {market.name, market.std_name}:
                self.__r.hsetnx(self._hstd.format(market.sport), key=market_name, value=new_m_id)

            if not self.__r.hsetnx(self._hstd.format(sport), key=market, value=std_market):
                print(f"Market: '{sport}:{market}' already stored!")

        except KeyError as e:
            print("Error:", e)
            self.__aid.decrement()