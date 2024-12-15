from typing import Optional

import redis


class Positions:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def getpos(self, sport: str, pos: str, report: bool = False) -> Optional[str]:
        if position := self.__r.hget(f'positions:std:{sport}', pos):
            return position
        
        if report:
            self._set_noid(sport, pos)

    def getnoid(self) -> Optional[set[str]]:
        return self.__r.smembers('positions:noid')

    def _set_noid(self, sport: str, pos: str) -> None:
        self.__r.sadd('positions:noid', f'{sport}:{pos}')

    def store(self, sport: str, pos: str, std_pos: str) -> None:
        if not self.__r.hsetnx(f'positions:std:{sport}', key=pos, value=std_pos):
            print(f"Position: '{sport}:{pos}' already stored!")