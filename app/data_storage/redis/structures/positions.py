from typing import Optional

import redis


class Positions:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _set_unidentified(self, *args) -> None:
        self.__r.sadd('positions:noid', 'positions:{}:{}'.format(*args))

    def get(self, sport: str, position: str) -> Optional[str]:
        if position := self.__r.hget(f'positions:{sport}', position):
            return position

        self._set_unidentified(sport, position)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('positions:noid')

    def store(self, sport: str, pos: str, std_pos: str, override: bool = False) -> None:
        pos_id = f'positions:{sport}'
        self.__r.hset(pos_id, key=pos, value=std_pos) if not override \
            else self.__r.hsetnx(pos_id, key=pos, value=std_pos)