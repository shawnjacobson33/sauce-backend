from collections import defaultdict
from datetime import datetime
from typing import Optional, Iterable

import json
import redis

from app.data_storage.models import Game
from app.data_storage.stores.base import DynamicDataStore


class Games(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        """
        Initializes the Games class with a Redis client instance.

        Args:
            r (redis.Redis): The Redis client used to interact with the Redis database.
        """
        super().__init__(r, 'games')


    def getid(self, league: str, team: str) -> Optional[str]:
        return self._r.hget(f'{self.name}:lookup:{league.lower()}', team)

    def _scan_game_ids(self, league: str) -> Iterable:
        counter = defaultdict(int)
        for _, subj_id in self._r.hscan_iter(f'{self.name}:lookup:{league.lower()}'):
            if not counter[subj_id]:
                counter[subj_id] += 1
                yield subj_id

    def getids(self, league: str) -> Iterable:
        yield from self._scan_game_ids(league)

    def getgame(self, league: str, team: str, report: bool = False) -> Optional[dict]:
        if game_id := self.getid(league, team):
            return self._r.hget(f'{self.name}:info:{league.lower()}', game_id)

    def getgames(self, league: str) -> Iterable:
        if subj_ids := self.getids(league):
            for subj_id in subj_ids: yield self._r.hget(f'{self.name}:lookup:{league.lower()}', subj_id)

    def putqueue(self, league: str, queue_type: str, game_id: str, game_time: datetime) -> None:
        self._r.zadd(f'{self.name}:{queue_type}:queue:{league.lower()}', {game_id: int(game_time.timestamp())})

    def checkqueue(self, league: str, queue_type: str) -> Optional[str]:
        return self._r.zrange(f'{self.name}:{queue_type}:queue:{league.lower()}', 0, 0)

    def popqueue(self, league: str, queue_type: str) -> Optional[str]:
        return self._r.zpopmin(f'{self.name}:{queue_type}:queue:{league.lower()}')

    def _store_in_lookup(self, league: str, game: Game) -> Optional[str]:
        try:
            game_id = self.id_mngr.generate()
            teams = game.info.split('_')[2].split('@')
            for team in teams:
                self._r.hset(f'{self.name}:lookup:{league.lower()}', team, game_id)

            return game_id

        except IndexError as e:
            self.id_mngr.decr()
            print(f"Error extracting team keys: {game} -> {e}")

    def storegames(self, league: str, games: list[Game]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for game in games:
                    game_id = self._store_in_lookup(league, game)
                    pipe.hset(f'{self.name}:info:{league.lower()}', game_id, json.dumps({
                        'info': game.info,
                        'game_time': game.game_time.strftime("%Y-%m-%d %H:%M:%S")
                    }))
                    self.putqueue(game.domain, 'upcoming', game_id, game.game_time)
                pipe.execute()

        except KeyError as e:
            self._handle_error(e)
