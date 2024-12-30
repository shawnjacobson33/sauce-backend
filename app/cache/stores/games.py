from collections import defaultdict
from datetime import datetime
from typing import Optional, Iterable

import json
import redis

from app.cache.models import Game
from app.cache.stores.base import DataStore


class Games(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'games')


    def getid(self, league: str, team: str) -> Optional[str]:
        return self._r.hget(f'{self.lookup_name}:{league.lower()}', team)

    def _scan_game_ids(self, league: str) -> Iterable:
        counter = defaultdict(int)
        for _, game_id in self._r.hscan_iter(f'{self.lookup_name}:{league.lower()}'):
            if not counter[game_id]:
                counter[game_id] += 1
                yield game_id

    def getids(self, league: str) -> Iterable:
        yield from self._scan_game_ids(league)

    def getgame(self, league: str, team: str, report: bool = False) -> Optional[dict]:
        if game_id := self.getid(league, team):
            if game_json := self._r.hget(f'{self.info_name}:{league.lower()}', game_id):
                return json.loads(game_json)

    def getgames(self, league: str) -> Iterable:
        if game_ids := self.getids(league):
            for game_id in game_ids:
                if game_json := self._r.hget(f'{self.info_name}:{league.lower()}', game_id):
                    yield json.loads(game_json)

    def putqueue(self, league: str, queue_type: str, game_id: str, game_time: datetime) -> None:
        self._r.zadd(f'{self.name}:{queue_type}:{league.lower()}', {game_id: int(game_time.timestamp())})

    def checkqueue(self, league: str, queue_type: str) -> Optional[str]:
        return self._r.zrange(f'{self.name}:{queue_type}:{league.lower()}', 0, 0)

    def popqueue(self, league: str, queue_type: str) -> Optional[str]:
        return self._r.zpopmin(f'{self.name}:{queue_type}:{league.lower()}')

    def _store_in_lookup(self, league: str, game: Game) -> Optional[str]:
        try:
            game_id = self.id_mngr.generate()
            teams = game.info.split('_')[2].split('@')
            for team in teams:
                self._r.hset(f'{self.lookup_name}:{league.lower()}', team, game_id)

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
                    pipe.hset(f'{self.info_name}:{league.lower()}', game_id, json.dumps({
                        'info': game.info,
                        'game_time': game.game_time.strftime("%Y-%m-%d %H:%M:%S")
                    }))
                    self.putqueue(game.domain, 'upcoming', game_id, game.game_time)
                pipe.execute()

        except KeyError as e:
            self._handle_error(e)
