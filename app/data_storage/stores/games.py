from typing import Optional, Union, Any, Iterable

import redis

from app.data_storage.models import Team
from app.data_storage.stores import utils
from app.data_storage.stores.dynamic import L2DynamicDataStore as L2Dynamic

NAMESPACE = {
    'std': 'games:std:{}',
    'slive': 'games:live:{}',
    'zwatch': 'games:watch:{}'
}


class Games(L2Dynamic):
    """
    A class to manage game-related operations in a Redis-backed storage system.

    Attributes:
        _r (redis.Redis): The Redis connection object.
        _slive (str): Redis key pattern for live games.
        _zwatch (str): Redis key pattern for games being watched.
    """
    def __init__(self, r: redis.Redis, name: str):
        super().__init__(r, 'games')
        self._slive = NAMESPACE['slive']
        self._zwatch = NAMESPACE['zwatch']

    def getid(self, team: Team) -> Optional[str]:
        return self.geteid(team.domain, team.name)

    def getids(self, league: str = None, is_live: bool = False) -> Iterable:
        yield from self.geteids(league)

    def getgame(self, team: Team, report: bool = False) -> Optional[str]:
        return self.getentity(team.domain, team.name, report=report)

    def _getlivegames(self, league: str) -> Optional[set[Union[str, dict[str, Any]]]]:
        if g_ids := self.live_mngr.getgameids(league):
            for g_id in g_ids: yield self._r.hgetall(g_id)

    def getgames(self, league: str, is_live: bool = False) -> Iterable:
        yield from (self._getlivegames(league) if is_live else self.getentities(league))

    def getliveids(self, league: str) -> Optional[set[str]]:
        return self.live_mngr.getall(league)



    def _set_game_id(self, league: str, g_id: str, team: str) -> None:
        """
        Associate a game ID with a team ID in a specific league.

        Args:
           league (str): The league name.
           g_id (str): The game ID.
           team (str): The team name.
        """
        t_id = self.teams.getteamid(league, team)
        self._r.hsetnx(self._std.format(league), key=t_id, value=g_id)

    def _set_hash_and_watch(self, league: str, g_id: str, mapping: dict) -> None:
        """
        Store game information and add the game to the watch list.

        Args:
            league (str): The league name.
            g_id (str): The game ID.
            mapping (dict): Contains game details.
        """
        self._r.hset(g_id, mapping={
            'info': mapping['info'],
            'game_time': mapping['game_time']
        })
        utils.watch_game_time(self._r, self._zwatch.format(league), key=g_id, game_time=mapping['game_time'])

    def store(self, league: str, mapping: dict) -> None:
        """
        Store a new game's data in the system.

        Args:
            league (str): The league name.
            mapping (dict): Game information including teams and additional details.

        Raises:
            KeyError: If required keys are missing from the mapping.
        """
        try:
            new_g_id = self.__aid.generate()
            for team in mapping['info'].split('_')[2].split('@'):
                self._set_game_id(league, new_g_id, team)

            mapping['game_time'] = utils.convert_to_timestamp(mapping['game_time'])
            self._set_hash_and_watch(league, new_g_id, mapping)

        except KeyError as e:
            print("Error: ", e)
            self.__aid.decrement()

    def flush(self, name: str, league: str) -> None:
        f_name = NAMESPACE[name].format(league)
        iter_keys = self._r.hscan_iter(f_name) if 'std' in f_name else self._r.sscan_iter(f_name)
        keys = list(iter_keys)
        self._r.delete(*keys)
        print(f"[Games]: FOR {f_name} Deleted {keys[0]}, {keys[1]}, ...")

    def flushall(self) -> None:
        std_keys = list(self._r.hscan_iter('std'))
        slive_keys = list(self._r.sscan_iter('slive'))
        zwatch_keys = list(self._r.zscan_iter('zwatch'))
        with self._r.pipeline() as pipe:
            pipe.multi()
            self._r.delete(*std_keys)
            self._r.delete(*slive_keys)
            self._r.delete(*zwatch_keys)
            pipe.execute()