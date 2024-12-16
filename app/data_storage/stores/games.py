from typing import Optional, Union, Any

import redis

from app.data_storage.stores import Teams, utils


NAMESPACE = {
    'hstd': 'games:std:{}',
    'slive': 'games:live:{}',
    'zwatch': 'games:watch:{}'
}


class Games:
    """
    A class to manage game-related operations in a Redis-backed storage system.

    Attributes:
        __r (redis.Redis): The Redis connection object.
        teams (Teams): An instance of the Teams class for team-related operations.
        _hstd (str): Redis key pattern for standard games.
        _slive (str): Redis key pattern for live games.
        _zwatch (str): Redis key pattern for games being watched.
        __aid (utils.AutoId): Utility for generating unique game IDs.
    """
    def __init__(self, r: redis.Redis, teams: Teams):
        """
        Initialize the Games class.

        Args:
            r (redis.Redis): Redis connection object.
            teams (Teams): Teams instance for managing team information.
        """
        self.__r = r
        self.teams = teams
        self._hstd = NAMESPACE['hstd']
        self._slive = NAMESPACE['slive']
        self._zwatch = NAMESPACE['zwatch']
        self.__aid = utils.AutoId(r, 'games')

    def getgameid(self, league: str, t_id: str) -> Optional[str]:
        """
        Get the game ID associated with a team in a specific league.

        Args:
            league (str): The league name.
            t_id (str): The team ID.

        Returns:
            Optional[str]: The game ID if it exists, otherwise None.
        """
        return self.__r.hget(self._hstd.format(league), key=t_id)

    def getgame(self, league: str, t_id: str, key: str = None) -> Optional[Union[dict[str, str], str]]:
        """
        Retrieve information about a game.

        Args:
            league (str): The league name.
            t_id (str): The team ID.
            key (str, optional): Specific key to fetch from the game info. Defaults to None.

        Returns:
            Optional[Union[dict[str, str], str]]: The game information as a dictionary or specific field value.
        """
        if game_id := self.getgameid(league, t_id):
            return self.__r.hgetall(game_id) if not key else self.__r.hget(game_id, key=key)

    def getgames(self, league: str = None) -> list:
        """
        Retrieve details of all teams or teams in a specific league.

        Args:
            league (str, optional): The league to filter by. Defaults to None.

        Returns:
            list: List of dictionaries containing team details.
        """
        if not league:
            return [self.__r.hgetall(t_id) for t_id in self.__r.keys('game:*')]

        f_hstd_name = self._hstd.format(league)
        lt_ids = set(self.__r.hgetall(f_hstd_name).values())
        return [self.__r.hgetall(lt_id) for lt_id in lt_ids]

    def getlivegameids(self, league: str) -> Optional[set[str]]:
        """
        Get IDs of live games currently in play for a league.

        Args:0.

            league (str): The league name.

        Returns:
            Optional[set[str]]: Set of live game IDs, or None if none are found.
        """
        return utils.get_live_ids(self.__r, self._slive.format(league), self._zwatch.format(league))

    def getlivegames(self, league: str) -> Optional[set[Union[str, dict[str, Any]]]]:
        """
        Get details of games currently in play for a league.

        Args:
        league (str): The league name.

        Returns:
        Optional[set[Union[str, dict[str, Any]]]]: Set of game details or game IDs if no details are available.
        """
        if g_ids := self.getlivegameids(league):
            return {self.__r.hgetall(g_id) for g_id in g_ids}

    def _set_game_id(self, league: str, g_id: str, team: str) -> None:
        """
        Associate a game ID with a team ID in a specific league.

        Args:
           league (str): The league name.
           g_id (str): The game ID.
           team (str): The team name.
        """
        t_id = self.teams.getteamid(league, team)
        self.__r.hsetnx(self._hstd.format(league), key=t_id, value=g_id)

    def _set_hash_and_watch(self, league: str, g_id: str, mapping: dict) -> None:
        """
        Store game information and add the game to the watch list.

        Args:
            league (str): The league name.
            g_id (str): The game ID.
            mapping (dict): Contains game details.
        """
        self.__r.hset(g_id, mapping={
            'info': mapping['info'],
            'game_time': mapping['game_time']
        })
        utils.watch_game_time(self.__r, self._zwatch.format(league), key=g_id, game_time=mapping['game_time'])

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
        iter_keys = self.__r.hscan_iter(f_name) if 'std' in f_name else self.__r.sscan_iter(f_name)
        keys = list(iter_keys)
        self.__r.delete(*keys)
        print(f"[Games]: FOR {f_name} Deleted {keys[0]}, {keys[1]}, ...")

    def flushall(self) -> None:
        hstd_keys = list(self.__r.hscan_iter('hstd'))
        slive_keys = list(self.__r.sscan_iter('slive'))
        zwatch_keys = list(self.__r.zscan_iter('zwatch'))
        with self.__r.pipeline() as pipe:
            pipe.multi()
            self.__r.delete(*hstd_keys)
            self.__r.delete(*slive_keys)
            self.__r.delete(*zwatch_keys)
            pipe.execute()