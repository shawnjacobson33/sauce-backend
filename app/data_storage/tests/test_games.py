import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock
from app.data_storage.models import Team

from app.data_storage.main import Redis
from app.data_storage.stores.utils import convert_to_timestamp


class TestGames(unittest.TestCase):
    def _prepare_redis(self):
        # set up some team keys to map to game ids
        self.client.hset('games:std:nba', 'LAL', '1')
        self.client.hset('games:std:nba', 'GSW', '1')
        self.client.hset('games:std:nba', 'BOS', '2')
        self.client.hset('games:std:nba', 'BKN', '2')
        # set up some actual game data
        now = datetime.now()
        self.client.hset('1', mapping={
            'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW',
            'game_time': now.strftime('%Y%m%d'),
        })
        self.client.zadd('nba:gt', mapping={'1': convert_to_timestamp(now)})
        one_hour_from_now = datetime.now() + timedelta(hours=1)
        self.client.hset('2', mapping={
            'info': f'NBA_{one_hour_from_now.strftime('%Y%m%d')}_BOS@BKN',
            'game_time': one_hour_from_now.strftime('%Y%m%d'),
        })
        self.client.zadd('nba:gt', mapping={'2': convert_to_timestamp(one_hour_from_now)})

    @staticmethod
    def fixture(func):
        def wrapper(self, *args, **kwargs):
            self._prepare_redis()
            result = func(self, *args, **kwargs)
            self.client.flushall()
            return result

        return wrapper

    def setUp(self):
        self.redis = Redis()p
        self.client = self.redis.client
        self.games = self.redis.games

    @fixture
    def test_getid(self):
        team = Team(domain='NBA', name='LAL', std_name='LAL', full_name='Los Angeles Lakers')
        self.games.std_mngr.get_eid = MagicMock(return_value='1')
        result = self.games.getid(team)
        self.assertEqual(result, '1')
        self.games.std_mngr.get_eid.assert_called_once_with('NBA', 'LAL')

    @fixture
    def test_getids(self):
        result = list(self.games.getids('NBA', is_live=True))
        self.assertEqual(result, ['1'])
        self.games.live_mngr.getgameids.assert_called_once_with('NBA')

        result = list(self.games.getids('NBA', is_live=False))
        self.assertEqual(result, ['1', '2'])
        self.games.std_mngr.get_eids.assert_called_once_with('NBA')

    # @fixture
    # def test_getgame(self):
    #     team = Team(domain='nba', name='Lakers')
    #     self.games.get_entity = MagicMock(return_value='game_entity')
    #     result = self.games.getgame(team, report=True)
    #     self.assertEqual(result, 'game_entity')
    #     self.games.get_entity.assert_called_once_with('secondary', 'nba', 'Lakers', report=True)
    #
    # @fixture
    # def test_getgames(self):
    #     self.games.get_live_entities = MagicMock(return_value=iter(['game1', 'game2']))
    #     self.games.get_entities = MagicMock(return_value=iter(['game3', 'game4']))
    #
    #     result = list(self.games.getgames('nba', is_live=True))
    #     self.assertEqual(result, ['game1', 'game2'])
    #     self.games.get_live_entities.assert_called_once_with('nba')
    #
    #     result = list(self.games.getgames('nba', is_live=False))
    #     self.assertEqual(result, ['game3', 'game4'])
    #     self.games.get_entities.assert_called_once_with('nba')
    #
    # @fixture
    # def test_store(self):
    #     game1 = Game(info='NBA_20241113_BOS@BKN', game_time='2024-11-13T19:00:00Z')
    #     game2 = Game(info='NBA_20241114_LAL@GSW', game_time='2024-11-14T19:00:00Z')
    #     games = [game1, game2]
    #
    #     self.games.std_mngr.store_eids = MagicMock(return_value=[('game_id1', game1), ('game_id2', game2)])
    #     self.games._r.hset = MagicMock()
    #     self.games.live_mngr.track_game = MagicMock()
    #
    #     self.games.store('nba', games, to_timestamp=False)
    #
    #     self.games.std_mngr.store_eids.assert_called_once_with('nba', games, Games._get_keys)
    #     self.games._r.hset.assert_any_call('game_id1', mapping={'info': 'NBA_20241113_BOS@BKN', 'game_time': '2024-11-13T19:00:00Z'})
    #     self.games._r.hset.assert_any_call('game_id2', mapping={'info': 'NBA_20241114_LAL@GSW', 'game_time': '2024-11-14T19:00:00Z'})
    #     self.games.live_mngr.track_game.assert_any_call('nba', 'game_id1', '2024-11-13T19:00:00Z')
    #     self.games.live_mngr.track_game.assert_any_call('nba', 'game_id2', '2024-11-14T19:00:00Z')

if __name__ == '__main__':
    unittest.main()