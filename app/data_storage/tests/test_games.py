import json
from datetime import datetime, timedelta

import pytest

from app.data_storage.main import Redis
from app.data_storage.models import Game

now = datetime.now()
one_hour_from_now = datetime.now() + timedelta(hours=1)


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    redis.client.hset('games:lookup:nba', 'LAL', 'g1')
    redis.client.hset('games:lookup:nba', 'GSW', 'g1')
    redis.client.hset('games:lookup:nba', 'BOS', 'g2')
    redis.client.hset('games:lookup:nba', 'BKN', 'g2')

    game_json = json.dumps({
        'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW',
        'game_time': now.strftime("%Y-%m-%d %H:%M"),
    })
    redis.client.hset('games:nba', 'g1', game_json)
    game_json = json.dumps({
        'info': f'NBA_{one_hour_from_now.strftime('%Y%m%d')}_BOS@BKN',
        'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M"),
    })
    redis.client.hset('games:nba', 'g2', game_json)
    yield redis.games
    redis.client.flushdb()


def test_getgame(setup_redis):
    result = setup_redis.getgame('NBA', 'LAL')
    assert result == {'info': 'NBA_20241223_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}
    result = setup_redis.getgame('NBA', 'GSW')
    assert result == {'info': b'NBA_20241223_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}
    result = setup_redis.getgame('NBA', 'BOS')
    assert result == {'info': b'NBA_20241223_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}
    result = setup_redis.getgame('NBA', 'BKN')
    assert result == {'info': b'NBA_20241223_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}


def test_getgames(setup_redis):
    result = list(setup_redis.getgames('NBA'))
    assert result == [{b'info': b'NBA_20241223_LAL@GSW', b'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')},
                      {b'info': b'NBA_20241223_BOS@BKN', b'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}]


def test_getlivegames(setup_redis):
    result = list(setup_redis.getgames('NBA', is_live=True))
    assert result == [{b'info': b'NBA_20241223_LAL@GSW', b'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}]


def test_store(setup_redis):
    game1 = Game(domain='NBA', info='NBA_20241223_MIN@ATL', game_time=now.strftime("%Y-%m-%d %H:%M"))
    game2 = Game(domain='NBA', info='NBA_20241223_SAC@NYK', game_time= one_hour_from_now.strftime("%Y-%m-%d %H:%M"))
    setup_redis.store('NBA', [game1, game2])

    result = setup_redis.getgame('NBA', 'MIN')
    assert result == {b'info': b'NBA_20241223_MIN@ATL', b'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}
    result = setup_redis.getgame('NBA', 'ATL')
    assert result == {b'info': b'NBA_20241223_MIN@ATL', b'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}
    result = setup_redis.getgame('NBA', 'SAC')
    assert result == {b'info': b'NBA_20241223_SAC@NYK', b'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}
    result = setup_redis.getgame('NBA', 'NYK')
    assert result == {b'info': b'NBA_20241223_SAC@NYK', b'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}

    result = list(setup_redis.getgames('NBA', is_live=True))
    assert result == [{b'info': b'NBA_20241223_MIN@ATL', b'game_time': now.strftime("%Y-%m-%d %H:%M").encode('utf-8')}]