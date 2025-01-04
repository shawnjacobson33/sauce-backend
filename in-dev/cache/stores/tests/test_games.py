import json
from datetime import datetime, timedelta

import pytest

from app.cache.main import RedisCache
from app.cache.models import Game

now = datetime.now()
one_hour_from_now = datetime.now() + timedelta(hours=1)


@pytest.fixture
def setup_redis():
    redis = RedisCache(db='dev')

    lookup_name = 'games:lookup:nba'
    redis.client.hset(lookup_name, 'LAL', 'g1')
    redis.client.hset(lookup_name, 'GSW', 'g1')
    redis.client.hset(lookup_name, 'BOS', 'g2')
    redis.client.hset(lookup_name, 'BKN', 'g2')

    info_name = 'games:info:nba'
    redis.client.hset(info_name, 'g1', json.dumps({
        'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW',
        'game_time': now.strftime("%Y-%m-%d %H:%M"),
    }))
    redis.client.hset(info_name, 'g2', json.dumps({
        'info': f'NBA_{one_hour_from_now.strftime('%Y%m%d')}_BOS@BKN',
        'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M"),
    }))

    yield redis.games
    redis.client.flushdb()


def test_getgame(setup_redis):
    result = setup_redis.getgame('NBA', 'LAL')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")}
    result = setup_redis.getgame('NBA', 'GSW')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")}
    result = setup_redis.getgame('NBA', 'BOS')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M")}
    result = setup_redis.getgame('NBA', 'BKN')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M")}


def test_getgames(setup_redis):
    result = list(setup_redis.getgames('NBA'))
    assert result == [{'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")},
                      {'info': f'NBA_{now.strftime('%Y%m%d')}_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M")}]


def test_storegames(setup_redis):
    game_time = (now + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    game1 = Game(domain='NBA', info=f'NBA_{now.strftime('%Y%m%d')}_MIN@ATL', game_time=game_time)
    game2 = Game(domain='NBA', info=f'NBA_{now.strftime('%Y%m%d')}_SAC@NYK', game_time=game_time)
    setup_redis.storegames('NBA', [game1, game2])

    result = setup_redis.getgame('NBA', 'MIN')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_MIN@ATL', 'game_time': game_time}
    result = setup_redis.getgame('NBA', 'ATL')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_MIN@ATL', 'game_time': game_time}
    result = setup_redis.getgame('NBA', 'SAC')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_SAC@NYK', 'game_time': game_time}
    result = setup_redis.getgame('NBA', 'NYK')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_SAC@NYK', 'game_time': game_time}
