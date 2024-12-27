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
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")}
    result = setup_redis.getgame('NBA', 'GSW')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")}
    result = setup_redis.getgame('NBA', 'BOS')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M")}
    result = setup_redis.getgame('NBA', 'BKN')
    assert result == {'info': f'NBA_{now.strftime('%Y%m%d')}_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M")}


def test_getgames(setup_redis):
    result_json = list(setup_redis.getgames('NBA'))
    result = [json.loads(r) for r in result_json]
    assert result == [{'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")},
                      {'info': f'NBA_{now.strftime('%Y%m%d')}_BOS@BKN', 'game_time': one_hour_from_now.strftime("%Y-%m-%d %H:%M")}]


def test_getlivegames(setup_redis):
    result = list(setup_redis.getlivegames('NBA'))
    live_games = [{'info': f'NBA_{now.strftime('%Y%m%d')}_LAL@GSW', 'game_time': now.strftime("%Y-%m-%d %H:%M")}]
    assert result == live_games

    live_game = setup_redis._r.hget('games:live:nba', 'g1')
    assert live_game
    result = json.loads(live_game)
    assert result == live_games[0]

    assert not setup_redis._r.hget('games:nba', 'g1')


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
