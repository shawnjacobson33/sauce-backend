import pytest

from app.data_storage.models import Market
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    redis.client.hset('markets:std:nba', 'player_points', 'Points')
    redis.client.hset('markets:std:nba', 'POINTS', 'Points')
    redis.client.hset('markets:std:nba', 'Points', 'Points')
    yield redis.markets
    redis.client.flushdb()


def test_getmarket(setup_redis):
    result = setup_redis.getmarket('NBA', 'player_points')
    assert result == b'Points'
    result = setup_redis.getmarket('NBA', 'POINTS')
    assert result == b'Points'
    result = setup_redis.getmarket('NBA', 'Points')
    assert result == b'Points'


def test_getmarkets(setup_redis):
    result = list(setup_redis.getmarkets('NBA'))
    assert result == [(b'player_points', b'Points'), (b'POINTS', b'Points'), (b'Points', b'Points')]


def test_store(setup_redis):
    market1 = Market(domain='NFL', name='player_pass_yards', std_name='Pass Yards')
    market2 = Market(domain='NFL', name='passing yards', std_name='Pass Yards')
    setup_redis.store("NFL", [market1, market2])

    result = setup_redis.getmarket('NFL', 'player_pass_yards')
    assert result == b'Pass Yards'

    result = setup_redis.getmarket('NFL', 'passing yards')
    assert result == b'Pass Yards'

    result = list(setup_redis.getmarkets('NFL'))
    assert result == [(b'Pass Yards', b'Pass Yards'), (b'player_pass_yards', b'Pass Yards'),
                      (b'passing yards', b'Pass Yards')]
