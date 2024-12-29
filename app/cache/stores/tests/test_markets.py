import pytest

from app.data_storage.models import Market
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    lookup_name = 'markets:lookup:basketball'
    redis.client.hset(lookup_name, 'player_points', 'Points')
    redis.client.hset(lookup_name, 'POINTS', 'Points')
    redis.client.hset(lookup_name, 'Points', 'Points')
    yield redis.markets
    redis.client.flushdb()


def test_getmarket(setup_redis):
    result = setup_redis.getmarket('Basketball', 'player_points')
    assert result == b'Points'
    result = setup_redis.getmarket('Basketball', 'POINTS')
    assert result == b'Points'
    result = setup_redis.getmarket('Basketball', 'Points')
    assert result == b'Points'


def test_getmarkets(setup_redis):
    result = set(setup_redis.getmarkets('Basketball'))
    assert result == {(b'player_points', b'Points'), (b'POINTS', b'Points'), (b'Points', b'Points')}


def test_store(setup_redis):
    market1 = Market(domain='Football', name='player_pass_yards', std_name='Pass Yards')
    market2 = Market(domain='Football', name='passing yards', std_name='Pass Yards')
    setup_redis.storemarkets("Football", [market1, market2])

    result = setup_redis.getmarket('Football', 'player_pass_yards')
    assert result == b'Pass Yards'

    result = setup_redis.getmarket('Football', 'passing yards')
    assert result == b'Pass Yards'

    result = set(setup_redis.getmarkets('Football'))
    assert result == {(b'Pass Yards', b'Pass Yards'), (b'player_pass_yards', b'Pass Yards'),
                      (b'passing yards', b'Pass Yards')}
