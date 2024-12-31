import pytest

from app.cache.models import Position
from app.cache.main import RedisCache


@pytest.fixture
def setup_redis():
    redis = RedisCache(db='dev')
    lookup_name = 'positions:lookup:basketball'
    redis.client.hset(lookup_name, 'PG', 'G')
    redis.client.hset(lookup_name, 'SG', 'G')
    redis.client.hset(lookup_name, 'G', 'G')
    redis.client.hset(lookup_name, 'SF', 'F')
    redis.client.hset(lookup_name, 'PF', 'F')
    redis.client.hset(lookup_name, 'F', 'F')
    yield redis.positions
    redis.client.flushdb()


def test_getpos(setup_redis):
    result = setup_redis.getposition('Basketball', 'PG')
    assert result == b'G'
    result = setup_redis.getposition('Basketball', 'SG')
    assert result == b'G'
    result = setup_redis.getposition('Basketball', 'SF')
    assert result == b'F'
    result = setup_redis.getposition('Basketball', 'PF')
    assert result == b'F'


def test_getpositions(setup_redis):
    result = set(setup_redis.getpositions('Basketball'))
    assert result == {(b'PG', b'G'), (b'SG', b'G'), (b'G', b'G'), (b'SF', b'F'), (b'PF', b'F'), (b'F', b'F')}


def test_storepositions(setup_redis):
    pos1 = Position(domain='Football', name='LG', std_name='OG')
    pos2 = Position(domain='Football', name='LT', std_name='OT')
    setup_redis.storepositions("Football", [pos1, pos2])

    result = setup_redis.getposition('Football', 'LG')
    assert result == b'OG'

    result = setup_redis.getposition('Football', 'LT')
    assert result == b'OT'

    result = set(setup_redis.getpositions('Football'))
    assert result == {(b'LG', b'OG'), (b'OG', b'OG'), (b'OT', b'OT'), (b'LT', b'OT')}
