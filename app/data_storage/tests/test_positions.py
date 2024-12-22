import pytest

from app.data_storage.models import Position
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis()
    redis.client.hset('positions:std:nba', 'PG', 'G')
    redis.client.hset('positions:std:nba', 'SG', 'G')
    redis.client.hset('positions:std:nba', 'G', 'G')
    redis.client.hset('positions:std:nba', 'SF', 'F')
    redis.client.hset('positions:std:nba', 'PF', 'F')
    redis.client.hset('positions:std:nba', 'F', 'F')
    yield redis.positions
    redis.client.delete('positions:std:nba')


def test_getpos(setup_redis):
    result = setup_redis.getpos('NBA', 'PG')
    assert result == b'G'
    result = setup_redis.getpos('NBA', 'SG')
    assert result == b'G'
    result = setup_redis.getpos('NBA', 'SF')
    assert result == b'F'
    result = setup_redis.getpos('NBA', 'PF')
    assert result == b'F'


def test_getpositions(setup_redis):
    result = list(setup_redis.getpositions('NBA'))
    assert result == [(b'PG', b'G'), (b'SG', b'G'), (b'G', b'G'), (b'SF', b'F'), (b'PF', b'F'), (b'F', b'F')]


def test_store(setup_redis):
    pos1 = Position(domain='NFL', name='LG', std_name='OG')
    pos2 = Position(domain='NFL', name='LT', std_name='OT')
    setup_redis.store("NFL", [pos1, pos2])

    result = setup_redis.getpos('NFL', 'LG')
    assert result == b'OG'

    result = setup_redis.getpos('NFL', 'LT')
    assert result == b'OT'

    result = list(setup_redis.getpositions('NFL'))
    assert result == [(b'LG', b'OG'), (b'OG', b'OG'), (b'OT', b'OT'), (b'LT', b'OT')]
