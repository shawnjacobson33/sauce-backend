import pytest

from app.cache.models import Bookmaker
from app.cache.main import RedisCache


@pytest.fixture
def setup_redis():
    redis = RedisCache(db='dev')
    info_name = 'bookmakers:info'
    redis.client.hset(info_name, 'PrizePicks', "1.73")
    redis.client.hset(info_name, 'UnderdogFantasy', "1.65")
    yield redis.bookmakers
    redis.client.flushdb()


def test_getbkm(setup_redis):
    result = setup_redis.getbookmaker('PrizePicks')
    assert result == b'1.73'
    result = setup_redis.getbookmaker('UnderdogFantasy')
    assert result == b'1.65'


def test_getbookmakers(setup_redis):
    result = set(setup_redis.getbookmakers())
    assert result == {(b'PrizePicks', b'1.73'), (b'UnderdogFantasy', b'1.65')}

def test_store(setup_redis):
    bookmakers = [Bookmaker(name='PrizePicks', dflt_odds='1.73'), Bookmaker(name='UnderdogFantasy', dflt_odds='1.65')]
    setup_redis.storebookmakers(bookmakers)

    result = set(setup_redis.getbookmakers())
    assert result == {(b'PrizePicks', b'1.73'), (b'UnderdogFantasy', b'1.65')}

