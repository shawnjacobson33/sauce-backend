import pytest

from app.data_storage.models import Bookmaker
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    redis.client.hset('bookmakers:std', 'PrizePicks', "1.73")
    redis.client.hset('bookmakers:std', 'UnderdogFantasy', "1.65")
    yield redis.bookmakers
    redis.client.flushdb()


def test_getbkm(setup_redis):
    result = setup_redis.getbkm('PrizePicks')
    assert result == b'1.73'
    result = setup_redis.getbkm('UnderdogFantasy')
    assert result == b'1.65'


def test_getbookmakers(setup_redis):
    result = list(setup_redis.getbookmakers())
    assert result == [(b'PrizePicks', b'1.73'), (b'UnderdogFantasy', b'1.65')]

def test_store(setup_redis):
    bookmakers = [Bookmaker(name='PrizePicks', dflt_odds='1.73'), Bookmaker(name='UnderdogFantasy', dflt_odds='1.65')]
    setup_redis.store(bookmakers)

    result = list(setup_redis.getbookmakers())
    assert result == [(b'PrizePicks', b'1.73'), (b'UnderdogFantasy', b'1.65')]

