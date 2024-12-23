import pytest

from app.data_storage.models import Team
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    redis.client.hset('teams:std:nba', 'PHO', 't1')
    redis.client.hset('teams:std:nba', 'PHX', 't1')
    redis.client.hset('teams:std:nba', 'LAL', 't2')
    redis.client.hset('teams:std:nba', 'SAS', 't3')
    redis.client.hset('teams:std:nba', 'SA', 't3')

    redis.client.hset('t1', mapping={'abbr': 'PHO', 'full': 'Phoenix Suns'})
    redis.client.hset('t2', mapping={'abbr': 'LAL', 'full': 'Los Angeles Lakers'})
    redis.client.hset('t3', mapping={'abbr': 'SAS', 'full': 'San Antonio Spurs'})
    yield redis.teams
    redis.client.flushdb()


def test_getteam(setup_redis):
    result = setup_redis.getteam('NBA', 'PHO')
    assert result == {b'abbr': b'PHO', b'full': b'Phoenix Suns'}
    result = setup_redis.getteam('NBA', 'SA')
    assert result == {b'abbr': b'SAS', b'full': b'San Antonio Spurs'}
    result = setup_redis.getteam('NBA', 'LAL')
    assert result == {b'abbr': b'LAL', b'full': b'Los Angeles Lakers'}
    result = setup_redis.getteam('NBA', 'PHX')
    assert result == {b'abbr': b'PHO', b'full': b'Phoenix Suns'}
    result = setup_redis.getteam('NBA', 'SAS')
    assert result == {b'abbr': b'SAS', b'full': b'San Antonio Spurs'}


def test_getteams(setup_redis):
    result = list(setup_redis.getteams('NBA'))
    assert result == [{b'abbr': b'PHO', b'full': b'Phoenix Suns'},
                      {b'abbr': b'LAL', b'full': b'Los Angeles Lakers'},
                      {b'abbr': b'SAS', b'full': b'San Antonio Spurs'}]


def test_store(setup_redis):
    team1 = Team(domain='NBA', name='PHX', std_name='PHO', full_name='Phoenix Suns')
    team2 = Team(domain='NBA', name='SA', std_name='SAS', full_name='San Antonio Spurs')
    setup_redis.store("NBA", [team1, team2])

    result = setup_redis.getteam('NBA', 'PHX')
    assert result == {b'abbr': b'PHO', b'full': b'Phoenix Suns'}
    result = setup_redis.getteam('NBA', 'SA')
    assert result == {b'abbr': b'SAS', b'full': b'San Antonio Spurs'}
    result = list(setup_redis.getteams('NBA'))
    assert result == [{b'abbr': b'PHO', b'full': b'Phoenix Suns'}, {b'abbr': b'SAS', b'full': b'San Antonio Spurs'}]