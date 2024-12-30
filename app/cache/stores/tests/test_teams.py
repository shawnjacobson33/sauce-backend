import json

import pytest

from app.cache.models import Team
from app.cache.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')

    lookup_name = 'teams:lookup:nba'
    team_id_1 = redis.teams.id_mngr.generate()
    redis.client.hset(lookup_name, 'PHO', team_id_1)
    redis.client.hset(lookup_name, 'PHX', team_id_1)
    team_id_2 = redis.teams.id_mngr.generate()
    redis.client.hset(lookup_name, 'LAL', team_id_2)
    team_id_3 = redis.teams.id_mngr.generate()
    redis.client.hset(lookup_name, 'SAS', team_id_3)
    redis.client.hset(lookup_name, 'SA', team_id_3)

    info_name = 'teams:info:nba'
    redis.client.hset(info_name, team_id_1, json.dumps({'abbr': 'PHO', 'full': 'Phoenix Suns'}))
    redis.client.hset(info_name, team_id_2, json.dumps({'abbr': 'LAL', 'full': 'Los Angeles Lakers'}))
    redis.client.hset(info_name, team_id_3, json.dumps({'abbr': 'SAS', 'full': 'San Antonio Spurs'}))

    yield redis.teams
    redis.client.flushdb()


def test_getteam(setup_redis):
    result = setup_redis.getteam('NBA', 'PHO')
    assert result == {'abbr': 'PHO', 'full': 'Phoenix Suns'}
    result = setup_redis.getteam('NBA', 'SA')
    assert result == {'abbr': 'SAS', 'full': 'San Antonio Spurs'}
    result = setup_redis.getteam('NBA', 'PHX')
    assert result == {'abbr': 'PHO', 'full': 'Phoenix Suns'}
    result = setup_redis.getteam('NBA', 'SAS')
    assert result == {'abbr': 'SAS', 'full': 'San Antonio Spurs'}


def test_getteams(setup_redis):
    result = list(setup_redis.getteams('NBA'))
    result = [json.loads(team) for team in result]
    assert result == [{'abbr': 'PHO', 'full': 'Phoenix Suns'},
                      {'abbr': 'LAL', 'full': 'Los Angeles Lakers'},
                      {'abbr': 'SAS', 'full': 'San Antonio Spurs'}]

def test_storeteams(setup_redis):
    team1 = Team(domain='NBA', name='MN', std_name='MIN', full_name='Minnesota Timberwolves')
    team2 = Team(domain='NBA', name='MIN', std_name='MIN', full_name='Minnesota Timberwolves')
    setup_redis.storeteams("NBA", [team1, team2])

    result = setup_redis.getid(team1.domain, team1.name)
    assert result == b't4'
    result = setup_redis.getid(team2.domain, team2.name)
    assert result == b't4'

    result = setup_redis.getteam('NBA', 'MN')
    assert result == {'abbr': 'MIN', 'full': 'Minnesota Timberwolves'}
    result = setup_redis.getteam('NBA', 'MIN')
    assert result == {'abbr': 'MIN', 'full': 'Minnesota Timberwolves'}