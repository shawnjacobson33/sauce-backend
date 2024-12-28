import json

import pytest

from app.data_storage.models import Subject
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')

    lookup_name = 'subjects:lookup:nba'
    subj_id_1 = redis.subjects.id_mngr.generate()
    redis.client.hset(lookup_name, 'F:LeBronJames', subj_id_1)
    redis.client.hset(lookup_name, 'LAL:LeBronJames', subj_id_1)
    redis.client.hset(lookup_name, 'F:LebronJames', subj_id_1)
    redis.client.hset(lookup_name, 'LAL:LebronJames', subj_id_1)

    subj_id_2 = redis.subjects.id_mngr.generate()
    redis.client.hset(lookup_name, 'G:AnthonyEdwards', subj_id_2)
    redis.client.hset(lookup_name, 'MIN:AnthonyEdwards', subj_id_2)
    redis.client.hset(lookup_name, 'G:AntEdwards', subj_id_2)
    redis.client.hset(lookup_name, 'MIN:AntEdwards', subj_id_2)

    info_name = 'teams:info:nba'
    redis.client.hset(info_name, subj_id_1, json.dumps({'name': 'LeBron James', 'team': 'Los Angeles Lakers'}))
    redis.client.hset(info_name, subj_id_2, json.dumps({'name': 'Anthony Edwards', 'team': 'Minnesota Timberwolves'}))

    yield redis.subjects
    redis.client.flushdb()


def test_getsubj(setup_redis):
    subj1 = Subject(domain='NBA', name='Lebron James', std_name='LeBron James', position='F', team='LAL')
    subj2 = Subject(domain='NBA', name='Ant Edwards', std_name='Anthony Edwards', position='G', team='MIN')

    result = setup_redis.getsubj("NBA", subj1)
    assert result == {'name': 'LeBron James', 'team': 'Los Angeles Lakers'}
    result = setup_redis.getsubj("NBA", subj2)
    assert result == {'name': 'Anthony Edwards', 'team': 'Minnesota Timberwolves'}


def test_getsubjs(setup_redis):
    result = list(setup_redis.getsubjs('NBA'))
    assert result == [{'name': 'LeBron James', 'team': 'Los Angeles Lakers'},
                      {'name': 'Anthony Edwards', 'team': 'Minnesota Timberwolves'}]


def test_storesubjs(setup_redis):
    subj1 = Subject(domain='NBA', name='Lebron James', std_name='LeBron James', position='F', team='LAL')
    subj2 = Subject(domain='NBA', name='Ant Edwards', std_name='Anthony Edwards', position='G', team='MIN')

    setup_redis.store("NBA", [subj1, subj2])

    result = setup_redis.getid("NBA", subj1)
    assert result == b's1'
    result = setup_redis.getid("NBA", subj2)
    assert result == b's2'

    result = setup_redis.getsubj("NBA", subj1)
    assert result == {'name': 'LeBron James', 'team': 'LAL'}
    result = setup_redis.getsubj("NBA", subj2)
    assert result == {'name': 'Anthony Edwards', 'team': 'MIN'}