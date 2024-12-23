import pytest

from app.data_storage.models import Subject
from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    s_id = redis.subjects.id_mngr.generate()
    redis.client.hset('subjects:std:nba', 'F:LeBronJames', s_id)
    redis.client.hset('subjects:std:nba', 'LAL:LeBronJames', s_id)
    redis.client.hset('subjects:std:nba', 'F:LebronJames', s_id)
    redis.client.hset('subjects:std:nba', 'LAL:LebronJames', s_id)
    s_id = redis.subjects.id_mngr.generate()
    redis.client.hset('subjects:std:nba', 'G:AnthonyEdwards', s_id)
    redis.client.hset('subjects:std:nba', 'MIN:AnthonyEdwards', s_id)
    redis.client.hset('subjects:std:nba', 'G:AntEdwards', s_id)
    redis.client.hset('subjects:std:nba', 'MIN:AntEdwards', s_id)

    redis.client.hset('s1', mapping={'name': 'LeBron James', 'team': 'Los Angeles Lakers'})
    redis.client.hset('s2', mapping={'name': 'Anthony Edwards', 'team': 'Minnesota Timberwolves'})
    yield redis.subjects
    redis.client.flushdb()


def test_getsubj(setup_redis):
    subj1 = Subject(domain='NBA', name='Lebron James', std_name='LeBron James', position='F', team='LAL')
    subj2 = Subject(domain='NBA', name='Ant Edwards', std_name='Anthony Edwards', position='G', team='MIN')
    result = setup_redis.getsubj(subj1)
    assert result == {b'name': b'LeBron James', b'team': b'Los Angeles Lakers'}
    result = setup_redis.getsubj(subj2)
    assert result == {b'name': b'Anthony Edwards', b'team': b'Minnesota Timberwolves'}


def test_getsubjs(setup_redis):
    result = list(setup_redis.getsubjs('NBA'))
    assert result == [{b'name': b'LeBron James', b'team': b'Los Angeles Lakers'},
                      {b'name': b'Anthony Edwards', b'team': b'Minnesota Timberwolves'}]


def test_store(setup_redis):
    subj1 = Subject(domain='NBA', name='Lebron James', std_name='LeBron James', position='F', team='LAL')
    subj2 = Subject(domain='NBA', name='Ant Edwards', std_name='Anthony Edwards', position='G', team='MIN')
    setup_redis.store("NBA", [subj1, subj2])

    result = setup_redis.getid(subj1)
    assert result == b's1'
    result = setup_redis.getid(subj2)
    assert result == b's2'

    result = setup_redis.getsubj(subj1)
    assert result == {b'name': b'LeBron James', b'team': b'LAL'}
    result = setup_redis.getsubj(subj2)
    assert result == {b'name': b'Anthony Edwards', b'team': b'MIN'}