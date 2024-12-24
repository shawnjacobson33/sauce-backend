import pytest

from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    s_id = redis.subjects.id_mngr.generate()
    redis.client.hset('subjects:std:nba', 'F:LeBronJames', s_id)
    redis.client.hset('subjects:std:nba', 'LAL:LeBronJames', s_id)
    redis.client.hset('subjects:std:nba', 'F:LebronJames', s_id)
    redis.client.hset('subjects:std:nba', 'LAL:LebronJames', s_id)

    redis.client.hset('s1', mapping={'name': 'LeBron James', 'team': 'Los Angeles Lakers'})

    redis.client.hset('bs1', mapping={
        'league': 'NBA',
        's_id': 's1',
        'Points': 25,
        'Assists': 5,
        'Rebounds': 10
    })

    yield redis.box_scores
    redis.client.flushdb()


def test_getboxscore(setup_redis):
    result = setup_redis.getboxscore('s1')
    assert result == {
        b'league': b'NBA',
        b's_id': b's1',
        b'Points': b'25',
        b'Assists': b'5',
        b'Rebounds': b'10'
    }
    result = setup_redis.getboxscore('s1', stat='Points')
    assert result == b'25'


def test_store(setup_redis):
    s_id = 's2'
    setup_redis._r.hset('subjects:std:nba', 'G:AnthonyEdwards', s_id)
    setup_redis._r.hset('subjects:std:nba', 'MIN:AnthonyEdwards', s_id)
    setup_redis._r.hset('subjects:std:nba', 'G:AntEdwards', s_id)
    setup_redis._r.hset('subjects:std:nba', 'MIN:AntEdwards', s_id)
    setup_redis._r.hset(s_id, mapping={'name': 'Anthony Edwards', 'team': 'Minnesota Timberwolves'})

    setup_redis.store([{
        'league': 'NBA',
        's_id': 's2',
        'Points': 10,
        'Assists': 2,
        'Rebounds': 3
    }])

    result = setup_redis.getboxscore('s2')
    assert result == {
        b'league': b'NBA',
        b's_id': b's2',
        b'Points': b'10',
        b'Assists': b'2',
        b'Rebounds': b'3'
    }
    result = setup_redis.getboxscore('s2', stat='Points')
    assert result == b'10'