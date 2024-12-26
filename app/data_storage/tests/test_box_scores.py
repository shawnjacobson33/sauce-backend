import json

import pytest

from app.data_storage.main import Redis


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')
    subj_id = redis.subjects.id_mngr.generate()
    redis.client.hset('subjects:lookup:nba', 'F:LeBronJames', subj_id)
    redis.client.hset('subjects:lookup:nba', 'LAL:LeBronJames', subj_id)
    redis.client.hset('subjects:lookup:nba', 'F:LebronJames', subj_id)
    redis.client.hset('subjects:lookup:nba', 'LAL:LebronJames', subj_id)

    subject_json = json.dumps({'name': 'LeBron James', 'team': 'LAL'})
    redis.client.hset('subjects:nba', subj_id, subject_json)

    box_score_json = json.dumps({
        'league': 'NBA',
        'subj_id': 's1',
        'Points': 25,
        'Assists': 5,
        'Rebounds': 10
    })
    redis.client.hset('box_scores:nba', f'b{subj_id}', box_score_json)

    subj_id = 's2'
    redis.client.hset('subjects:lookup:nba', 'G:AnthonyEdwards', subj_id)
    redis.client.hset('subjects:lookup:nba', 'MIN:AnthonyEdwards', subj_id)
    redis.client.hset('subjects:lookup:nba', 'G:AntEdwards', subj_id)
    redis.client.hset('subjects:lookup:nba', 'MIN:AntEdwards', subj_id)

    subject_json = json.dumps({'name': 'Anthony Edwards', 'team': 'MIN'})
    redis.client.hset('subjects:nba', subj_id, subject_json)

    yield redis.box_scores
    redis.client.flushdb()


def test_getboxscore(setup_redis):
    result = setup_redis.getboxscore('NBA', 's1')
    assert result == {
        'league': 'NBA',
        'subj_id': 's1',
        'Points': 25,
        'Assists': 5,
        'Rebounds': 10
    }
    result = setup_redis.getboxscore('NBA', 's1', stat='Points')
    assert result == 25


def test_storeboxscores(setup_redis):
    setup_redis.storeboxscores([{
        'league': 'NBA',
        'subj_id': 's2',
        'Points': 10,
        'Assists': 2,
        'Rebounds': 3,
        'is_completed': False
    }])

    result = setup_redis.getboxscore("NBA", 's2')
    assert result == {
        'league': 'NBA',
        'subj_id': 's2',
        'Points': 10,
        'Assists': 2,
        'Rebounds': 3,
        'is_completed': False
    }
    result = setup_redis.getboxscore("NBA", 's2', stat='Points')
    assert result == 10