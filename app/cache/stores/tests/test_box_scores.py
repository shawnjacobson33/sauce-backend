import json
from datetime import datetime

import pytest

from app.cache.main import Redis
from app.cache.models import Game, Subject


@pytest.fixture
def setup_redis():
    redis = Redis(db='dev')

    redis.subjects.storesubjects("NBA", [
        Subject(domain='NBA', name='Lebron James', team='LAL', position='F', std_name='LeBron James'),
        Subject(domain='NBA', name='Ant Edwards', team='MIN', position='G', std_name='Anthony Edwards')
    ])


    redis.games.storegames("NBA", [
        Game(domain='NBA', info=f'NBA_{datetime.now().strftime('%Y%m%d')}_MIN@LAL',
             game_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ])

    yield redis
    redis.client.flushdb()


def test_getboxscore(setup_redis):
    subj_id = setup_redis.subjects.getid(Subject(domain='NBA', name='Lebron James', team='LAL'))
    setup_redis.client.hset('box_scores:info:nba', f'b{subj_id}', json.dumps({
        'league': 'NBA',
        'subj_id': str(subj_id),
        'Points': 25,
        'Assists': 5,
        'Rebounds': 10
    }))

    result = setup_redis.box_scores.getboxscore('NBA', subj_id)
    assert result == {
        'league': 'NBA',
        'subj_id': str(subj_id),
        'Points': 25,
        'Assists': 5,
        'Rebounds': 10
    }

    result = setup_redis.box_scores.getboxscore('NBA', subj_id, stat='Points')
    assert result == 25


def test_storeboxscores(setup_redis):
    subj_id_1 = setup_redis.subjects.getid(Subject(domain='NBA', name='Lebron James', team='LAL'))
    subj_id_2 = setup_redis.subjects.getid(Subject(domain='NBA', name='Ant Edwards', position='G'))

    game_id = setup_redis.games.getid("NBA", 'LAL')
    setup_redis.box_scores.storeboxscores([{
        'league': 'NBA',
        'game_id': str(game_id),
        'subj_id': str(subj_id_1),
        'Points': 10,
        'Assists': 2,
        'Rebounds': 3,
        'is_completed': False
    }, {
        'league': 'NBA',
        'game_id': str(game_id),
        'subj_id': str(subj_id_2),
        'Points': 15,
        'Assists': 5,
        'Rebounds': 8,
        'is_completed': True
    }])

    result = setup_redis.box_scores.getboxscore("NBA", subj_id_1)
    assert result == {
        'league': 'NBA',
        'game_id': str(game_id),
        'subj_id': str(subj_id_1),
        'Points': 10,
        'Assists': 2,
        'Rebounds': 3,
        'is_completed': False
    }

    result = setup_redis.box_scores.getboxscore("NBA", subj_id_2)
    assert result == {
        'league': 'NBA',
        'game_id': str(game_id),
        'subj_id': str(subj_id_2),
        'Points': 15,
        'Assists': 5,
        'Rebounds': 8,
        'is_completed': True
    }

    result = setup_redis.box_scores.getboxscore("NBA", subj_id_2, stat='Points')
    assert result == 15