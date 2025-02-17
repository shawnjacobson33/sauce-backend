import json
from datetime import datetime

import pytest

from app.cache.main import RedisCache
from app.cache.models import Game, Subject, Market


@pytest.fixture
def setup_redis():
    redis = RedisCache(db='dev')

    redis.subjects.storesubjects("NBA", [
        Subject(domain='NBA', name='Lebron James', team='LAL', position='F', std_name='LeBron James'),
        Subject(domain='NBA', name='Ant Edwards', team='MIN', position='G', std_name='Anthony Edwards')
    ])

    redis.markets.storemarkets('Basketball', [
        Market(domain='Basketball', name='pts', std_name='Points')
    ])

    redis.games.storegames("NBA", [
        Game(domain='NBA', info=f'NBA_{datetime.now().strftime('%Y%m%d')}_MIN@LAL',
             game_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ])
    game_id = redis.games.getid("NBA", 'LAL')

    redis.client.hset('lines:info:PrizePicks', f'NBA:{game_id.decode('utf-8')}:s1:Points:Over:5.5', json.dumps(
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'dflt_odds': 1.5,
            'multiplier': 2.5
        }
    ))
    redis.client.hset('lines:info:PrizePicks', f'NBA:{game_id.decode('utf-8')}:s5:Points + Rebounds + Assists:Under:8.5', json.dumps(
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'dflt_odds': 2.5,
            'multiplier': 2.5
        }
    ))
    redis.client.hset('lines:info:DraftKings', f'NBA:{game_id.decode('utf-8')}:s2:Points:Under:10.5', json.dumps(
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'odds': 1.15,
        }
    ))

    yield redis
    redis.client.flushdb()


def test_getlines(setup_redis):
    result = list(setup_redis.lines.getlines('PrizePicks'))
    assert result == [
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'PrizePicks',
            'league': 'NBA',
            'game_id': 'g1',
            'subj_id': 's5',
            'market': 'Points + Rebounds + Assists',
            'label': 'Under',
            'line': '8.5',
            'dflt_odds': 2.5,
            'multiplier': 2.5
        },
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'PrizePicks',
            'league': 'NBA',
            'game_id': 'g1',
            'subj_id': 's1',
            'market': 'Points',
            'label': 'Over',
            'line': '5.5',
            'dflt_odds': 1.5,
            'multiplier': 2.5
        }
    ]

    result = list(setup_redis.lines.getlines('DraftKings'))
    assert result == [
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'DraftKings',
            'league': 'NBA',
            'game_id': 'g1',
            'subj_id': 's2',
            'market': 'Points',
            'label': 'Under',
            'line': '10.5',
            'odds': 1.15,
        }
    ]

    result = list(setup_redis.lines.getlines('PrizePicks', match='s5'))
    assert result == [
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'PrizePicks',
            'league': 'NBA',
            'game_id': 'g1',
            'subj_id': 's5',
            'market': 'Points + Rebounds + Assists',
            'label': 'Under',
            'line': '8.5',
            'dflt_odds': 2.5,
            'multiplier': 2.5
        }
    ]


def test_storelines(setup_redis):
    subj_id_1 = setup_redis.subjects.getid(Subject(domain='NBA', name='Lebron James', team='LAL'))
    subj_id_2 = setup_redis.subjects.getid(Subject(domain='NBA', name='Ant Edwards', position='G'))

    game_id = setup_redis.games.getid("NBA", 'LAL')

    setup_redis.lines.storelines([{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'BoomFantasy',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_1.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }, {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'BoomFantasy',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_2.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }, {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'ParlayPlay',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_2.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }])

    result = list(setup_redis.lines.getlines('BoomFantasy'))
    assert result == [{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'BoomFantasy',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_1.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }, {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'BoomFantasy',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_2.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }]

    result = list(setup_redis.lines.getlines("ParlayPlay"))
    assert result == [{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'ParlayPlay',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_2.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }]

    result = list(setup_redis.lines.getlines("BoomFantasy", match='s2'))
    assert result == [{
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'bookmaker': 'BoomFantasy',
        'league': 'NBA',
        'game_id': game_id.decode('utf-8'),
        'subj_id': subj_id_2.decode('utf-8'),
        'market': 'Points',
        'label': 'Under',
        'line': '10.5',
        'odds': 1.15,
    }]
