import uuid
from datetime import datetime, timedelta

import random
import copy

# data to be used in the tests
CONFIGS = {
    'batch_timestamp': datetime(2025, 1, 28, 20, 30, 0),
    'collection_timestamp': datetime(2025, 1, 28, 20, 30, 0),
    'extra_source_stats': { "hold": 0.014, "tw_prb": 0.622 },
    'bookmaker': 'STNSports',
    'home_team': 'PHI',
    'away_team': 'LAL',
    'game_time': '2025-01-28 19:30:00',
    'label': 'Over',
    'league': 'NBA',
    'market': 'Moneyline',
    'market_domain': 'Gamelines',
    'subject': 'Los Angeles Lakers',
    'impl_prb': 0.667,
    'tw_prb': 0.629,
    'ev': -0.057,
    'ev_formula': 'sully',
    'odds': 1.5,
    'line': 0.5
}

# the structure of the dictionary constructed during collection time
MOCK_COLLECTED_BETTING_LINE = {
    "_id": f'{CONFIGS["bookmaker"]}:{CONFIGS["league"]}:{CONFIGS["market"]}:{CONFIGS["subject"]}:{CONFIGS["label"]}',
    'batch_timestamp': CONFIGS['batch_timestamp'],
    'collection_timestamp': CONFIGS['collection_timestamp'],
    'bookmaker': CONFIGS['bookmaker'],
    'game': {
        '_id': f'{CONFIGS["league"]}_{CONFIGS["batch_timestamp"].strftime("%Y%m%d")}_{CONFIGS["away_team"]}@{CONFIGS["home_team"]}',
        'away_team': CONFIGS['away_team'],
        'home_team': CONFIGS['home_team'],
        'game_time': CONFIGS['game_time']
    },
    'label': CONFIGS['label'],
    'league': CONFIGS['league'],
    'market': CONFIGS['market'],
    'market_domain': CONFIGS['market_domain'],
    'subject': CONFIGS['subject'],
    'metrics': {
        'impl_prb': CONFIGS['impl_prb'],
        'tw_prb': CONFIGS['tw_prb'],
        'ev': CONFIGS['ev'],
        'ev_formula': CONFIGS['ev_formula']
    },
    'odds': CONFIGS['odds'],
    'line': CONFIGS['line']
}

# the document constructed during storage time
MOCK_STORED_BETTING_LINE = {
    **{k: v for k, v in MOCK_COLLECTED_BETTING_LINE.items() if k != 'stream'},
    'stream': [ {k: v for k, v in MOCK_COLLECTED_BETTING_LINE.items()
                 if k in {'batch_timestamp', 'collection_timestamp', 'odds', 'line'}} ]
}


def wrong_field_type():
    betting_line = copy.deepcopy(MOCK_COLLECTED_BETTING_LINE)
    betting_line[random.choice(['line', 'odds'])] = '1.5'
    return betting_line

def update_odds(collected: bool = False):
    if collected:
        betting_line = copy.deepcopy(MOCK_COLLECTED_BETTING_LINE)
        betting_line['odds'] = 1.6
        betting_line['batch_timestamp'] = CONFIGS['batch_timestamp'] + timedelta(minutes=1)
        betting_line['collection_timestamp'] = CONFIGS['collection_timestamp'] + timedelta(minutes=1)
        return betting_line

    betting_line = copy.deepcopy(MOCK_STORED_BETTING_LINE)
    betting_line['stream'].append({
        'batch_timestamp': CONFIGS['batch_timestamp'] + timedelta(minutes=1),
        'collection_timestamp': CONFIGS['collection_timestamp'] + timedelta(minutes=1),
        'odds': 1.6,
        'line': CONFIGS['line']
    })
    return betting_line

def update_metrics(delete: bool = False):
    if delete:
        return {k: v for k, v in MOCK_COLLECTED_BETTING_LINE.items() if k != 'metrics'}

    betting_line = copy.deepcopy(MOCK_COLLECTED_BETTING_LINE)
    betting_line['metrics'] = {
        'impl_prb': 0.666,
        'tw_prb': 0.630,
        'ev': -0.058,
        'ev_formula': 'sully'
    }
    betting_line['batch_timestamp'] = CONFIGS['batch_timestamp'] + timedelta(minutes=1)
    betting_line['collection_timestamp'] = CONFIGS['collection_timestamp'] + timedelta(minutes=1)
    return betting_line

def update_extra_source_stats(delete: bool = False):
    if delete:
        return {k: v for k, v in MOCK_COLLECTED_BETTING_LINE.items() if k != 'extra_source_stats'}

    betting_line = copy.deepcopy(MOCK_COLLECTED_BETTING_LINE)
    betting_line['extra_source_stats'] = {
        "hold": 0.015,
        "tw_prb": 0.623
    }
    betting_line['batch_timestamp'] = CONFIGS['batch_timestamp'] + timedelta(minutes=1)
    betting_line['collection_timestamp'] = CONFIGS['collection_timestamp'] + timedelta(minutes=1)
    return betting_line

def new_id(return_id: bool = False):
    betting_line = copy.deepcopy(MOCK_COLLECTED_BETTING_LINE)
    betting_line['_id'] = betting_line['_id'] + str(uuid.uuid4())
    return betting_line if not return_id else betting_line['_id']


COLLECTED_BETTING_LINE_CASES = {
    'base': [MOCK_COLLECTED_BETTING_LINE],
    'missing_critical_field': [{
        k: v for k, v in MOCK_COLLECTED_BETTING_LINE.items()
        if k != random.choice([
            'label', 'market_domain', 'subject', 'bookmaker', 'game', 'league', 'market', 'odds', 'line'
        ])
    }],
    'wrong_field_type': [wrong_field_type()],
    'odds_moved': [update_odds(collected=True)],
    'metrics_updated': [update_metrics()],
    'extra_source_stats_updated': [update_extra_source_stats()],
    'no_metrics': [update_metrics(delete=True)],
    'no_extra_source_stats': [update_extra_source_stats(delete=True)],
    'new_id': [new_id()],
}

STORED_BETTING_LINE_CASES = {
    'base': MOCK_STORED_BETTING_LINE,
    'filter_by_field': [
        MOCK_STORED_BETTING_LINE,
        {**{k: v for k, v in MOCK_STORED_BETTING_LINE.items() if k != 'league'}, 'league': 'NCAAM'}
    ],
    'odds_moved': [update_odds(collected=False)],
}

UPDATE_BETTING_LINE_CASES = {
    'base': { '_id': new_id(return_id=True) }
}
