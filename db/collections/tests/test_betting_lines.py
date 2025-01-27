from datetime import datetime, timedelta
import random

import pytest
import copy

from db import db


# {
#                                                         'batch_timestamp': self.batch_timestamp.strftime(
#                                                             '%Y-%m-%d %H:%M:%S'),
#                                                         'collection_timestamp': collection_datetime.strftime(
#                                                             '%Y-%m-%d %H:%M:%S'),
#                                                         # Todo: are you sure this is the format to use?
#                                                         'date': collection_datetime.strftime('%Y-%m-%d'),
#                                                         'bookmaker': bookmaker,
#                                                         'league': league,
#                                                         'game': game,
#                                                         'market_domain': market_domain,
#                                                         'market': market,
#                                                         'subject': subject,
#                                                         'label': label,
#                                                         'line': line,
#                                                         'odds': odds,
#                                                     }

@pytest.fixture
def setup():
    batch_timestamp = datetime.now()
    collection_datetime = datetime.now()
    date = datetime.now().strftime('%Y-%m-%d')

    bookmakers = ["BookmakerA", "BookmakerB", "BookmakerC", "BookmakerD", "BookmakerE", "BookmakerF", "BookmakerG"]
    leagues = ["NBA", "NCAAM"]
    games = {
        'NBA': [
            {
              "_id": 'NBA_20250127_SAC@BKN',
              "away_team": 'SAC',
              "home_team": 'BKN',
              "game_time": '2025-01-27 19:30:00'
            },
            {
                "_id": 'NBA_20250127_ATL@DAL',
                "away_team": 'ATL',
                "home_team": 'DAL',
                "game_time": '2025-01-27 19:30:00'
            },
            {
                "_id": 'NBA_20250127_MIN@BOS',
                "away_team": 'MIN',
                "home_team": 'BOS',
                "game_time": '2025-01-27 19:30:00'
            },
            {
                "_id": 'NBA_20250127_MIA@GS',
                "away_team": 'MIA',
                "home_team": 'GS',
                "game_time": '2025-01-27 19:30:00'
            },
        ],
        'NCAAM': [
            {
                "_id": 'NCAAM_20250127_SAC@BKN',
                "away_team": 'SAC',
                "home_team": 'BKN',
                "game_time": '2025-01-27 19:30:00'
            },
            {
                "_id": 'NCAAM_20250127_ATL@DAL',
                "away_team": 'ATL',
                "home_team": 'DAL',
                "game_time": '2025-01-27 19:30:00'
            },
            {
                "_id": 'NCAAM_20250127_MIN@BOS',
                "away_team": 'MIN',
                "home_team": 'BOS',
                "game_time": '2025-01-27 19:30:00'
            },
            {
                "_id": 'NCAAM_20250127_MIA@GS',
                "away_team": 'MIA',
                "home_team": 'GS',
                "game_time": '2025-01-27 19:30:00'
            },
        ],

    }
    market_domains = ["Gamelines", "PlayerProps"]
    markets = {
        'PlayerProps': ["Points", "Points + Assists", "1Q Points"],
        'Gamelines': ["Moneyline", "Spread", "Total", "1Q Moneyline"]
    }
    subjects = {
        'PlayerProps': ["Player1", "Player2", "Player3", "Player4", "Player5", "Player6", "Player7"],
        'Gamelines': ["Team1", "Team2", "Team3", "Team4", "Team5", "Team6", "Team7"]
    }
    lines = {
        'Moneyline': (0.5, 0.5, 1),
        'Total': (150, 225, 0.5),
        'Spread': (-9, 9, 0.5)
    }

    # create random sample of betting lines
    betting_lines_dicts = []
    betting_lines_docs = []
    for i in range(random.randint(10_000, 50_000)):
        betting_line_dict = {
            'batch_timestamp': batch_timestamp,
            'collection_timestamp': collection_datetime,
            'date': date,
            'bookmaker': random.choice(bookmakers),
            'league': random.choice(leagues),
            'label': random.choice(["Over", "Under"]),
            "market_domain": random.choice(market_domains),
            "metrics": { 'ev': random.uniform(-0.2, 0.2), 'tw_prb': random.uniform(0.2, 0.9), 'ev_formula': 'sully' },
            "extra_source_stats": { 'hold': random.uniform(0.2, 0.8), 'tw_prb': random.uniform(0.2, 0.9)},
            "odds": random.uniform(1, 10)
        }

        # add to the COMPLETE betting lines docs
        betting_line_dict['metrics']['impl_prb'] = 1 / betting_line_dict['odds']
        betting_line_dict['game'] = random.choice(games[betting_line_dict['league']])
        betting_line_dict['subject'] = random.choice(subjects[betting_line_dict["market_domain"]])
        betting_line_dict['market'] = random.choice(markets[betting_line_dict['market_domain']])
        betting_line_dict['line'] = random.randrange(*lines.get(betting_line_dict['market'], (3, 20, 0.5)))
        betting_lines_dicts.append(betting_line_dict)

        # add to the STREAM betting lines docs
        betting_line_doc = copy.deepcopy(betting_line_dict)
        betting_line_doc['stream'] = [{
            'batch_timestamp': betting_line_dict['batch_timestamp'],
            'collection_timestamp': betting_line_dict['collection_timestamp'],
            'line': betting_lines_dict['line'],
            'odds': odds,
        }]



    yield test_betting_lines_docs


def _update_timestamps(betting_lines) -> list:
    betting_lines_copy = copy.deepcopy(betting_lines)
    new_timestamp = datetime.now().isoformat()
    for betting_line in betting_lines_copy:
        betting_line['timestamp'] = new_timestamp

    return betting_lines_copy


def _update_odds_and_lines(betting_lines) -> list:
    for betting_line in betting_lines:
        betting_line['odds'] += 0.1
        betting_line['line'] = str(float(betting_line['line']) + 1)

    return betting_lines


@pytest.mark.asyncio
async def test_betting_lines(setup):
    betting_lines_test_collection = db.client['sauce-test']['betting_lines']
    # **************** TEST 1: Query betting lines ****************
    await betting_lines_test_collection.delete_many({})
    await betting_lines_test_collection.insert_many(setup)
    setup_set_tuples = setup

    betting_lines_result = await db.betting_lines({})
    assert betting_lines_result == setup_set_tuples

    betting_lines = await db.betting_lines.get_betting_lines({'bookmaker': 'PrizePicks'})
    assert betting_lines == [setup[0]]

    betting_lines = await db.betting_lines.get_betting_lines({'bookmaker': 'BoomFantasy'})
    assert betting_lines == [setup[3]]

    betting_lines = await db.betting_lines.get_betting_lines({'league': 'NBA'})
    assert betting_lines == [setup[0], setup[1], setup[2]]

    betting_lines = await db.betting_lines.get_betting_lines({'league': 'MLB'})
    assert betting_lines == [setup[3]]

    betting_lines = await db.betting_lines.get_betting_lines({'subject': 'LeBron James'})
    assert betting_lines == [setup[0]]

    betting_lines = await db.betting_lines.get_betting_lines({'subject': 'Anthony Edwards'})
    assert betting_lines == [setup[1]]

    betting_lines = await db.betting_lines.get_betting_lines({'market': 'Points'})
    assert betting_lines == [setup[0], setup[1]]

    betting_lines = await db.betting_lines.get_betting_lines({'market': 'Hits'})
    assert betting_lines == [setup[3]]

    betting_lines = await db.betting_lines.get_betting_lines({'league': 'NBA', 'market': 'Points'})
    assert betting_lines == [setup[0], setup[1]]

    betting_lines = await db.betting_lines.get_betting_lines({'league': 'NBA', 'market': 'Rebounds'})
    assert betting_lines == [setup[2]]

    betting_lines = await db.betting_lines.get_betting_lines({'bookmaker': 'DraftKings', 'market': 'Points'})
    assert betting_lines == [setup[1]]

    await betting_lines_test_collection.delete_many({})

    # **************** TEST 2: Store betting lines ****************
    await db.betting_lines.store_betting_lines(setup)

    setup_batch_1_docs = []
    for betting_line_dict in setup:
        betting_line_doc = db.betting_lines._create_doc(betting_line_dict)
        setup_batch_1_docs.append(betting_line_doc)

    stored_docs = await db.betting_lines.get_betting_lines({})
    assert stored_docs == setup_batch_1_docs

    # New Batch of Betting Lines where nothing has changed except timestamp
    setup_batch_2 = _update_timestamps(setup)
    await db.betting_lines.store_betting_lines(setup_batch_2)

    setup_batch_2_docs = []
    for i, betting_line_dict in enumerate(setup_batch_2):
        betting_line_doc = db.betting_lines._create_doc(betting_line_dict)
        for record in betting_line_doc['records']:
            record['timestamps'].insert(0, setup[i]['timestamp'])
        setup_batch_2_docs.append(betting_line_doc)

    stored_docs = await db.betting_lines.get_betting_lines({})
    assert stored_docs == setup_batch_2_docs

    # New Batch of Betting Lines where something has changed
    setup_batch_3 = _update_timestamps(setup_batch_2)
    setup_batch_3 = _update_odds_and_lines(setup_batch_3)
    await db.betting_lines.store_betting_lines(setup_batch_3)

    setup_batch_3_docs = []
    for i, betting_line_dict in enumerate(setup_batch_3):
        new_record = db.betting_lines._create_record(betting_line_dict)
        setup_batch_2_docs[i]['records'].append(new_record)
        setup_batch_3_docs.append(setup_batch_2_docs[i])

    stored_docs = await db.betting_lines.get_betting_lines({})
    assert stored_docs == setup_batch_3_docs

    await betting_lines_test_collection.delete_many({})