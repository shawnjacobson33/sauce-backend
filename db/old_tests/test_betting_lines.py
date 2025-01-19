from datetime import datetime

import pytest
import copy

from db import db


@pytest.fixture
def setup():
    betting_lines = [
        {
            '_id': 'ndoaiuyasd123',
            'timestamp': datetime.now().isoformat(),
            'bookmaker': 'PrizePicks',
            'league': 'NBA',
            'subject': 'LeBron James',
            'market': 'Points',
            'label': 'Under',
            'line': '8.5',
            'odds': 2.5,
            'impl_prb': 0.4
        },
        {
            '_id': 'aksd;oiji097',
            'timestamp': datetime.now().isoformat(),
            'bookmaker': 'DraftKings',
            'league': 'NBA',
            'subject': 'Anthony Edwards',
            'market': 'Points',
            'label': 'Under',
            'line': '10.5',
            'odds': 1.15,
            'impl_prb': 0.87
        },
        {
            '_id': 'lakshd780',
            'timestamp': datetime.now().isoformat(),
            'bookmaker': 'DraftKings',
            'league': 'NBA',
            'subject': 'Anthony Davis',
            'market': 'Rebounds',
            'label': 'Under',
            'line': '10.5',
            'odds': 1.15,
            'impl_prb': 0.87
        },
        {
            '_id': 'a09sdpu1h2p4',
            'timestamp': datetime.now().isoformat(),
            'bookmaker': 'BoomFantasy',
            'league': 'MLB',
            'subject': 'Juan Soto',
            'market': 'Hits',
            'label': 'Over',
            'line': '1.5',
            'odds': 1.5,
            'impl_prb': 0.67
        }
    ]

    yield betting_lines


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

    betting_lines_result = await db.betting_lines.get_betting_lines({})
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