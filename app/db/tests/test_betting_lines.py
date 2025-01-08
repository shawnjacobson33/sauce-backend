import pytest

from app.db import db

betting_lines_test_collection = db.client['sauce-test']['betting_lines']


@pytest.fixture
def setup():
    betting_lines = [
        {
            '_id': 'ndoaiuyasd123',
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


@pytest.mark.asyncio
async def test_query_betting_lines(setup):
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


@pytest.mark.asyncio
async def test_store_betting_lines(setup):
    await betting_lines_test_collection.delete_many({})
    await db.betting_lines.store_betting_lines(setup)

    betting_lines = await betting_lines_test_collection.find({}).to_list(length=4)
    assert betting_lines == setup

    await betting_lines_test_collection.delete_many({})
