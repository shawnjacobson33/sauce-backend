from db import test_db as db
from db.collections.t.mock_data import (
    COLLECTED_BETTING_LINE_CASES, STORED_BETTING_LINE_CASES, UPDATE_BETTING_LINE_CASES
)


async def t_storing_betting_lines_1():
    """~~~~~~~~~~ CASE 1: Base ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert stored_betting_line_docs == [STORED_BETTING_LINE_CASES['base']]
        print('Test StoreBettingLines Case 1: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 1: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_2():
    """~~~~~~~~~~ CASE 2: Missing Critical Field ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['missing_critical_field'])
        print('Test StoreBettingLines Case 2: ❌')

    except Exception:
        print('Test StoreBettingLines Case 2: ✅')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_3():
    """~~~~~~~~~~ CASE 3: Wrong Field Type ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['wrong_field_type'])
        print('Test StoreBettingLines Case 3: ❌')

    except Exception:
        print('Test StoreBettingLines Case 3: ✅')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_4():
    """~~~~~~~~~~ CASE 4: Updated Odds ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['odds_moved'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            (len(stored_betting_line_docs[0]['stream']) == 2) and
            (stored_betting_line_docs[0]['stream'][1]['odds'] == COLLECTED_BETTING_LINE_CASES['updated_odds'][0]['odds']) and
            (stored_betting_line_docs[0]['stream'][1]['line'] == COLLECTED_BETTING_LINE_CASES['base'][0]['line']) and
            (stored_betting_line_docs[0]['stream'][1]['batch_timestamp'] == COLLECTED_BETTING_LINE_CASES['updated_odds'][0]['batch_timestamp']) and
            (stored_betting_line_docs[0]['stream'][1]['collection_timestamp'] == COLLECTED_BETTING_LINE_CASES['updated_odds'][0]['collection_timestamp'])
        )
        print('Test StoreBettingLines Case 4: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 4: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_5():
    """~~~~~~~~~~ CASE 5: No Updates to Betting Line Odds or Line ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            (len(stored_betting_line_docs[0]['stream']) == 1)
        )
        print('Test StoreBettingLines Case 5: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 5: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_6():
    """~~~~~~~~~~ CASE 6: Metrics Existed Before And Then Changed ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['metrics_updated'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['metrics'] == COLLECTED_BETTING_LINE_CASES['metrics_updated'][0]['metrics'])
        )
        print('Test StoreBettingLines Case 6: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 6: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_7():
    """~~~~~~~~~~ CASE 7: Extra Source Stats Existed Before And Then Changed ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['extra_source_stats_updated'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['extra_source_stats'] == COLLECTED_BETTING_LINE_CASES['extra_source_stats_updated'][0]['extra_source_stats'])
        )
        print('Test StoreBettingLines Case 7: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 7: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_8():
    """~~~~~~~~~~ CASE 8: Metrics Non-Existent -> Existing ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_metrics'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['metrics'] == COLLECTED_BETTING_LINE_CASES['base'][0]['metrics'])
        )
        print('Test StoreBettingLines Case 8: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 8: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_9():
    """~~~~~~~~~~ CASE 9: Extra Source Stats Non-Existent -> Existing ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_extra_source_stats'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['extra_source_stats'] == COLLECTED_BETTING_LINE_CASES['base'][0]['extra_source_stats'])
        )
        print('Test StoreBettingLines Case 9: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 9: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_10():
    """~~~~~~~~~~ CASE 10: Metrics Existed Before And Then Deleted ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_metrics'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            ('metrics' not in stored_betting_line_docs[0])
        )
        print('Test StoreBettingLines Case 10: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 10: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_11():
    """~~~~~~~~~~ CASE 11: Extra Source Stats Existed Before And Then Deleted ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_extra_source_stats'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 1) and
            ('extra_source_stats' not in stored_betting_line_docs[0])
        )
        print('Test StoreBettingLines Case 11: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 11: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_12():
    """~~~~~~~~~~ CASE 12: Stored Betting Line Was Not Collected ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['new_id'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 2) and
            ({k: v for k, v in stored_betting_line_docs[0].items() if k != 'stream'} == STORED_BETTING_LINE_CASES['base']) and
            (len(stored_betting_line_docs[0]['stream']) == 2) and
            (stored_betting_line_docs[0]['stream'][1].keys() == {'batch_timestamp'}) and
            (stored_betting_line_docs[0]['stream'][1]['batch_timestamp'] == COLLECTED_BETTING_LINE_CASES['base'][0]['batch_timestamp'])
        )
        print('Test StoreBettingLines Case 12: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 12: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_storing_betting_lines_13():
    """~~~~~~~~~~ CASE 13: Stored Betting Line Was Not Collected (for > 1 Batch) ~~~~~~~~~~~"""
    try:
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['new_id'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['new_id'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        assert (
            (len(stored_betting_line_docs) == 3) and
            ({k: v for k, v in stored_betting_line_docs[0].items() if k != 'stream'} == STORED_BETTING_LINE_CASES['base']) and
            (len(stored_betting_line_docs[0]['stream']) == 2) and
            (stored_betting_line_docs[0]['stream'][1].keys() == {'batch_timestamp'}) and
            (stored_betting_line_docs[0]['stream'][1]['batch_timestamp'] == COLLECTED_BETTING_LINE_CASES['base'][0]['batch_timestamp'])
        )
        print('Test StoreBettingLines Case 13: ✅')

    except AssertionError:
        print('Test StoreBettingLines Case 13: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_get_betting_lines_1():
    """~~~~~~~~~~ CASE 1: base ~~~~~~~~~~~"""
    try:
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'][0])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines({})
        assert stored_betting_line_docs == [STORED_BETTING_LINE_CASES['base']]
        print('Test GetBettingLines Case 1: ✅')

    except AssertionError:
        print('Test GetBettingLines Case 1: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_get_betting_lines_2():
    """~~~~~~~~~~ CASE 2: Filter by Field ~~~~~~~~~~~"""
    try:
        await db.betting_lines.collection.insert_many(COLLECTED_BETTING_LINE_CASES['filter_by_field'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines({'league': 'NBA'})
        assert stored_betting_line_docs == [STORED_BETTING_LINE_CASES['base']]
        print('Test GetBettingLines Case 2: ✅')

    except AssertionError:
        print('Test GetBettingLines Case 2: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_get_betting_lines_3():
    """~~~~~~~~~~ CASE 3: Previous Batch (One Record) ~~~~~~~~~~~"""
    try:
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines(prev_batch=True)
        assert stored_betting_line_docs == COLLECTED_BETTING_LINE_CASES['base']
        print('Test GetBettingLines Case 3: ✅')

    except AssertionError:
        print('Test GetBettingLines Case 3: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_get_betting_lines_4():
    """~~~~~~~~~~ CASE 4: Previous Batch (Multiple Records) ~~~~~~~~~~~"""
    try:
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'])
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['odds_moved'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines(prev_batch=True)
        assert stored_betting_line_docs == COLLECTED_BETTING_LINE_CASES['odds_moved']
        print('Test GetBettingLines Case 4: ✅')

    except AssertionError:
        print('Test GetBettingLines Case 4: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def t_update_betting_line_1():
    """~~~~~~~~~~ CASE 1: Base ~~~~~~~~~~~"""
    try:
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'])
        await db.betting_lines.update_betting_line(COLLECTED_BETTING_LINE_CASES['base']['_id'], UPDATE_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines({})
        assert stored_betting_line_docs == [STORED_BETTING_LINE_CASES['base']]
        print('Test UpdateBettingLine Case 1: ✅')

    except AssertionError:
        print('Test UpdateBettingLine Case 1: ❌')

    finally:
        await db.betting_lines.collection.delete_many({})


async def main():
    await t_storing_betting_lines_1()
    await t_storing_betting_lines_2()
    await t_storing_betting_lines_3()
    await t_storing_betting_lines_4()
    await t_storing_betting_lines_5()
    await t_storing_betting_lines_6()
    await t_storing_betting_lines_7()
    await t_storing_betting_lines_8()
    await t_storing_betting_lines_9()
    await t_storing_betting_lines_10()
    await t_storing_betting_lines_11()
    await t_storing_betting_lines_12()
    await t_storing_betting_lines_13()
    await t_get_betting_lines_1()
    await t_get_betting_lines_2()
    await t_get_betting_lines_3()
    await t_get_betting_lines_4()
    await t_update_betting_line_1()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
