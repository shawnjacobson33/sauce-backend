import unittest

from db import Database
from db.collections.tests.mock_data import (
    COLLECTED_BETTING_LINE_CASES, STORED_BETTING_LINE_CASES, UPDATE_BETTING_LINE_CASES
)


db = Database('mongodb://localhost:27017/', 'test')


class TestStoringBettingLines(unittest.IsolatedAsyncioTestCase):

    async def setUp(self):
        await db.betting_lines.delete_betting_lines({})

    async def test_storing_betting_lines_1(self):
         """~~~~~~~~~~ CASE 1: Base ~~~~~~~~~~~"""
         await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])

         stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
         self.assertEqual(stored_betting_line_docs, [STORED_BETTING_LINE_CASES['base']])

    async def test_storing_betting_lines_2(self):
        """~~~~~~~~~~ CASE 2: Missing Critical Field ~~~~~~~~~~~"""
        with self.assertRaises(Exception) as cm:
            await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['missing_critical_field'])

    async def test_storing_betting_lines_3(self):
        """~~~~~~~~~~ CASE 3: Wrong Field Type ~~~~~~~~~~~"""
        with self.assertRaises(Exception) as cm:
            await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['wrong_field_type'])

    async def test_storing_betting_lines_4(self):
        """~~~~~~~~~~ CASE 4: Updated Odds ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['updated_odds'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            (len(stored_betting_line_docs[0]['stream']) == 2) and
            (stored_betting_line_docs[0]['stream'][1]['odds'] == COLLECTED_BETTING_LINE_CASES['updated_odds'][0]['odds']) and
            (stored_betting_line_docs[0]['stream'][1]['line'] == COLLECTED_BETTING_LINE_CASES['base'][0]['line']) and
            (stored_betting_line_docs[0]['stream'][1]['batch_timestamp'] == COLLECTED_BETTING_LINE_CASES['updated_odds'][0]['batch_timestamp']) and
            (stored_betting_line_docs[0]['stream'][1]['collection_timestamp'] == COLLECTED_BETTING_LINE_CASES['updated_odds'][0]['collection_timestamp'])
        )

    async def test_storing_betting_lines_5(self):
        """~~~~~~~~~~ CASE 5: No Updates to Betting Line Odds or Line ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            (len(stored_betting_line_docs[0]['stream']) == 1)
        )

    async def test_storing_betting_lines_6(self):
        """~~~~~~~~~~ CASE 6: Metrics Existed Before And Then Changed ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['metrics_updated'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['metrics'] == COLLECTED_BETTING_LINE_CASES['metrics_updated'][0]['metrics'])
        )

    async def test_storing_betting_lines_7(self):
        """~~~~~~~~~~ CASE 7: Extra Source Stats Existed Before And Then Changed ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['extra_source_stats_updated'])

        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['extra_source_stats'] == COLLECTED_BETTING_LINE_CASES['extra_source_stats_updated'][0]['extra_source_stats'])
        )
    
    async def test_storing_betting_lines_8(self):
        """~~~~~~~~~~ CASE 8: Metrics Non-Existent -> Existing ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_metrics'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        
        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['metrics'] ==
            COLLECTED_BETTING_LINE_CASES['base'][0]['metrics'])
        )
    
    async def test_storing_betting_lines_9(self):
        """~~~~~~~~~~ CASE 9: Extra Source Stats Non-Existent -> Existing ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_extra_source_stats'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        
        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            (stored_betting_line_docs[0]['extra_source_stats'] ==
            COLLECTED_BETTING_LINE_CASES['base'][0]['extra_source_stats'])
        )
    
    async def test_storing_betting_lines_10(self):
        """~~~~~~~~~~ CASE 10: Metrics Existed Before And Then Deleted ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_metrics'])
        
        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            ('metrics' not in stored_betting_line_docs[0])
        )
    
    async def test_storing_betting_lines_11(self):
        """~~~~~~~~~~ CASE 11: Extra Source Stats Existed Before And Then Deleted ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['no_extra_source_stats'])
        
        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 1) and
            ('extra_source_stats' not in stored_betting_line_docs[0])
        )
    
    async def test_storing_betting_lines_12(self):
        """~~~~~~~~~~ CASE 12: Stored Betting Line Was Not Collected ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['new_id'])
        
        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 2) and
            ({ k: v for k, v in stored_betting_line_docs[0].items() if k != 'stream' } == STORED_BETTING_LINE_CASES['base']) and
            (len(stored_betting_line_docs[0]['stream']) == 2) and
            (stored_betting_line_docs[0]['stream'][1].keys() == {'batch_timestamp'}) and
            (stored_betting_line_docs[0]['stream'][1]['batch_timestamp'] == COLLECTED_BETTING_LINE_CASES['base'][0]['batch_timestamp'])
        )
    
    async def test_storing_betting_lines_13(self):
        """~~~~~~~~~~ CASE 13: Stored Betting Line Was Not Collected (for > 1 Batch) ~~~~~~~~~~~"""
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['base'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['new_id'])
        await db.betting_lines.store_betting_lines(COLLECTED_BETTING_LINE_CASES['new_id'])
        
        stored_betting_line_docs = await db.betting_lines.collection.find({}).to_list()
        self.assertTrue(
            (len(stored_betting_line_docs) == 3) and
            ({ k: v for k, v in stored_betting_line_docs[0].items() if k != 'stream' } == STORED_BETTING_LINE_CASES['base']) and
            (len(stored_betting_line_docs[0]['stream']) == 2) and
            (stored_betting_line_docs[0]['stream'][1].keys() == {'batch_timestamp'}) and
            (stored_betting_line_docs[0]['stream'][1]['batch_timestamp'] == COLLECTED_BETTING_LINE_CASES['base'][0]['batch_timestamp'])
        )
        
        
class TestGetBettingLines(unittest.IsolatedAsyncioTestCase):

    async def setUp(self):
        await db.betting_lines.delete_betting_lines({})

    async def test_get_betting_lines_1(self):
        """~~~~~~~~~~ CASE 1: base ~~~~~~~~~~~"""
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'][0])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines({})
        self.assertEqual(stored_betting_line_docs, [STORED_BETTING_LINE_CASES['base']])

    async def test_get_betting_lines_2(self):
        """~~~~~~~~~~ CASE 2: Filter by Field ~~~~~~~~~~~"""
        await db.betting_lines.collection.insert_many(COLLECTED_BETTING_LINE_CASES['filter_by_field'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines({'league': 'NBA'})
        self.assertEqual(stored_betting_line_docs, [STORED_BETTING_LINE_CASES['base']])

    async def test_get_betting_lines_3(self):
        """~~~~~~~~~~ CASE 3: Previous Batch (One Record) ~~~~~~~~~~~"""
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines(prev_batch=True)
        self.assertEqual(stored_betting_line_docs, COLLECTED_BETTING_LINE_CASES['base'])
    
    async def test_get_betting_lines_4(self):
        """~~~~~~~~~~ CASE 4: Previous Batch (Multiple Records) ~~~~~~~~~~~"""
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'])
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['odds_moved'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines(prev_batch=True)
        self.assertEqual(stored_betting_line_docs, COLLECTED_BETTING_LINE_CASES['odds_moved'])
    
     # Todo: still need a test case for completed betting lines


class TestUpdateBettingLine(unittest.IsolatedAsyncioTestCase):
    
    async def setUp(self):
        await db.betting_lines.delete_betting_lines({})
        
    async def test_update_betting_line_1(self):
        """~~~~~~~~~~ CASE 1: Base ~~~~~~~~~~~"""
        await db.betting_lines.collection.insert_one(STORED_BETTING_LINE_CASES['base'])
        await db.betting_lines.update_betting_line(COLLECTED_BETTING_LINE_CASES['base']['_id'], UPDATE_BETTING_LINE_CASES['base'])

        stored_betting_line_docs = await db.betting_lines.get_betting_lines({})
        self.assertEqual(stored_betting_line_docs, [STORED_BETTING_LINE_CASES['base']])
        

if __name__ == '__main__':
    unittest.main()