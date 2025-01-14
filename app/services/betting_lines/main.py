import asyncio
import time
from datetime import datetime
import random

from app.db import db
from app.services.utils import utilities as utils, Standardizer
from app.services.configs import load_configs
from app.services.betting_lines.data_collection import run_collectors
from app.services.betting_lines.data_processing import BettingLinesProcessor


class BettingLinesPipeline:

    def __init__(self, reset: bool = False):
        self.reset = reset

        self.configs = load_configs('betting_lines')
        self.secondary_markets_ev_formula = self.configs['ev_formulas']['secondary_markets']

        self.times = {}

    async def _get_last_batch_details(self) -> datetime:
        if self.reset:
            await db.database['betting_lines_pipeline_status'].update_one(
                {'_id': 'betting_lines_pipeline_status'},
                {'$set': {'batch_timestamp': datetime.now()}},
                upsert=True
            )

        else:
            if last_batch_details := await db.database['betting_lines_pipeline_status'].find_one(
                {'_id': 'betting_lines_pipeline_status'}):
                prev_batch_timestamp = last_batch_details['batch_timestamp']
                return prev_batch_timestamp

        return datetime.now()

    @staticmethod
    async def _reset_betting_lines_and_pipeline_stats_collections():
        await db.database['betting_lines'].delete_many({})
        await db.database['pipeline_stats'].delete_many({})

    async def _store_betting_lines(self, betting_lines: list[dict]):
        print('[BettingLinesPipeline]: Storing processed betting lines...')
        start_time = time.time()
        await db.betting_lines.store_betting_lines(betting_lines)
        end_time = time.time()
        print(f'[BettingLinesPipeline]: Stored {len(betting_lines)} processed betting lines...')
        self.times['storage_time'] = round(end_time - start_time, 2)

    @utils.logger.pipeline_logger('BettingLines', message='Running Pipeline')
    async def run_pipeline(self):
        if self.reset:
            await self._reset_betting_lines_and_pipeline_stats_collections()

        secondary_markets_ev_formula = await db.metadata.get_ev_formula(
            'secondary_markets', self.secondary_markets_ev_formula
        )
        subjects = await db.subjects.get_subjects({})
        standardizer = Standardizer(subjects)
        while True:
            batch_timestamp = datetime.now()
            db.pipeline_stats.update_batch_details(batch_timestamp)
            collected_betting_lines = await run_collectors(batch_timestamp, standardizer)  # Todo: need to collect game markets also

            betting_lines_processor = BettingLinesProcessor(collected_betting_lines, secondary_markets_ev_formula)
            betting_lines_pr = betting_lines_processor.run_processor() # Todo: should be multi-processed

            await self._store_betting_lines(betting_lines_pr)
            await db.pipeline_stats.update_daily_stats(datetime.today())

            sleep_time = random.choice([60, 90, 120])
            print(f'[BettingLinesPipeline]: Iteration complete! Sleeping for {sleep_time} seconds...')
            await asyncio.sleep(sleep_time)


if __name__ == '__main__':
    pipeline = BettingLinesPipeline(reset=True)
    asyncio.run(pipeline.run_pipeline())
