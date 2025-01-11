import asyncio
import time
from datetime import datetime
import random

from app.db import db
from app.services.utils import Standardizer
from app.services.configs import load_configs
from app.services.betting_lines.data_collection import run_collectors
from app.services.betting_lines.data_processing import run_processors


_CONFIGS = load_configs('betting_lines')
SECONDARY_MARKETS_EV_FORMULA_NAME = _CONFIGS['ev_formulas']['secondary_markets']


async def _get_last_batch_details() -> tuple[int, datetime]:
    if last_batch_details := await db.database['betting_lines_pipeline_status'].find_one(
        {'_id': 'betting_lines_pipeline_status'}):
        prev_batch_num, prev_batch_timestamp = last_batch_details['batch_num'], last_batch_details['batch_timestamp']
        if datetime.now().day > prev_batch_timestamp.day:
            prev_batch_num = 0

        return prev_batch_num, prev_batch_timestamp

    return 0, datetime.now()


def _update_batch_details(batch_num: int, prev_batch_timestamp: datetime) -> tuple[int, datetime]:
    new_batch_num = batch_num + 1
    if datetime.now().day > prev_batch_timestamp.day:
        new_batch_num = 0

    return new_batch_num, datetime.now()


async def run_pipeline():
    secondary_markets_ev_formula = await db.metadata.get_ev_formula('secondary_markets',
                                                                    SECONDARY_MARKETS_EV_FORMULA_NAME)
    batch_num, batch_timestamp = await _get_last_batch_details()
    try:
        rosters = await db.rosters.get_rosters({})
        standardizer = Standardizer(rosters)
        while True:
            start_time = time.time()
            print('[BettingLines]: Running betting lines pipeline...')
            print('[BettingLines]: Starting data collectors...')
            collected_betting_lines = await run_collectors(batch_num, batch_timestamp, standardizer)  # Todo: need to collect game markets also
            print('[BettingLines]: Finished data collection...')
            print('[BettingLines]: Starting data processors...')
            betting_lines_pr = run_processors(
                collected_betting_lines, secondary_markets_ev_formula, SECONDARY_MARKETS_EV_FORMULA_NAME) # Todo: should be multi-processed
            print('[BettingLines]: Finished data processing...')
            print('[BettingLines]: Storing processed betting lines...')
            await db.betting_lines.store_betting_lines(betting_lines_pr)
            print(f'[BettingLines]: Stored {len(betting_lines_pr)} processed betting lines...')
            end_time = time.time()
            print(f'[BettingLines]: Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for 60 seconds...')
            batch_num, batch_timestamp = _update_batch_details(batch_num, batch_timestamp)
            await asyncio.sleep(random.randint(85, 95))

    finally:
        await db.database['betting_lines_pipeline_status'].update_one(
            {'_id': 'betting_lines_pipeline_status'},
            {'$set': {'batch_num': batch_num, 'batch_timestamp': batch_timestamp}},
            upsert=True
        )


if __name__ == '__main__':
    asyncio.run(run_pipeline())
