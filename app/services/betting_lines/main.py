import asyncio
import time
from datetime import datetime
import random

from app.db import db
from app.services.utils import Standardizer
from app.services.configs import load_configs
from app.services.betting_lines.data_collection import run_collectors
from app.services.betting_lines.data_processing import BettingLinesProcessor


_CONFIGS = load_configs('betting_lines')
SECONDARY_MARKETS_EV_FORMULA_NAME = _CONFIGS['ev_formulas']['secondary_markets']


async def _get_last_batch_details(reset: bool = False) -> tuple[int, datetime]:
    if reset:
        await db.database['betting_lines_pipeline_status'].update_one(
            {'_id': 'betting_lines_pipeline_status'},
            {'$set': {'batch_num': 0, 'batch_timestamp': datetime.now()}},
            upsert=True
        )

    else:
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


async def _reset_betting_lines_and_pipeline_stats_collections():
    await db.database['betting_lines'].deleteMany({})
    await db.database['pipeline_stats'].deleteMany({})


async def run_pipeline():
    # await _reset_betting_lines_and_pipeline_stats_collections()
    secondary_markets_ev_formula = await db.metadata.get_ev_formula('secondary_markets',
                                                                    SECONDARY_MARKETS_EV_FORMULA_NAME)
    batch_num, batch_timestamp = await _get_last_batch_details(reset=True)  # Todo: store pipeline status in the betting lines collection not its own collection
    try:
        rosters = await db.rosters.get_rosters({})
        standardizer = Standardizer(rosters)
        while True:
            start_time = time.time()
            db.pipeline_stats.update_batch_details(batch_num, batch_timestamp)

            collected_betting_lines = await run_collectors(batch_num, batch_timestamp, standardizer)  # Todo: need to collect game markets also

            betting_lines_processor = BettingLinesProcessor(collected_betting_lines, secondary_markets_ev_formula)
            betting_lines_pr = betting_lines_processor.run_processor() # Todo: should be multi-processed

            print('[BettingLines]: Storing processed betting lines...')
            await db.betting_lines.store_betting_lines(betting_lines_pr)
            print(f'[BettingLines]: Stored {len(betting_lines_pr)} processed betting lines...')

            end_time = time.time()
            sleep_time = random.choice([60, 90, 120])
            print(f'[BettingLines]: Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for {sleep_time} seconds...')
            await db.pipeline_stats.update_daily_stats(datetime.today())
            batch_num, batch_timestamp = _update_batch_details(batch_num, batch_timestamp)
            await asyncio.sleep(sleep_time)

    finally:
        await db.database['betting_lines_pipeline_status'].update_one(
            {'_id': 'betting_lines_pipeline_status'},
            {'$set': {'batch_num': batch_num, 'batch_timestamp': batch_timestamp}},
            upsert=True
        )


if __name__ == '__main__':
    asyncio.run(run_pipeline())
