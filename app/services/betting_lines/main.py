import asyncio
import time

from app.db import db
from app.services.utils import Standardizer
from app.services.betting_lines.data_collection import run_collectors
from app.services.betting_lines.data_processing import run_processors


async def run_pipeline():
    rosters = await db.rosters.get_rosters({})
    standardizer = Standardizer(rosters)
    while True:
        start_time = time.time()
        print('[BettingLines]: Running betting lines pipeline...')
        print('[BettingLines]: Starting data collectors...')
        collected_betting_lines = await run_collectors(standardizer)
        print('[BettingLines]: Finished data collection...')
        print('[BettingLines]: Starting data processors...')
        betting_lines_pr = run_processors(collected_betting_lines) # Todo: should be multi-processed
        print('[BettingLines]: Finished data processing...')
        print('[BettingLines]: Storing processed betting lines...')
        await db.betting_lines.store_betting_lines(betting_lines_pr)
        print(f'[BettingLines]: Stored {len(betting_lines_pr)} processed betting lines...')
        end_time = time.time()
        print(f'[BettingLines]: Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for 60 seconds...')
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(run_pipeline())
