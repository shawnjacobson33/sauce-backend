import asyncio
import time

from app.db import db
from app.services.betting_lines.data_collection import run_collectors
from app.services.betting_lines.data_processing import run_processors


async def run_betting_lines_pipeline():
    # while True:
        start_time = time.time()
        print('Running betting lines pipeline...')
        print('Starting data collectors...')
        collected_betting_lines = await run_collectors()
        print('Finished data collection...')
        print('Starting data processors...')
        betting_lines_pr = run_processors(collected_betting_lines) # Todo: should be multi-processed
        print('Finished data processing...')
        print('Storing processed betting lines...')
        await db.betting_lines.store_betting_lines(betting_lines_pr)
        print(f'Stored {len(betting_lines_pr)} processed betting lines...')
        end_time = time.time()
        print(f'Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for 60 seconds...')
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(run_betting_lines_pipeline())
