import asyncio
import time

from app.cache import session

from app.services.betting_lines.data_collection import run_collectors
from app.services.betting_lines.data_processing import run_processors


async def run_betting_lines_pipeline():
    while True:
        start_time = time.time()
        print('Running betting lines pipeline...')
        print('Starting data collection...')
        collected_betting_lines = await run_collectors()
        print('Finished data collection...')
        print('Starting data processing...')
        betting_lines_df_pr = await run_processors(collected_betting_lines) # Todo: should be multi-processed
        print('Finished data processing...')
        print('Saving processed betting lines...')
        session.betting_lines.storelines(betting_lines_df_pr)
        print(f'Saved {len(betting_lines_df_pr)} processed betting lines...')
        end_time = time.time()
        print(f'Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for 60 seconds...')
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(run_betting_lines_pipeline())
