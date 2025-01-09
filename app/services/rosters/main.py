import asyncio
import time

from app.db import db
from app.services.rosters.data_collection import run_collectors


# Todo: if you are sleeping for an entire day make sure to release resources
async def run_pipeline():
    while True:
        start_time = time.time()
        print('[Rosters]: Running teams pipeline...')
        print('[Rosters]: Starting data collectors...')
        collected_rosters = await run_collectors()
        print('[Rosters]: Finished data collection...')
        print('[Rosters]: Storing collected rosters...')
        await db.rosters.store_rosters(collected_rosters)
        print(f'[Rosters]: Stored {len(collected_rosters)} collected rosters...')
        end_time = time.time()
        print(f'[Rosters]: Pipeline completed in {round(end_time - start_time, 2)} seconds. See you tomorrow...')
        await asyncio.sleep(60 * 60 * 24)


if __name__ == '__main__':
    asyncio.run(run_pipeline())