import asyncio
import time

from app.db import db
from app.services.rosters.data_collection import run_collectors


async def run_pipeline():
    teams = await db.teams.get_teams()
    while True:
        start_time = time.time()
        print('[Teams]: Running teams pipeline...')
        print('[Teams]: Starting data collectors...')
        rosters = await run_collectors(teams)
        print('Finished data collection...')
        print('Storing processed betting lines...')
        await db.betting_lines.store_betting_lines(betting_lines_pr)
        print(f'Stored {len(betting_lines_pr)} processed betting lines...')
        end_time = time.time()
        print(f'Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for 60 seconds...')
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(run_pipeline())