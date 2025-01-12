import asyncio
import time

from app.db import db
from app.services.box_scores.data_collection import run_collectors
from app.services.utils import Standardizer


# Todo: if you are sleeping for an entire day make sure to release resources
async def run_pipeline():
    players = await db.players.get_players({})
    standardizer = Standardizer(players)
    while True:
        if live_games := await db.games.get_games({}, live=True):  # Poll for live games...TODO: more efficient ways to get live games?
            start_time = time.time()
            print('[BoxScores]: Running boxscores pipeline...')
            print('[BoxScores]: Starting data collectors...')
            collected_boxscores = await run_collectors(live_games, standardizer)
            print('[BoxScores]: Finished data collection...')
            print('[BoxScores]: Storing collected boxscores...')
            await db.boxscores.store_games(collected_boxscores)
            print(f'[BoxScores]: Stored {len(collected_boxscores)} collected boxscores...')
            end_time = time.time()
            print(f'[BoxScores]: Pipeline completed in {round(end_time - start_time, 2)} seconds. See you tomorrow...')
            await asyncio.sleep(30)


if __name__ == '__main__':
    asyncio.run(run_pipeline())