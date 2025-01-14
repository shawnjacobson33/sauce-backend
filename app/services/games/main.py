import asyncio
import time

from app.db import db
from app.services.base import BasePipeline
from app.services.games.data_collection import run_collectors
from app.services.utils import utilities as utils


class GamesPipeline(BasePipeline):

    def __init__(self, reset: bool = False):
        super().__init__(reset)

    @utils.logger.pipeline_logger('Games', message='Running Pipeline')
    async def run_pipeline(self):
        if self.reset:
            await db.games.delete_games({})

        while True:
            start_time = time.time()
            print('[Games]: Running games pipeline...')
            print('[Games]: Starting data collectors...')
            collected_games = await run_collectors()
            print('[Games]: Finished data collection...')
            print('[Games]: Storing collected games...')
            await db.games.store_games(collected_games)
            print(f'[Games]: Stored {len(collected_games)} collected rosters...')
            end_time = time.time()
            print(f'[Games]: Pipeline completed in {round(end_time - start_time, 2)} seconds. See you tomorrow...')
            await asyncio.sleep(60 * 60 * 24)
