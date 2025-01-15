import asyncio
import time
from datetime import datetime

from app.db import db
from app.services.base import BasePipeline
from app.services.games.data_collection import run_collectors
from app.services.utils import utilities as utils


class GamesPipeline(BasePipeline):

    def __init__(self, reset: bool = False):
        super().__init__('Games', reset)

    @utils.logger.pipeline_logger(message='Running Pipeline')
    async def run_pipeline(self):
        if self.reset:
            await db.games.delete_games({})

        while True:
            batch_timestamp = datetime.now()
            start_time = time.time()
            print('[Games]: Running games pipeline...')
            print('[Games]: Starting data collectors...')
            collected_games = await run_collectors(batch_timestamp)
            print('[Games]: Finished data collection...')
            print('[Games]: Storing collected games...')
            await db.games.store_games(collected_games)
            print(f'[Games]: Stored {len(collected_games)} collected games...')
            end_time = time.time()
            print(f'[Games]: Pipeline completed in {round(end_time - start_time, 2)} seconds. See you tomorrow...')
            await asyncio.sleep(60 * 60 * 24)


if __name__ == '__main__':
    asyncio.run(GamesPipeline(reset=True).run_pipeline())
