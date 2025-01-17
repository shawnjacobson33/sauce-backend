import asyncio
import time
from datetime import datetime

from db import db
from pipelines.pipeline_base import BasePipeline, logger
from pipelines.games.data_collection import GamesDataCollectionManager
from pipelines.utils import utilities as utils


class GamesPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('Games', configs)

    @logger
    async def run_pipeline(self):
        if self.configs['reset']:
            await db.games.delete_games({})

        while True:
            batch_timestamp = datetime.now()
            start_time = time.time()

            games_dc_manager = GamesDataCollectionManager(self.configs['data_collection'])
            collected_games = await games_dc_manager.run_collectors(batch_timestamp)

            print('[Games]: Storing collected games...')
            await db.games.store_games(collected_games)
            print(f'[Games]: Stored {len(collected_games)} collected games...')

            end_time = time.time()

            sleep_time = self.configs['throttle']
            print(f'[Games]: Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for {sleep_time} seconds...')
            await asyncio.sleep(sleep_time)
