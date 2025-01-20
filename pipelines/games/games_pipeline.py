from datetime import datetime

from db import db
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger
from pipelines.games.data_collection import GamesDataCollectionManager


class GamesPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('Games', configs)

    async def _configure_pipeline(self):
        if self.configs['reset']:
            await db.games.delete_games({})

    @pipeline_logger
    async def run_pipeline(self):
        batch_timestamp = datetime.now()

        games_dc_manager = GamesDataCollectionManager(self.configs['data_collection'])
        collected_games = await games_dc_manager.run_collectors(batch_timestamp)

        await db.games.store_games(collected_games)
