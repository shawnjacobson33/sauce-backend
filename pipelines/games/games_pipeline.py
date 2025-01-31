from datetime import datetime

from db import dev_db as db
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger
from pipelines.games.data_collection import GamesDataCollectionManager


class GamesPipeline(BasePipeline):
    """
    A pipeline class for handling the games data processing.

    Inherits from:
        BasePipeline: The base class for pipelines.
    """

    def __init__(self, configs: dict):
        """
        Initializes the GamesPipeline with the given parameters.

        Args:
            configs (dict): The configuration settings.
        """
        super().__init__('Games', configs)

    async def _configure_pipeline(self):
        """
        Configures the pipeline by resetting the database collections if specified in the configs.

        Raises:
            Exception: If an error occurs while configuring the pipeline.
        """
        try:
            if self.configs['reset']:
                await db.games.delete_games({})

        except Exception as e:
            self.log_message(f'Failed to configure pipeline: {e}', level='EXCEPTION')

    @pipeline_logger
    async def run_pipeline(self):
        """
        Runs the games pipeline to collect, process, and store games data.

        Raises:
            Exception: If an error occurs during the pipeline execution.
        """
        try:
            batch_timestamp = datetime.now()

            games_dc_manager = GamesDataCollectionManager(self.configs['data_collection'])
            collected_games = await games_dc_manager.run_collectors(batch_timestamp)

            await db.games.store_games(collected_games)

        except Exception as e:
            self.log_message(f'Failed to run pipeline: {e}', level='EXCEPTION')