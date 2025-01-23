import time
from datetime import datetime

from db import db
from pipelines.utils import Standardizer
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger
from pipelines.betting_lines.data_collection import BettingLinesDataCollectionManager
from pipelines.betting_lines.data_processing import BettingLinesDataProcessingManager


class BettingLinesPipeline(BasePipeline):
    """
    A pipeline class for handling the betting lines data processing.

    Inherits from:
        BasePipeline: The base class for pipelines.
    """

    def __init__(self, configs: dict, standardizer: Standardizer):
        """
        Initializes the BettingLinesPipeline with the given parameters.

        Args:
            configs (dict): The configuration settings.
            standardizer (Standardizer): The standardizer instance.
        """
        super().__init__('BettingLines', configs)
        self.standardizer = standardizer

    async def _store_betting_lines(self, betting_lines: list[dict]) -> None:
        """
        Stores the betting lines in the database.

        Args:
            betting_lines (list[dict]): The list of betting lines to store.

        Raises:
            Exception: If an error occurs while storing the betting lines.
        """
        try:
            start_time = time.time()
            await db.betting_lines.store_betting_lines(betting_lines)
            self.times['store_betting_lines_time'] = round(time.time() - start_time, 2)

        except Exception as e:
            self.log_message(message=f'Error storing betting lines: {e}', level='EXCEPTION')

    async def _configure_pipeline(self) -> None:
        """
        Configures the pipeline by resetting the database collections if specified in the configs.

        Raises:
            Exception: If an error occurs while configuring the pipeline.
        """
        try:
            if self.configs['reset']:
                await db.database['betting_lines'].delete_many({})
                await db.database['completed_betting_lines'].delete_many({})
                await db.database['pipeline_stats'].delete_many({})

        except Exception as e:
            self.log_message(message=f'Failed to configure pipeline: {e}', level='EXCEPTION')

    @pipeline_logger
    async def run_pipeline(self):
        """
        Runs the betting lines pipeline to collect, process, and store betting lines data.

        Raises:
            Exception: If an error occurs during the pipeline execution.
        """
        try:
            batch_timestamp = datetime.now()
            db.pipeline_stats.update_batch_details(batch_timestamp)

            betting_lines_dc_manager = BettingLinesDataCollectionManager(self.configs['data_collection'],
                                                                         self.standardizer)
            betting_lines_container = await betting_lines_dc_manager.run_collectors(
                batch_timestamp)  # Todo: need to collect game markets also

            betting_lines_prc_manager = BettingLinesDataProcessingManager(self.configs['data_processing'],
                                                                          betting_lines_container)
            betting_lines_container = await betting_lines_prc_manager.run_processors()

            await self._store_betting_lines(betting_lines_container)
            await db.pipeline_stats.update_daily_stats(datetime.today())

        except Exception as e:
            self.log_message(message=f'Failed to run pipeline: {e}', level='EXCEPTION')