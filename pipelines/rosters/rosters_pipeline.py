from datetime import datetime

from db import db
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger
from pipelines.rosters.data_collection import RostersDataCollectionManager


class RostersPipeline(BasePipeline):
    """
    A pipeline class for handling the rosters data processing.

    Inherits from:
        BasePipeline: The base class for pipelines.
    """

    def __init__(self, configs: dict):
        """
        Initializes the RostersPipeline with the given parameters.

        Args:
            configs (dict): The configuration settings.
        """
        super().__init__('Rosters', configs)

    async def _configure_pipeline(self) -> None:
        """
        Configures the pipeline by resetting the database collections if specified in the configs.

        Raises:
            Exception: If an error occurs while configuring the pipeline.
        """
        try:
            if self.configs['reset']:
                await db.subjects.delete_subjects({})

        except Exception as e:
            self.log_message(f'Failed to configure pipeline: {e}', level='EXCEPTION')

    @pipeline_logger
    async def run_pipeline(self):
        """
        Runs the rosters pipeline to collect, process, and store rosters data.

        Raises:
            Exception: If an error occurs during the pipeline execution.
        """
        batch_timestamp = datetime.now()

        try:
            rosters_dc_manager = RostersDataCollectionManager(self.configs['data_collection'])
            collected_rosters = await rosters_dc_manager.run_collectors(batch_timestamp)

            await db.subjects.store_subjects(collected_rosters)

        except Exception as e:
            self.log_message(f'Failed to run pipeline: {e}', level='EXCEPTION')
