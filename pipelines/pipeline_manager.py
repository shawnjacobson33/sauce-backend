import asyncio

from db import db
from pipelines.base import BaseManager

from pipelines.games import GamesPipeline
from pipelines.rosters import RostersPipeline
from pipelines.box_scores import BoxScoresPipeline
from pipelines.betting_lines import BettingLinesPipeline
from pipelines.storage_pipeline import GCSPipeline
from pipelines.utils import Standardizer


class PipelineManager(BaseManager):
    """
    A manager class for handling the execution of various data pipelines.

    Inherits from:
        BaseManager: The base class for managers.
    """

    def __init__(self, configs: dict):
        """
        Initializes the PipelineManager with the given parameters.

        Args:
            configs (dict): The configuration settings.
        """
        super().__init__('PipelineBoss', configs)

    async def _get_standardizer(self) -> Standardizer:
        """
        Retrieves the standardizer with the necessary data.

        Returns:
            Standardizer: The standardizer object.

        Raises:
            Exception: If an error occurs while retrieving the standardizer.
        """
        try:
            teams = await db.teams.get_teams({})
            subjects = await db.subjects.get_subjects({})
            return Standardizer(self.configs['standardization'], teams, subjects)

        except Exception as e:
            self.log_message(f'Failed to get standardizer: {e}', level='EXCEPTION')

    async def run_pipelines(self) -> None:
        """
        Runs the configured pipelines to process and store data.

        Raises:
            Exception: If an error occurs during the pipeline execution.
        """
        try:
            if standardizer := await self._get_standardizer():
                pipelines = [
                    GamesPipeline(self.configs['games']).run_pipeline(),
                    RostersPipeline(self.configs['rosters']).run_pipeline(),
                    BoxScoresPipeline(self.configs['box_scores'], standardizer).run_pipeline(),
                    BettingLinesPipeline(self.configs['betting_lines'], standardizer).run_pipeline(),
                    GCSPipeline(self.configs['gcs']).run_pipeline()
                ]

                await asyncio.gather(*pipelines)

        except Exception as e:
            self.log_message(f'Failed to run pipelines: {e}', level='EXCEPTION')

l
if __name__ == '__main__':
    from pipelines.configs import load_configs

    pipeline_manager = PipelineManager(load_configs())
    asyncio.run(pipeline_manager.run_pipelines())