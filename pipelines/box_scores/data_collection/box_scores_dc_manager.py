import asyncio
from datetime import datetime

from pipelines.base import BaseManager
from pipelines.utils.standardization import Standardizer
from pipelines.box_scores.data_collection import collectors


class BoxScoresDataCollectionManager(BaseManager):
    """
    A manager class for handling the data collection of box scores.

    Attributes:
        configs (dict): The configuration settings.
        standardizer (Standardizer): The standardizer instance.
    """

    def __init__(self, configs: dict, standardizer: Standardizer):
        """
        Initializes the BoxScoresDataCollectionManager with the given parameters.

        Args:
            configs (dict): The configuration settings.
            standardizer (Standardizer): The standardizer instance.
        """
        super().__init__('BoxScores', configs)
        self.standardizer = standardizer

    async def run_collectors(self, batch_timestamp: datetime, games: list[dict]):
        """
        Runs the collectors to gather box scores data.

        Args:
            batch_timestamp (datetime): The timestamp for the batch.
            games (list[dict]): The list of games to collect box scores for.

        Returns:
            list: The container with collected box scores data.
        """
        box_scores_container = []

        try:
            coros = [
                (collectors.BasketballBoxScoresCollector(batch_timestamp, self.standardizer, box_scores_container,
                                                         games, self.configs)
                 .run_collector()),
            ]
            await asyncio.gather(*coros)

            return box_scores_container

        except Exception as e:
            self.log_message(f'Failed to run collectors: {e}', level='EXCEPTION')