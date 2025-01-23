import asyncio
from datetime import datetime

from pipelines.base import BaseManager
from pipelines.rosters.data_collection import collectors


class RostersDataCollectionManager(BaseManager):
    """
    A manager class for handling the data collection of rosters.

    Attributes:
        configs (dict): The configuration settings.
    """

    def __init__(self, configs: dict):
        """
        Initializes the RostersDataCollectionManager with the given parameters.

        Args:
            configs (dict): The configuration settings.
        """
        super().__init__('Rosters', configs)

    async def run_collectors(self, batch_timestamp: datetime):
        """
        Runs the collectors to gather rosters data.

        Args:
            batch_timestamp (datetime): The timestamp for the batch.

        Returns:
            list: The container with collected rosters data.
        """
        rosters_container = []

        try:
            coros = [
                collectors.BasketballRostersCollector(
                    batch_timestamp, rosters_container, self.configs).run_collector(),
            ]
            await asyncio.gather(*coros)

            return rosters_container

        except Exception as e:
            self.log_message(message=f'Failed to run collectors: {e}', level='EXCEPTION')