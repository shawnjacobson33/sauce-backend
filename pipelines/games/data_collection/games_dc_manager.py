import asyncio
from datetime import datetime

from pipelines.games.data_collection import collectors
from pipelines.base import BaseManager


class GamesDataCollectionManager(BaseManager):
    """
    A manager class for handling the data collection of games.

    Attributes:
        configs (dict): The configuration settings.
    """

    def __init__(self, configs: dict):
        """
        Initializes the GamesDataCollectionManager with the given parameters.

        Args:
            configs (dict): The configuration settings.
        """
        super().__init__('Games', configs)

    async def run_collectors(self, batch_timestamp: datetime):
        """
        Runs the collectors to gather games data.

        Args:
            batch_timestamp (datetime): The timestamp for the batch.

        Returns:
            list: The container with collected games data.

        Raises:
            Exception: If an error occurs while running the collectors.
        """
        games_container = []

        try:
            coros = [
                collectors.BasketballGamesCollector(batch_timestamp, games_container, self.configs).run_collector(),
            ]
            await asyncio.gather(*coros)

            return games_container

        except Exception as e:
            self.log_message(f'Failed to run collectors: {e}', level='EXCEPTION')