import asyncio
from datetime import datetime

from pipelines.utils import Standardizer
from pipelines.betting_lines.data_collection import collectors


class BettingLinesDataCollectionManager:
    """
    A class to manage the collection of betting lines data.

    Attributes:
        configs (dict): The configuration settings.
        standardizer (Standardizer): The standardizer for data.
    """

    def __init__(self, configs: dict, standardizer: Standardizer):
        """
        Initializes the BettingLinesDataCollectionManager with the given parameters.

        Args:
            configs (dict): The configuration settings.
            standardizer (Standardizer): The standardizer for data.
        """
        self.configs = configs
        self.standardizer = standardizer

    async def run_collectors(self, batch_timestamp: datetime):
        """
        Runs the collectors to gather betting lines data.

        Args:
            batch_timestamp (datetime): The timestamp of the batch.

        Returns:
            list[dict]: The collected betting lines data.
        """
        betting_lines_container = []

        coros = [
            collectors.OddsShopperCollector(batch_timestamp, betting_lines_container, self.standardizer, self.configs).run_collector(),
            collectors.BoomFantasyCollector(batch_timestamp, betting_lines_container, self.standardizer, self.configs).run_collector()
        ]

        await asyncio.gather(*coros)

        return betting_lines_container