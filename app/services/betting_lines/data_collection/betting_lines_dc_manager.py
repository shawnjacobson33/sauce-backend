import asyncio
from datetime import datetime

from app.services.utils import Standardizer
from app.services.betting_lines.data_collection import collectors



class BettingLinesDataCollectionManager:

    def __init__(self, configs: dict, standardizer: Standardizer):
        self.configs = configs
        self.standardizer = standardizer

    async def run_collectors(self, batch_timestamp: datetime):
        betting_lines_container = []

        coros = [
            collectors.OddsShopperCollector(batch_timestamp, betting_lines_container, self.standardizer, self.configs).run_collector(),
            collectors.BoomFantasyCollector(batch_timestamp, betting_lines_container, self.standardizer, self.configs).run_collector()
        ]
        await asyncio.gather(*coros)

        return betting_lines_container
