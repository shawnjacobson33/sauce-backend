import asyncio
from datetime import datetime

from app.pipelines.rosters.data_collection import collectors


class RostersDataCollectionManager:

    def __init__(self, configs: dict):
        self.configs = configs

    async def run_collectors(self, batch_timestamp: datetime):
        rosters_container = []

        coros = [
            collectors.BasketballRostersCollector(
                batch_timestamp, rosters_container, self.configs).run_collector(),
        ]
        await asyncio.gather(*coros)

        return rosters_container
