import asyncio
from datetime import datetime

from app.services.utils import Standardizer
from app.services.box_scores.data_collection import collectors


class BoxScoresDataCollectionManager:

    def __init__(self, configs: dict, standardizer: Standardizer):
        self.configs = configs
        self.standardizer = standardizer

    async def run_collectors(self, batch_timestamp: datetime, games: list[dict]):
        box_scores_container = []

        coros = [
            (collectors.BasketballBoxScoresCollector(batch_timestamp, self.standardizer, box_scores_container, games, self.configs)
                        .run_collector()),
        ]
        await asyncio.gather(*coros)

        return box_scores_container
