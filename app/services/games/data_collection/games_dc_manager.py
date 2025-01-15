import asyncio
from datetime import datetime

from app.services.games.data_collection import collectors


class GamesDataCollectionManager:

    def __init__(self, configs: dict):
        self.configs = configs

    async def run_collectors(self, batch_timestamp: datetime):
        games_container = []

        coros = [
            collectors.BasketballGamesCollector(batch_timestamp, games_container, self.configs).run_collector(),
        ]
        await asyncio.gather(*coros)

        return games_container
