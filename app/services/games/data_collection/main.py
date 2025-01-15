import asyncio
from datetime import datetime

from app.services.games.data_collection import collectors


async def run_collectors(batch_timestamp: datetime):
    games_container = []
    coros = [
        collectors.BasketballGamesCollector(batch_timestamp, games_container).run_collector(),
    ]
    await asyncio.gather(*coros)
    return games_container


if __name__ == '__main__':

    asyncio.run(run_collectors())