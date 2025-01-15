import asyncio
from datetime import datetime

from app.services.rosters.data_collection import collectors


async def run_collectors(batch_timestamp: datetime):
    rosters_container = []
    coros = [
        collectors.BasketballRostersCollector(batch_timestamp, rosters_container).run_collector(),
    ]
    await asyncio.gather(*coros)
    return rosters_container
