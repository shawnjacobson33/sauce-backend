import asyncio
from datetime import datetime

from app.services.utils import Standardizer
from app.services.betting_lines.data_collection import collectors


async def run_collectors(batch_timestamp: datetime, standardizer: Standardizer):
    collected_betting_lines = []
    coros = [
        collectors.OddsShopperCollector(batch_timestamp, collected_betting_lines, standardizer).run_collector()
    ]
    await asyncio.gather(*coros)
    return collected_betting_lines


if __name__ == '__main__':
    asyncio.run(run_collectors())