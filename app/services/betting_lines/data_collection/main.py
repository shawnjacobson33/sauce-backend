import asyncio

from app.services.utils import Standardizer
from app.services.betting_lines.data_collection import collectors


async def run_collectors(standardizer: Standardizer):
    collected_betting_lines = []
    coros = [
        collectors.OddsShopperCollector(collected_betting_lines, standardizer).run_collector()
    ]
    await asyncio.gather(*coros)
    return collected_betting_lines


if __name__ == '__main__':
    asyncio.run(run_collectors())