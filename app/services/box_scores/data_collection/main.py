import asyncio

from app.services.utils import Standardizer
from app.services.box_scores.data_collection import collectors


async def run_collectors(games: list[dict], standardizer: Standardizer):
    collected_boxscores = []
    coros = [
        collectors.CBSSportsBasketballBoxscoresCollector(standardizer).run_collector(collected_boxscores, games),
    ]
    await asyncio.gather(*coros)
    return collected_boxscores
