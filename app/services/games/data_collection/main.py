import asyncio

from app.services.games.data_collection import collectors


async def run_collectors():
    collected_games = []
    coros = [
        collectors.CBSSportsBasketballGamesCollector().run_collector(collected_games),
    ]
    await asyncio.gather(*coros)
    return collected_games


if __name__ == '__main__':
    asyncio.run(run_collectors())