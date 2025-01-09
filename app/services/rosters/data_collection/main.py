import asyncio

from app.services.rosters.data_collection import collectors


async def run_collectors():
    collected_rosters = []
    coros = [
        collectors.run_cbssports_basketball_rosters_collector(collected_rosters),
    ]
    await asyncio.gather(*coros)
    return collected_rosters


if __name__ == '__main__':
    asyncio.run(run_collectors())