import asyncio

from app.services.rosters.data_collection import collectors


async def run_collectors(teams: dict):
    collected_rosters = []
    coros = [
        collectors.run_cbssports_basketball_rosters_collector(collected_rosters, teams=teams['Basketball']),
    ]
    await asyncio.gather(*coros)
    return collected_rosters


if __name__ == '__main__':
    asyncio.run(run_collectors())