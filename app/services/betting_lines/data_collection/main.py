import asyncio

from app.services.betting_lines.data_collection import collectors


async def run_collectors():
    collected_betting_lines = []
    coros = [
        collectors.run_boomfantasy_collector(collected_betting_lines),
        collectors.run_oddsshopper_collector(collected_betting_lines)
    ]
    await asyncio.gather(*coros)
    return collected_betting_lines


if __name__ == '__main__':
    asyncio.run(run_collectors())