import asyncio

from app.db import db
from app.services.games import GamesPipeline
from app.services.rosters import RostersPipeline
from app.services.box_scores import BoxScoresPipeline
from app.services.betting_lines import BettingLinesPipeline
from app.services.utils import Standardizer


async def run_services():
    subjects = await db.subjects.get_subjects({})
    standardizer = Standardizer(subjects)
    pipelines = [
        # RostersPipeline().run_pipeline(),
        GamesPipeline().run_pipeline(),
        # BoxScoresPipeline(standardizer, reset=True).run_pipeline(),
        # BettingLinesPipeline(standardizer, reset=True).run_pipeline(),
    ]

    await asyncio.gather(*pipelines)


if __name__ == '__main__':
    asyncio.run(run_services())