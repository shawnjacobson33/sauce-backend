import asyncio

from app.services.games import GamesPipeline
from app.services.rosters import RostersPipeline
from app.services.box_scores import BoxScoresPipeline
from app.services.betting_lines import BettingLinesPipeline


async def run_services():
    pipelines = [
        RostersPipeline().run_pipeline(),
        GamesPipeline().run_pipeline(),
        BoxScoresPipeline(reset=False).run_pipeline(),
        BettingLinesPipeline(reset=False).run_pipeline(),
    ]

    await asyncio.gather(*pipelines)
