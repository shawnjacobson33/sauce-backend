import asyncio

from app.db import db
from app.services.configs import load_configs
from app.services.games import GamesPipeline
from app.services.rosters import RostersPipeline
from app.services.box_scores import BoxScoresPipeline
from app.services.betting_lines import BettingLinesPipeline
from app.services.utils import Standardizer


async def _get_standardizer(configs: dict) -> Standardizer:
    subjects = await db.subjects.get_subjects({})
    return Standardizer(configs, subjects)


async def run_services():
    configs = load_configs()
    standardizer = await _get_standardizer(configs['standardization'])

    pipelines = [  # Todo: make sure RostersPipline runs first before BettingLinesPipeline just for first iteration?
        RostersPipeline(configs['rosters']).run_pipeline(),
        GamesPipeline(configs['games']).run_pipeline(),
        BoxScoresPipeline(configs['box_scores'], standardizer).run_pipeline(),
        BettingLinesPipeline(configs['betting_lines'], standardizer).run_pipeline(),
    ]

    await asyncio.gather(*pipelines)


if __name__ == '__main__':
    asyncio.run(run_services())