import asyncio

from app.db import db
from app.pipelines.configs import load_configs
from app.pipelines.games import GamesPipeline
from app.pipelines.rosters import RostersPipeline
from app.pipelines.box_scores import BoxScoresPipeline
from app.pipelines.betting_lines import BettingLinesPipeline
from app.pipelines.utils import Standardizer


async def _get_standardizer(configs: dict) -> Standardizer:
    subjects = await db.subjects.get_subjects({})
    return Standardizer(configs, subjects)


async def run_pipelines():
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
    asyncio.run(run_pipelines())