import asyncio

from app.db import db
from app.services.configs import load_configs
from app.services.games import GamesPipeline
from app.services.rosters import RostersPipeline
from app.services.box_scores import BoxScoresPipeline
from app.services.betting_lines import BettingLinesPipeline
from app.services.utils import Standardizer


def _get_standardizer():
    subjects = db.subjects.get_subjects({})
    return Standardizer(subjects)


async def run_services():
    configs = load_configs()
    standardizer = _get_standardizer()

    pipelines = [
        RostersPipeline(configs['rosters']).run_pipeline(),
        GamesPipeline(configs['games']).run_pipeline(),
        BoxScoresPipeline(configs['box_scores'], standardizer).run_pipeline(),
        BettingLinesPipeline(configs['betting_lines'], standardizer).run_pipeline(),
    ]

    await asyncio.gather(*pipelines)


if __name__ == '__main__':
    asyncio.run(run_services())