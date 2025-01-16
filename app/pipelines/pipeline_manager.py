import asyncio

from app.db import db

from app.pipelines.games import GamesPipeline
from app.pipelines.rosters import RostersPipeline
from app.pipelines.box_scores import BoxScoresPipeline
from app.pipelines.betting_lines import BettingLinesPipeline
from app.pipelines.utils import Standardizer


class PipelineManager:
    
    def __init__(self, configs: dict):
        self.configs = configs
    
    async def _get_standardizer(self) -> Standardizer:
        teams = await db.teams.get_teams({})
        subjects = await db.subjects.get_subjects({})
        return Standardizer(self.configs['standardization'], teams, subjects)
    
    
    async def run_pipelines(self):
        standardizer = await self._get_standardizer()

        pipelines = [  # Todo: make sure RostersPipline runs first before BettingLinesPipeline just for first iteration?
            RostersPipeline(self.configs['rosters']).run_pipeline(),
            GamesPipeline(self.configs['games']).run_pipeline(),
            BoxScoresPipeline(self.configs['box_scores'], standardizer).run_pipeline(),
            BettingLinesPipeline(self.configs['betting_lines'], standardizer).run_pipeline(),
        ]
    
        await asyncio.gather(*pipelines)


if __name__ == '__main__':
    from app.pipelines.configs import load_configs
    pipeline_manager = PipelineManager(load_configs())
    asyncio.run(pipeline_manager.run_pipelines())