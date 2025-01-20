import asyncio

from db import db

from pipelines.games import GamesPipeline
from pipelines.rosters import RostersPipeline
from pipelines.box_scores import BoxScoresPipeline
from pipelines.betting_lines import BettingLinesPipeline
from pipelines.storage_pipeline import GCSPipeline
from pipelines.utils import Standardizer


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
            # RostersPipeline(self.configs['rosters']).run_pipeline(),
            # GamesPipeline(self.configs['games']).run_pipeline(),
            # BoxScoresPipeline(self.configs['box_scores'], standardizer).run_pipeline(),
            BettingLinesPipeline(self.configs['betting_lines'], standardizer).run_pipeline(),
            # GCSPipeline(self.configs['gcs']).run_pipeline()
        ]
    
        await asyncio.gather(*pipelines)



# async def main():
# #     # await db.database['games'].insert_one({
# #     #     '_id': 'NBA_20250118_WAS@GS',
# #     #     'league': 'NBA',
# #     #     'away_team': 'WAS',
# #     #     'home_team': 'GS',
# #     #     'game_time': '2025-01-18T23:00:00Z',
# #     #     'status': 'live'
# #     # })
#     doc = await db.database['metadata'].find_one({'_id': 'metadata'})
#     await db.database['metadata'].update_one(
#         {'_id': 'metadata'},
#         {
#             '$rename': { 'ev_formulas.secondary_markets': 'ev_formulas.PlayerProps' },
#         }
#     )


# if __name__ == '__main__':
#     import asyncio
#
#     asyncio.run(main())

if __name__ == '__main__':
    from pipelines.configs import load_configs
    pipeline_manager = PipelineManager(load_configs())
    asyncio.run(pipeline_manager.run_pipelines())
