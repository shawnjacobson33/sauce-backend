from datetime import datetime

from db import db
from pipelines.pipeline_base import BasePipeline, logger
from pipelines.box_scores.data_collection import BoxScoresDataCollectionManager
from pipelines.utils import Standardizer


class BoxScoresPipeline(BasePipeline):

    def __init__(self, configs: dict, standardizer: Standardizer):
        super().__init__('Boxscores', configs)
        self.standardizer = standardizer

    @staticmethod
    def _get_finished_games(games: list[dict]) -> list[dict]:
        return [game for game in games if game.get('period') == 'End']

    @staticmethod
    async def _cleanup_finished_games(games: list[dict]) -> None:
        game_ids = [game['_id'] for game in games]
        await db.betting_lines.store_completed_betting_lines(game_ids=game_ids)
        await db.games.delete_games(game_ids)
        await db.box_scores.delete_box_scores(game_ids)

    async def _check_for_finished_games(self, games: list[dict]) -> None:
        if finished_games := self._get_finished_games(games):
            await self._cleanup_finished_games(finished_games)

    async def _configure_pipeline(self):
        if self.configs['reset']:
            await db.box_scores.delete_box_scores({})

    @logger
    async def run_pipeline(self):
        if live_games := await db.games.get_games({}, live=True):  # Poll for live games...TODO: more efficient ways to get live games?
            batch_timestamp = datetime.now()

            box_scores_dc_manager = BoxScoresDataCollectionManager(self.configs['data_collection'], self.standardizer)
            collected_boxscores = await box_scores_dc_manager.run_collectors(batch_timestamp, live_games)

            await db.box_scores.store_box_scores(collected_boxscores)
            await db.betting_lines.update_live_stats(collected_boxscores)
            await self._check_for_finished_games(live_games)
