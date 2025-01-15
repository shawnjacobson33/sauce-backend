import asyncio

from app.db import db
from app.services.base import BasePipeline
from app.services.box_scores.data_collection import run_collectors
from app.services.utils import utilities as utils, Standardizer


class BoxScoresPipeline(BasePipeline):

    def __init__(self, standardizer: Standardizer, reset: bool = False):
        super().__init__('Boxscores', reset)
        self.standardizer = standardizer

    @staticmethod
    def _get_finished_games(games: list[dict]) -> list[dict]:
        return [game for game in games if db.games.is_game_finished(game)]

    @staticmethod
    async def _cleanup_finished_games(games: list[dict]) -> None:
        game_ids = [game['_id'] for game in games]
        await db.betting_lines.update_betting_line_results(game_ids)
        await db.games.delete_games(game_ids)
        await db.box_scores.delete_box_scores(game_ids)

    async def _check_for_finished_games(self, games: list[dict]) -> None:
        if finished_games := self._get_finished_games(games):
            await self._cleanup_finished_games(finished_games)

    @staticmethod
    async def _store_box_scores(collected_boxscores: list[dict]) -> None:
        print('[BoxScores]: Storing collected boxscores...')
        await db.box_scores.store_box_scores(collected_boxscores)
        print(f'[BoxScores]: Stored {len(collected_boxscores)} collected boxscores...')

    @utils.logger.pipeline_logger(message='Running Pipeline')
    async def run_pipeline(self):
        if self.reset:
            await db.box_scores.delete_box_scores({})

        while True:
            if live_games := await db.games.get_games({}, live=True):  # Poll for live games...TODO: more efficient ways to get live games?
                collected_boxscores = await run_collectors(live_games, self.standardizer)

                await self._store_box_scores(collected_boxscores)
                await db.betting_lines.update_live_stats(collected_boxscores)
                await self._check_for_finished_games(live_games)

                sleep_time = 30
                print(f'[BoxScoresPipeline]: Iteration complete! Sleeping for {sleep_time} seconds...')
                await asyncio.sleep(sleep_time)
