from datetime import datetime

from db import db
from pipelines.utils import Standardizer
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger
from pipelines.box_scores.data_collection import BoxScoresDataCollectionManager


class BoxScoresPipeline(BasePipeline):
    """
    A pipeline class for handling the box scores data processing.

    Inherits from:
        BasePipeline: The base class for pipelines.
    """

    def __init__(self, configs: dict, standardizer: Standardizer):
        """
        Initializes the BoxScoresPipeline with the given parameters.

        Args:
            configs (dict): The configuration settings.
            standardizer (Standardizer): The standardizer instance.
        """
        super().__init__('BoxScores', configs)
        self.standardizer = standardizer

    def _is_game_finished(self, game: dict) -> bool:
        """
        Checks if a game is finished.

        Args:
            game (dict): The game information.

        Returns:
            bool: True if the game is finished, False otherwise.
        """
        try:
            return game['period'] == 'End' and game['live_time'] == '4th'

        except Exception as e:
            self.log_message(f"Error in _is_game_finished: {e}", level='EXCEPTION')

    def _get_finished_games(self, games: list[dict]) -> list[dict]:
        """
        Retrieves the list of finished games.

        Args:
            games (list[dict]): The list of games.

        Returns:
            list[dict]: The list of finished games.
        """
        finished_games = []
        for game in games:
            if self._is_game_finished(game):
                finished_games.append(game)

        return finished_games

    async def _cleanup_finished_games(self, games: list[dict]) -> None:
        """
        Cleans up the finished games from the database.

        Args:
            games (list[dict]): The list of finished games.

        Raises:
            Exception: If an error occurs during the cleanup process.
        """
        try:
            game_ids = [game['_id'] for game in games]
            await db.betting_lines.store_completed_betting_lines(game_ids=game_ids)
            await db.games.delete_games(game_ids)
            await db.box_scores.delete_box_scores(game_ids)

        except Exception as e:
            self.log_message(f"Error in _cleanup_finished_games: {e}", level='EXCEPTION')

    async def _check_for_finished_games(self, games: list[dict]) -> None:
        """
        Checks for finished games and performs cleanup if any are found.

        Args:
            games (list[dict]): The list of games.
        """
        if finished_games := self._get_finished_games(games):
            await self._cleanup_finished_games(finished_games)

    async def _configure_pipeline(self):
        """
        Configures the pipeline by resetting the database collections if specified in the configs.

        Raises:
            Exception: If an error occurs while configuring the pipeline.
        """
        try:
            if self.configs['reset']:
                await db.box_scores.delete_box_scores({})

        except Exception as e:
            self.log_message(f"Error in _configure_pipeline: {e}", level='EXCEPTION')

    @pipeline_logger
    async def run_pipeline(self):
        """
        Runs the box scores pipeline to collect, process, and store box scores data.

        Raises:
            Exception: If an error occurs during the pipeline execution.
        """
        try:
            if live_games := await db.games.get_games({},
                                                      live=True):  # Poll for live games...TODO: more efficient ways to get live games?
                batch_timestamp = datetime.now()

                box_scores_dc_manager = BoxScoresDataCollectionManager(self.configs['data_collection'],
                                                                       self.standardizer)
                collected_boxscores = await box_scores_dc_manager.run_collectors(batch_timestamp, live_games)

                await db.box_scores.store_box_scores(
                    collected_boxscores)  # Todo: need to be more efficient with storing game info
                await db.betting_lines.update_live_stats(collected_boxscores)
                await self._check_for_finished_games(live_games)

        except Exception as e:
            self.log_message(f"Error in run_pipeline: {e}", level='EXCEPTION')