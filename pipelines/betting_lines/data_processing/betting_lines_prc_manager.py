import pandas as pd

from db import db
from pipelines.betting_lines.data_processing import processors
from pipelines.base import BaseManager


class BettingLinesDataProcessingManager(BaseManager):
    """
    A class to manage the processing of betting lines data.

    Attributes:
        configs (dict): The configuration settings.
        betting_lines_container (list[dict]): The container for betting lines data.
    """

    def __init__(self, configs: dict, betting_lines_container: list[dict]):
        """
        Initializes the BettingLinesDataProcessingManager with the given parameters.

        Args:
            configs (dict): The configuration settings.
            betting_lines_container (list[dict]): The container for betting lines data.
        """
        super().__init__('BettingLines', configs)
        self.betting_lines_container = betting_lines_container

    async def _get_ev_formulas(self):
        """
        Retrieves the expected value formulas from the database and updates the configuration settings.
        """
        for market_type, ev_formula_info in self.configs['ev_formulas'].items():
            ev_formula_info['formula'] = await db.metadata.get_ev_formula(market_type, ev_formula_info['name'])

    async def run_processors(self):
        """
        Runs the processors to calculate expected values for game lines and player prop lines.

        Returns:
            list[dict]: The list of processed betting lines.

        Logs:
            Warning: If no betting lines are found.
        """
        try:
            await self._get_ev_formulas()

            if self.betting_lines_container:
                betting_lines_df = pd.DataFrame(self.betting_lines_container)
                game_lines = processors.GameLinesProcessor(betting_lines_df, self.configs).run_processor()
                player_prop_lines = processors.PlayerPropsProcessor(betting_lines_df, self.configs).run_processor()

                betting_lines = game_lines + player_prop_lines
                return betting_lines

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to run processors: {e}')


