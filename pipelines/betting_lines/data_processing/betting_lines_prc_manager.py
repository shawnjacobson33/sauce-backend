import multiprocessing as mp

import pandas as pd

from db import db
from pipelines.betting_lines.data_processing import processors


class BettingLinesDataProcessingManager:

    def __init__(self, configs: dict, betting_lines_container: list[dict]):
        self.configs = configs
        self.betting_lines_container = betting_lines_container

    async def _get_ev_formulas(self):
        for market_type, ev_formula_info in self.configs['ev_formulas'].items():
            ev_formula_info['formula'] = await db.metadata.get_ev_formula(market_type, ev_formula_info['name'])
            
    async def run_processors(self):
        await self._get_ev_formulas()

        betting_lines_df = pd.DataFrame(self.betting_lines_container)
        if not betting_lines_df.empty:
            game_lines = processors.GameLinesProcessor(betting_lines_df, self.configs).run_processor()
            player_prop_lines = processors.PlayerPropsProcessor(betting_lines_df, self.configs).run_processor()

            betting_lines = game_lines + player_prop_lines

            return betting_lines