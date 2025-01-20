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

    async def run_processors(self):  # Todo: Needs to be multi-processsed
        await self._get_ev_formulas()

        betting_lines_df = pd.DataFrame(self.betting_lines_container)

        game_lines_df = betting_lines_df[betting_lines_df['market_domain'] == 'Gamelines']
        game_lines_container = processors.GameLinesProcessor(game_lines_df, self.configs).run_processor()

        player_prop_lines_df = betting_lines_df[betting_lines_df['market_domain'] == 'PlayerProps']
        player_prop_lines_container = processors.PlayerPropsProcessor(player_prop_lines_df, self.configs).run_processor()

        return [*game_lines_container, *player_prop_lines_container]