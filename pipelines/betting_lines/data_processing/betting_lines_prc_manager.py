from db import db
from pipelines.betting_lines.data_processing import processors


class BettingLinesDataProcessingManager:

    def __init__(self, configs: dict, betting_lines_container: list[dict]):
        self.configs = configs
        self.betting_lines_container = betting_lines_container

    async def _get_ev_formulas(self):
        ev_formulas_main_markets = self.configs['ev_formulas']['main_markets']
        ev_formulas_main_markets['formula'] = await db.metadata.get_ev_formula(
            'main_markets', ev_formulas_main_markets['name']
        )

        ev_formulas_secondary_markets = self.configs['ev_formulas']['secondary_markets']
        ev_formulas_secondary_markets['formula'] = await db.metadata.get_ev_formula(
            'secondary_markets', ev_formulas_secondary_markets['name']
        )

    async def run_processors(self):  # Todo: Needs to be multi-processsed
        await self._get_ev_formulas()

        processor = processors.BettingLinesProcessor(self.betting_lines_container, self.configs)
        betting_lines_container = processor.run_processor()

        return betting_lines_container