
from app.db import db
from app.services.betting_lines.data_processing import processors



class BettingLinesDataProcessingManager:

    def __init__(self, configs: dict, betting_lines_container: list[dict]):
        self.configs = configs
        self.betting_lines_container = betting_lines_container

    async def _get_ev_formula(self):
        self.configs['ev_formulas']['secondary_markets']['formula'] = await db.metadata.get_ev_formula(
            'secondary_markets', self.configs['ev_formula']['secondary_markets']['name']
        )

    async def run_processors(self):  # Todo: Needs to be multi-processsed
        await self._get_ev_formula()

        processor = processors.BettingLinesProcessor(self.betting_lines_container, self.configs)
        betting_lines_container = processor.run_processor()

        return betting_lines_container