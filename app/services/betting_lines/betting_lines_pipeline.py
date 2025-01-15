import asyncio
import time
from datetime import datetime

from app.db import db
from app.services.base import BasePipeline
from app.services.utils import utilities as utils, Standardizer
from app.services.betting_lines.data_collection import BettingLinesDataCollectionManager
from app.services.betting_lines.data_processing import BettingLinesDataProcessingManager


class BettingLinesPipeline(BasePipeline):

    def __init__(self, configs: dict, standardizer: Standardizer):
        super().__init__('BettingLines', configs)
        self.configs = configs
        self.standardizer = standardizer

        self.times = {}

    async def _store_betting_lines(self, betting_lines: list[dict]):
        print('[BettingLinesPipeline]: Storing processed betting lines...')
        start_time = time.time()
        await db.betting_lines.store_betting_lines(betting_lines)
        end_time = time.time()
        print(f'[BettingLinesPipeline]: Stored {len(betting_lines)} processed betting lines...')
        self.times['storage_time'] = round(end_time - start_time, 2)

    @utils.logger.pipeline_logger(message='Running Pipeline')
    async def run_pipeline(self):
        if self.configs['reset']:
            await db.database['betting_lines'].delete_many({})
            await db.database['pipeline_stats'].delete_many({})


        while True:
            batch_timestamp = datetime.now()
            db.pipeline_stats.update_batch_details(batch_timestamp)

            betting_lines_dc_manager = BettingLinesDataCollectionManager(self.configs['data_collection'], self.standardizer)
            betting_lines_container = await betting_lines_dc_manager.run_collectors(batch_timestamp)  # Todo: need to collect game markets also

            betting_lines_prc_manager = BettingLinesDataProcessingManager(self.configs['data_processing'], betting_lines_container)
            betting_lines_container = await betting_lines_prc_manager.run_processors()

            await self._store_betting_lines(betting_lines_container)
            await db.pipeline_stats.update_daily_stats(datetime.today())

            sleep_time = 120
            print(f'[BettingLinesPipeline]: Iteration complete! Sleeping for {sleep_time} seconds...')
            await asyncio.sleep(sleep_time)
