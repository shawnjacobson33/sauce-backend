import asyncio
import time
from datetime import datetime

from app.db import db
from app.services.base import BasePipeline
from app.services.rosters.data_collection import RostersDataCollectionManager
from app.services.utils import utilities as utils


class RostersPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('Rosters', configs)

        self.times = {}

    @utils.logger.pipeline_logger(message='Running Pipeline')
    async def run_pipeline(self):
        if self.configs['reset']:
            await db.subjects.delete_subjects({})

        while True:
            batch_timestamp = datetime.now()
            start_time = time.time()
            
            rosters_dc_manager = RostersDataCollectionManager(self.configs['data_collection'])
            collected_rosters = await rosters_dc_manager.run_collectors(batch_timestamp)
            
            await db.subjects.store_subjects(collected_rosters)
            print(f'[Rosters]: Stored {len(collected_rosters)} collected rosters...')
            
            end_time = time.time()
            print(f'[Rosters]: Pipeline completed in {round(end_time - start_time, 2)} seconds. See you tomorrow...')
            await asyncio.sleep(60 * 60 * 24)


if __name__ == '__main__':
    asyncio.run(RostersPipeline(reset=True).run_pipeline())
