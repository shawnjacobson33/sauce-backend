import asyncio
import time
from datetime import datetime

from db import db
from pipelines.pipeline_base import BasePipeline, logger
from pipelines.rosters.data_collection import RostersDataCollectionManager
from pipelines.utils import utilities as utils


class RostersPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('Rosters', configs)

        self.times = {}

    async def _configure_pipeline(self):
        if self.configs['reset']:
            await db.subjects.delete_subjects({})

    @logger
    async def run_pipeline(self):
        batch_timestamp = datetime.now()

        rosters_dc_manager = RostersDataCollectionManager(self.configs['data_collection'])
        collected_rosters = await rosters_dc_manager.run_collectors(batch_timestamp)

        await db.subjects.store_subjects(collected_rosters)

