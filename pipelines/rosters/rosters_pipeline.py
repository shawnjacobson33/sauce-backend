from datetime import datetime

from db import db
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger
from pipelines.rosters.data_collection import RostersDataCollectionManager


class RostersPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('Rosters', configs)

        self.times = {}

    async def _configure_pipeline(self):
        if self.configs['reset']:
            await db.subjects.delete_subjects({})

    @pipeline_logger
    async def run_pipeline(self):
        batch_timestamp = datetime.now()

        rosters_dc_manager = RostersDataCollectionManager(self.configs['data_collection'])
        collected_rosters = await rosters_dc_manager.run_collectors(batch_timestamp)

        await db.subjects.store_subjects(collected_rosters)

