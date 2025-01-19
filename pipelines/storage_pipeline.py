
from db import db
from pipelines.pipeline_base import BasePipeline, logger


class GCSPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('GCS', configs)

    async def _configure_pipeline(self):
        return

    @logger
    async def run_pipeline(self):
        await db.betting_lines.store_completed_betting_lines(in_gcs=True)
