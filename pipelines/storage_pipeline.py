
from db import db
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger

class GCSPipeline(BasePipeline):

    def __init__(self, configs: dict):
        super().__init__('GCS', configs)

    async def _configure_pipeline(self):
        return

    @pipeline_logger
    async def run_pipeline(self):
        await db.betting_lines.store_completed_betting_lines(in_gcs=True)
