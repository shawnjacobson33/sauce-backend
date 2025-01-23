from db import db
from pipelines.base.base_pipeline import BasePipeline, pipeline_logger


class GCSPipeline(BasePipeline):
    """
    A pipeline class for handling the storage of betting lines in Google Cloud Storage (GCS).

    Inherits from:
        BasePipeline: The base class for pipelines.
    """

    def __init__(self, configs: dict):
        """
        Initializes the GCSPipeline with the given parameters.

        Args:
            configs (dict): The configuration settings.
        """
        super().__init__('GCS', configs)

    async def _configure_pipeline(self):
        """
        Configures the pipeline. This method is currently a placeholder and does not perform any actions.
        """
        return

    @pipeline_logger
    async def run_pipeline(self):
        """
        Runs the GCS pipeline to store completed betting lines in GCS.

        Raises:
            Exception: If an error occurs during the pipeline execution.
        """
        try:
            await db.betting_lines.store_completed_betting_lines(in_gcs=True)

        except Exception as e:
            self.log_message(f'Failed to run pipeline: {e}', level='EXCEPTION')