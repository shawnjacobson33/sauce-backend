from datetime import date

from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base_collection import BaseCollection


class PipelineStats(BaseCollection):
    """
    A class to manage pipeline statistics in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The pipeline stats collection.
        _daily_pipeline_stats (list): A list to store daily pipeline statistics.
        _betting_lines_batch_stats (dict): A dictionary to store betting lines batch statistics.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the PipelineStats class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__('pipeline_stats', db)

        self._daily_pipeline_stats = []
        self._betting_lines_batch_stats = {}

    def update_batch_details(self, batch_timestamp: date) -> None:
        """
        Updates the batch details with the given timestamp.

        Args:
            batch_timestamp (date): The timestamp of the batch.
        """
        self._betting_lines_batch_stats['batch_timestamp'] = batch_timestamp

    def add_collector_stats(self, collector_name: str, stats: dict) -> None:
        """
        Adds collector statistics to the batch stats.

        Args:
            collector_name (str): The name of the collector.
            stats (dict): The statistics of the collector.
        """
        data_collection_dict = self._betting_lines_batch_stats.setdefault('data_collection', {})
        data_collectors_dict = data_collection_dict.setdefault('collectors', {})
        data_collectors_dict[collector_name] = stats

    def add_processor_stats(self, stats: dict):
        """
        Adds processor statistics to the batch stats.

        Args:
            stats (dict): The statistics of the processor.
        """
        self._betting_lines_batch_stats['data_processing'] = stats

    def add_pipeline_stats(self, stats: dict):
        """
        Adds pipeline statistics to the batch stats.

        Args:
            stats (dict): The statistics of the pipeline.
        """
        self._betting_lines_batch_stats['pipeline'] = stats

    async def update_daily_stats(self, curr_date: date):
        """
        Updates the daily statistics in the database.

        Args:
            curr_date (date): The current date.
        """
        await self.collection.update_one(
            { 'date': curr_date },
            { '$push': { 'betting_lines': self._betting_lines_batch_stats } },
            upsert=True
        )
        self._betting_lines_batch_stats = {}