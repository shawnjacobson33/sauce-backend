from datetime import date

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class PipelineStats(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['pipeline_stats']

        self._daily_pipeline_stats = []
        self._betting_lines_batch_stats = {}

    def update_batch_details(self, batch_timestamp: date) -> None:
        self._betting_lines_batch_stats['batch_timestamp'] = batch_timestamp

    def add_collector_stats(self, collector_name: str, stats: dict) -> None:
        data_collection_dict = self._betting_lines_batch_stats.setdefault('data_collection', {})
        data_collectors_dict = data_collection_dict.setdefault('collectors', {})
        data_collectors_dict[collector_name] = stats

    def add_processor_stats(self, stats: dict):
        self._betting_lines_batch_stats['data_processing'] = stats

    def add_pipeline_stats(self, stats: dict):
        self._betting_lines_batch_stats['pipeline'] = stats

    async def update_daily_stats(self, curr_date: date):
        await self.collection.update_one(
            { 'date': curr_date },
            { '$push': { 'betting_lines': self._betting_lines_batch_stats } },
            upsert=True
        )
        self._betting_lines_batch_stats = {}
