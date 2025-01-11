from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class PipelineStats(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['pipeline_stats']

        self._daily_pipeline_stats = []

        self._betting_lines_stats = []
        self._betting_lines_batch_stats = {}

    def add_collector_stats(self, collector_name: str, stats: dict) -> None:
        data_collection_dict = self._betting_lines_batch_stats.setdefault('data_collection', {})
        data_collectors_dict = data_collection_dict.setdefault('collectors', {})
        data_collectors_dict[collector_name] = stats

    def add_processor_stats(self, processor_name: str, stats: dict):
        data_processing_dict = self._betting_lines_batch_stats.setdefault('data_processing', {})
        data_processing_dict[processor_name] = stats

    # def update_daily_stats(self):
    #     self._daily_pipeline_stats.append(self._batch_stats)
    #     self._batch_stats = {}
    #
    # async def store_daily_stats(self, curr_date: date) -> None:
    #     if self._daily_pipeline_stats:
    #         await self.collection.update_one(
    #             {'date': curr_date},
    #             {'$set': {'betting_lines_pipeline_stats': self._daily_pipeline_stats}},
    #             upsert=True
    #         )
    #         self._daily_pipeline_stats = []