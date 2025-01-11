from datetime import date
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class BettingLinesPipelineStats(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['pipeline_stats']

        self._daily_pipeline_stats = []
        self._batch_stats = {}

    def add_batch_stat(self, stat_type: str, stat_name: str, stat: Any) -> None:
        if stat_type_dict := self._batch_stats.get(stat_type):
            stat_type_dict[stat_name] = stat

        raise KeyError(f'Invalid stat type: {stat_type}')

    def update_daily_stats(self):
        self._daily_pipeline_stats.append(self._batch_stats)
        self._batch_stats = {}

    async def store_daily_stats(self, curr_date: date) -> None:
        if self._daily_pipeline_stats:
            await self.collection.update_one(
                {'date': curr_date},
                {'$set': {'betting_lines_pipeline_stats': self._daily_pipeline_stats}},
                upsert=True
            )
            self._daily_pipeline_stats = []