from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class Metadata(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['metadata']

    async def get_ev_formula(self, market_domain: str, ev_formula_name: str) -> str | None:
        return await self.collection.find_one({'_id': 'metadata'}, {f'ev_algos.{market_domain}.{ev_formula_name}': 1})