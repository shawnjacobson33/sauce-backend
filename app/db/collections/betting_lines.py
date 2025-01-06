from typing import Iterable

from motor.motor_asyncio import AsyncIOMotorClient


class BettingLines:
    def __init__(self, client: AsyncIOMotorClient):
        self.client = client
        self.db = client['sauce-dev']
        self.collection = self.db['betting_lines']

    async def get_betting_lines(self) -> list[dict]:
        return await self.collection.find({}, { '_id': 0 }).to_list()

    async def store_betting_lines(self, betting_lines: list[dict]):
        await self.collection.insert_many(betting_lines)

    async def delete_betting_lines(self):
        await self.collection.delete_many({})
