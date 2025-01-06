from typing import Iterable

from motor.motor_asyncio import AsyncIOMotorClient


class BettingLines:
    def __init__(self, client: AsyncIOMotorClient):
        self.client = client
        self.db = client['sauce-dev']
        self.collection = self.db['betting_lines']

    async def get_betting_lines(self) -> Iterable:
        async for line in self.collection.find():
            yield line

    async def store_betting_lines(self, betting_lines: Iterable):
        await self.collection.delete_many({})  # Todo: this is a temporary solution
        await self.collection.insert_many(betting_lines)
