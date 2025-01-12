from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class Boxscores(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['boxscores']

    async def get_boxscores(self, query: dict) -> list[dict]:
        return await self.collection.find(query).to_list()

    async def get_boxscore(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    async def store_boxscores(self, boxscores: list[dict]) -> None:
        requests = []
        for boxscore in boxscores:
            query = { '_id': boxscore['_id'] }
            if await self.get_boxscore(query):
                update_op = await self.update_boxscore(query, return_op=True, **boxscore)
                requests.append(update_op)
            else:
                insert_op = InsertOne(boxscore)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_boxscore(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_boxscores(self, query: dict) -> None:
        await self.collection.delete_many(query)

