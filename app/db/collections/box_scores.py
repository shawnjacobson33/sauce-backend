from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class BoxScores(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['box_scores']

    async def get_box_scores(self, query: dict) -> list[dict]:
        return await self.collection.find(query).to_list()

    async def get_box_score(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    async def store_box_scores(self, box_scores: list[dict]) -> None:
        requests = []
        for box_score in box_scores:
            query = { '_id': box_score['_id'] }
            if await self.get_box_score(query):
                update_op = await self.update_box_score(query, return_op=True, **box_score)
                requests.append(update_op)
            else:
                insert_op = InsertOne(box_score)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_box_score(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_box_scores(self, game_ids: list[str]) -> None:
        await self.collection.delete_many({'game._id': {'$in': game_ids}})
