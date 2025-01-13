from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class Subjects(BaseCollection):
    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['subjects']

    async def get_subjects(self, query: dict) -> list[dict]:
        return await self.collection.find(query).to_list()

    async def get_subject(self, query: dict, proj: dict = None) -> dict:
        if proj is None:
            proj = {}

        return await self.collection.find_one(query, proj)

    async def store_subjects(self, subjects: list[dict]) -> None:
        requests = []
        for subject in subjects:
            query = { 'team.full_name': subject['team']['full_name'] }
            if await self.get_subject(query):
                update_op = await self.update_subject(query, return_op=True, **subject)
                requests.append(update_op)
            else:
                insert_op = InsertOne(subject)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_subject(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_subjects(self, query: dict) -> None:
        await self.collection.delete_many(query)