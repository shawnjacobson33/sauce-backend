from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class Rosters(BaseCollection):
    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['rosters']

    async def get_rosters(self, query: dict) -> list[dict]:
        return await self.collection.find(query, { '_id': 0 }).to_list()

    async def get_roster(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    async def store_rosters(self, rosters: list[dict]) -> None:
        requests = []
        for roster in rosters:
            query = { 'team.full_name': roster['team']['full_name'] }
            if await self.get_roster(query):
                update_op = await self.update_roster(query, return_op=True, **roster)
                requests.append(update_op)
            else:
                insert_op = InsertOne(roster)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_roster(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_rosters(self, query: dict) -> None:
        await self.collection.delete_many(query)