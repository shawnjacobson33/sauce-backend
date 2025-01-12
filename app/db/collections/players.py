from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class Players(BaseCollection):
    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['players']

    async def get_players(self, query: dict) -> list[dict]:
        return await self.collection.find(query).to_list()

    async def get_player(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    async def store_players(self, players: list[dict]) -> None:
        requests = []
        for player in players:
            query = { 'team.full_name': player['team']['full_name'] }
            if await self.get_player(query):
                update_op = await self.update_player(query, return_op=True, **player)
                requests.append(update_op)
            else:
                insert_op = InsertOne(player)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_player(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_players(self, query: dict) -> None:
        await self.collection.delete_many(query)