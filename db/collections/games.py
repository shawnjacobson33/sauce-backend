from datetime import datetime

from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base import BaseCollection


class Games(BaseCollection):
    # Todo: games should be sorted by game_time ascending
    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['games']

    async def get_games(self, query: dict, live: bool = False) -> list[dict]:
        if live:
            live_query = {
                **query,
                '$or': [
                    {'status': 'live'},
                    {'game_time': {'$lte': datetime.now()}}
                ]
            }
            return await self.collection.find(live_query).to_list()

        return await self.collection.find(query).to_list()

    async def get_game(self, query: dict, proj: dict = None) -> dict:
        return await self.collection.find_one(query, proj if proj else {})

    async def store_games(self, games: list[dict]) -> None:
        requests = []
        for game in games:
            query = { '_id': game['_id'] }
            if await self.get_game(query):
                update_op = await self.update_game(query, return_op=True, **game)
                requests.append(update_op)
            else:
                insert_op = InsertOne(game)
                requests.append(insert_op)

        if requests:
            await self.collection.bulk_write(requests)

    async def update_game(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_games(self, game_ids: list[str] = None) -> None:
        if game_ids:
            return await self.collection.delete_many({'_id': {'$in': game_ids}})

        await self.collection.delete_many({})
