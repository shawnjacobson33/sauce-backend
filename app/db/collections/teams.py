from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne, InsertOne


class Teams:
    def __init__(self, client: AsyncIOMotorClient, db: str = 'dev'):
        self.client = client
        self.db = client[f'sauce-{db}']
        self.collection = self.db['teams']

    async def get_teams(self, query: dict) -> list[dict]:
        return await self.collection.find(query).to_list()

    async def get_team(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    async def store_teams(self, teams: list[dict]) -> None:
        requests = []
        for team in teams:
            query = {'full_name': team['full_name']}
            if await self.get_team(query):
                update_op = await self.update_team(query, return_op=True, **team)
                requests.append(update_op)
            else:
                insert_op = InsertOne(team)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_team(self, query: dict, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_teams(self, query: dict) -> None:
        await self.collection.delete_many(query)