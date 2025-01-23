from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base_collection import BaseCollection


class Users(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__('users', db)

    async def get_user(self):
        pass

    async def update_user(self):
        pass

    async def store_user(self):
        pass

    async def delete_user(self):
        pass
