from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base_collection import BaseCollection


class Users(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__('users', db)

    async def get_user(self, **kwargs) -> dict | None:
        return await self.collection.find_one(kwargs)

    async def update_user(self, **kwargs) -> int:
        return await self.collection.update_one(kwargs)

    async def store_user(self, **kwargs) -> int:
        return await self.collection.insert_one(kwargs)

    async def delete_user(self, **kwargs) -> int:
        return await self.collection.delete_one(kwargs)

    async def is_username_valid(self, username: str) -> bool:
        if await self.collection.find_one({ 'username': username }):
            return True

        return False