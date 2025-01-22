from motor.motor_asyncio import AsyncIOMotorDatabase


class BaseCollection:

    def __init__(self, collection_name: str, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = self.db[collection_name]
