from motor.motor_asyncio import AsyncIOMotorClient

from app.db.collections import BettingLines


client = AsyncIOMotorClient('mongodb://db:27017')

betting_lines = BettingLines(client)
