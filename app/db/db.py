from motor.motor_asyncio import AsyncIOMotorClient

from app.db.collections import BettingLines, Teams

client = AsyncIOMotorClient('mongodb://localhost:27017')

betting_lines = BettingLines(client, 'dev')

teams = Teams(client, 'dev')
