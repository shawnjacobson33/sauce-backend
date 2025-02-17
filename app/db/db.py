from motor.motor_asyncio import AsyncIOMotorClient

from app.db import collections

client = AsyncIOMotorClient('mongodb://localhost:27017')

database = client[f'sauce-dev']

betting_lines = collections.BettingLines(database)

teams = collections.Teams(database)

rosters = collections.Rosters(database)
