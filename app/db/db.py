from motor.motor_asyncio import AsyncIOMotorClient

from app.db import collections

client = AsyncIOMotorClient('mongodb://localhost:27017')

database = client[f'sauce-dev']

metadata = collections.Metadata(database)

pipeline_stats = collections.PipelineStats(database)

betting_lines = collections.BettingLines(database)

teams = collections.Teams(database)

players = collections.Players(database)

games = collections.Games(database)

boxscores = collections.Boxscores(database)




