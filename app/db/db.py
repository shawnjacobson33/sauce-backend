from motor.motor_asyncio import AsyncIOMotorClient

from app.db import collections

client = AsyncIOMotorClient('mongodb://localhost:27017')

database = client[f'sauce-dev']

metadata = collections.Metadata(database)

betting_lines_pipeline_stats = collections.BettingLinesPipelineStats(database)

betting_lines = collections.BettingLines(database)

teams = collections.Teams(database)

rosters = collections.Rosters(database)


