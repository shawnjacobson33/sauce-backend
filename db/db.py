from motor.motor_asyncio import AsyncIOMotorClient

from db import collections

client = AsyncIOMotorClient('mongodb://localhost:27017')

database = client['sauce-dev']

metadata = collections.Metadata(database)

pipeline_stats = collections.PipelineStats(database)

betting_lines = collections.BettingLines(database)

teams = collections.Teams(database)

subjects = collections.Subjects(database)

games = collections.Games(database)

box_scores = collections.BoxScores(database)
