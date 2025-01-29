from motor.motor_asyncio import AsyncIOMotorClient

from db import collections


class Database:
    def __init__(self, client_name: str, database_name: str):
        self.client = AsyncIOMotorClient(client_name)

        self.database = self.client[database_name]

        self.metadata = collections.Metadata(self.database)

        self.pipeline_stats = collections.PipelineStats(self.database)

        self.betting_lines = collections.BettingLines(self.database)

        self.teams = collections.Teams(self.database)

        self.subjects = collections.Subjects(self.database)

        self.games = collections.Games(self.database)

        self.box_scores = collections.BoxScores(self.database)

        self.users = collections.Users(self.database)
