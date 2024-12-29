import dotenv
import os
import redis

from app.data_storage import stores


dotenv.load_dotenv('../../.env')


class Redis:
    def __init__(self, db: str = 'prod'):
        self.client = redis.Redis(db=0 if db == 'prod' else 1, password=os.getenv('REDIS_PASSWORD'))
        self.markets = stores.Markets(self.client)
        self.positions = stores.Positions(self.client)
        self.teams = stores.Teams(self.client)
        self.bookmakers = stores.Bookmakers(self.client)
        self.games = stores.Games(self.client)
        self.subjects = stores.Subjects(self.client)
        self.lines = stores.BettingLines(self.client)
        self.box_scores = stores.BoxScores(self.client)


Redis = Redis()
