import dotenv
import os
import redis

from app.cache import stores


dotenv.load_dotenv('../../.env')


# password=os.getenv('REDIS_PASSWORD')

class RedisCache:
    def __init__(self, db: str = 'prod'):
        self.client = redis.Redis(db=0 if db == 'prod' else 1)
        self.data_providers = stores.DataProviders(self.client)
        self.leagues = stores.Leagues(self.client)
        self.markets = stores.Markets(self.client)
        self.positions = stores.Positions(self.client)
        self.teams = stores.Teams(self.client)
        self.bookmakers = stores.Bookmakers(self.client)
        self.games = stores.Games(self.client)
        self.subjects = stores.Subjects(self.client)
        self.lines = stores.BettingLines(self.client)
        self.box_scores = stores.BoxScores(self.client)


redis_cache = RedisCache('prod')

# redis_cache.leagues.storeleagues([
#     stores.League('NBA', 'NBA'),
#     stores.League('NCAAM', 'NCAAM'),
#     stores.League('NCAAW', 'NCAAW'),
#     stores.League('NCAAF', 'NCAAF'),
#     stores.League('NCAAFB', 'NCAAF'),
#     stores.League('NFL', 'NFL'),
#     stores.League('MLB', 'MLB'),
#     stores.League('NHL', 'NHL'),
# ])