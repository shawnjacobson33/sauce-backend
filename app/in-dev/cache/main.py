import dotenv
import os
import redis
from pathlib import Path

from app.cache import stores

base_dir = Path(os.getenv('BASE_DIR', Path.home()))

dotenv.load_dotenv(base_dir / ".env")


class RedisCache:
    def __init__(self, db: str = 'prod'):
        self.client = redis.Redis(db=0 if db == 'prod' else 1, password=os.getenv('REDIS_PASSWORD'))
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