import dotenv
import os
import redis

from app.data_storage.client import Client
from app.data_storage import stores as st
from app.database import TEAMS_COLLECTION_NAME
from app.database.mongo import MongoDB


dotenv.load_dotenv('../../.env')


class Redis:
    def __init__(self, db: str = 'prod'):
        self.client = redis.Redis(db=0 if 'prod' else 1, password=os.getenv('REDIS_PASSWORD'))
        # Structures
        self.markets = st.Markets(self.client)
        self.positions = st.Positions(self.client)
        self.teams = st.Teams(self.client)
        self.bookmakers = st.Bookmakers(self.client)
        self.games = st.Games(self.client)
        self.subjects = st.Subjects(self.client)
        self.betting_lines = st.BettingLines(self.client)
        self.box_scores = st.BoxScores(self.client)

    # TODO: At some point create tester methods with dummy data

ri = Redis()
