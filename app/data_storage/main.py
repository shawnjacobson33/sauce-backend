from collections import namedtuple

import redis

from app.data_storage.client import Client
from app.data_storage import stores as st
from app.database import TEAMS_COLLECTION_NAME
from app.database.mongo import MongoDB


NAMESPACE_TEMPLATE = {
    'std': '{}:std:{}',
    'name': '{}:noid',
}


class Redis:
    def __init__(self):
        self.client = Client()
        # Structures
        self.markets = st.Markets(self.client.r)
        self.positions = st.Positions(self.client.r)
        self.teams = st.Teams(self.client.r)
        self.bookmakers = st.Bookmakers(self.client.r)
        self.games = st.Games(self.client.r)
        self.subjects = st.Subjects(self.client.r)
        self.betting_lines = st.BettingLines(self.client.r)
        self.box_scores = st.BoxScores(self.client.r)

    # TODO: At some point create tester methods with dummy data

ri = Redis()
