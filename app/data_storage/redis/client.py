import redis

import app.data_storage.redis.structures as strcs
from app.data_storage import MARKETS_COLLECTION_NAME
from app.data_storage.mongodb import MongoDB


class Redis:
    __r = redis.Redis(host='localhost', port=6379)
    # Structures
    markets = strcs.Markets(__r)
    teams = strcs.Teams(__r)
    positions = strcs.Positions(__r)
    bookmakers = strcs.Bookmakers(__r)
    games = strcs.Games(__r)
    subjects = strcs.Subjects(__r)
    betting_lines = strcs.BettingLines(__r)
    box_scores = strcs.BoxScores(__r)


for doc in MongoDB.fetch_collection(MARKETS_COLLECTION_NAME).find():
    print(doc)
