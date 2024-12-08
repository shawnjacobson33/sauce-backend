import redis

import app.data_storage.redis.structures as strcs
from app.database import MARKETS_COLLECTION_NAME
from app.database.mongo import MongoDB


class Redis:
    client = redis.Redis(host='localhost', port=6379)
    # Structures
    markets = strcs.Markets(client)
    teams = strcs.Teams(client)
    positions = strcs.Positions(client)
    bookmakers = strcs.Bookmakers(client)
    games = strcs.Games(client)
    subjects = strcs.Subjects(client)
    betting_lines = strcs.BettingLines(client)
    box_scores = strcs.BoxScores(client)


Redis.subjects.store('PrizePicks', '1.58')
print(Redis.bookmakers.get('PrizePicks'))
# print(Redis.positions.get_unidentified())
# # print(Redis.client.delete(b'teams:auto:id', b'teams:std:NBA', b'teams:lookup:NBA', 'team:None'))
# print(Redis.client.flushdb())
print(Redis.client.keys('*'))
# print(Redis.client.delete('bookmakers:PrizePicks'))