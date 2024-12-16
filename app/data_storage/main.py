from collections import namedtuple

import redis

from app.data_storage.client import Client
from app.data_storage import stores as st
from app.database import TEAMS_COLLECTION_NAME
from app.database.mongo import MongoDB


class Redis:
    def __init__(self):
        self.client = Client()
        # Structures
        self.markets = st.L1StaticDataStore(self.client.r, 'markets')
        self.positions = st.L1StaticDataStore(self.client.r, 'positions')
        self.teams = st.Teams(self.client.r)
        self.bookmakers = st.Bookmakers(self.client.r)
        self.games = st.Games(self.client.r, self.teams)
        self.subjects = st.Subjects(self.client.r)
        self.betting_lines = st.BettingLines(self.client.r)
        self.box_scores = st.BoxScores(self.client.r)

    # TODO: At some point create tester methods with dummy data








# data = len(redis_instance.teams.getall())
# print(data)
# from collections import namedtuple
# Subject = namedtuple('subject', ['name', 'std_name', 'league', 'team', 'pos', 'jersey_num'])
# subject = Subject(name='Decobie Durant', std_name='Cobie Durant', league='NFL', team='LA', pos='CB', jersey_num='5')
# # Redis.subjects.store(subject)
# print(Redis.subjects.get('NFL', 'CB', 'Cobie Durant'))
# # # # print(Redis.client.flushdb())
# #
# # # print(Redis.client.delete('subject:2'))
# # print(Redis.client.hgetall('subject:4'))
# # print(Redis.subjects.rollback(subject))
# # print(Redis.client.r.hgetall('subject:3'))
# # print(Redis.client.r.get('subjects:auto:id'))
# # print(Redis.client.hgetall('subject:4'))
# # print(Redis.client.hgetall('subject:2'))
# # Redis.client.r.delete('subject:1')
# # Redis.client.reset_auto_ids('subjects')


# league: str, team: str, std_team: str, full_team: str

# Team = namedtuple('team', ['name', 'league', 'std_name', 'full_name'])
# # team = Team(name=)
# # print(Redis.teams.get('NBA', 'DAL'))
# M_teams = {Team(name=doc['abbr_name'], league=doc['league'], std_name=doc['abbr_name'], full_name=doc['full_name']) \
#     for doc in MongoDB.fetch_collection(TEAMS_COLLECTION_NAME).find()}
#
# Redis.teams.store(M_teams)
# print(sorted(Redis.client.r.keys('team:*'), key=lambda x: int(x.decode().split(':')[1])))
# nba_teams = Redis.teams.getall('NBA')
# print(len(nba_teams))
# print(Redis.teams.get('NBA', 'MIN'))

# Redis.client.r.flushdb()
# print(Redis.teams.getid('NBA', 'BOS'))
# print(Redis.r.keys('*'))
# Redis.client.r.delete(*subject_keys)