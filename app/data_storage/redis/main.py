from collections import namedtuple

from app.data_storage.redis.client import Client
import app.data_storage.redis.structures as strcs
from app.database import TEAMS_COLLECTION_NAME
from app.database.mongo import MongoDB


class Redis:
    client = Client()
    # Structures
    markets = strcs.Markets(client.r)
    teams = strcs.Teams(client.r)
    positions = strcs.Positions(client.r)
    bookmakers = strcs.Bookmakers(client.r)
    games = strcs.Games(client.r)
    subjects = strcs.Subjects(client.r)
    betting_lines = strcs.BettingLines(client.r)
    box_scores = strcs.BoxScores(client.r)

    # TODO: At some point create tester methods with dummy data



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
# teams = {Team(name=doc['abbr_name'], league=doc['league'], std_name=doc['abbr_name'], full_name=doc['full_name']) \
#     for doc in MongoDB.fetch_collection(TEAMS_COLLECTION_NAME).find()}

# Redis.teams.store(teams)
# print(sorted(Redis.client.r.keys('team:*'), key=lambda x: int(x.decode().split(':')[1])))
# nba_teams = Redis.teams.getall('NBA')
# print(len(nba_teams))
# print(Redis.teams.get('NBA', 'MIN'))

print(len({key: val for key, val in item for item in Redis.teams.getall('NBA')])
# Redis.client.r.delete(*subject_keys)