import json
from collections import defaultdict
from datetime import datetime
from typing import Union

from app.backend.database import GAMES_COLLECTION_NAME
from app.backend.database.db import get_db_session
from app.backend.database.utils.definitions import SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME, TEAMS_COLLECTION_NAME

MongoDB = get_db_session()


def delete_duplicates(collection, name: str, attribute: str):
    # DELETING DUPLICATES
    counter_dict = defaultdict(int)
    for doc in collection.find():
        if counter_dict[(doc[attribute], doc[name])] > 0:
            collection.delete_one({'_id': doc['_id']})

        counter_dict[(doc[attribute], doc[name])] += 1


def insert_subject(subject_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[SUBJECTS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_subjects.json') as file:
            pending_subjects = json.load(file)
            pending_subjects = [subject for bookmaker, subjects in pending_subjects.items() for subject in subjects if
                             (bookmaker not in exclude) and ((subject['league'], subject['name'],) not in exclude)]
            collection.insert_many(pending_subjects)
    else:
        collection.insert_one({
            'name': subject_data['name'],
            'league': subject_data['league'],
            'team': subject_data['team'],
            'position': subject_data['position'],
            'jersey_number': subject_data['jersey_number']
        })

    delete_duplicates(collection, 'name', 'league')


def insert_market(market_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[MARKETS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_markets.json') as file:
            pending_markets = json.load(file)
            pending_markets = [market for bookmaker, markets in pending_markets.items() for market in markets if
                                (bookmaker not in exclude) and ((market['sport'], market['name'],) not in exclude)]
            collection.insert_many(pending_markets)
    else:
        collection.insert_one(market_data)

    delete_duplicates(collection, 'name', 'sport')


def insert_team(team_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[TEAMS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_teams.json') as file:
            pending_teams = json.load(file)
            pending_teams = [team_obj for bookmaker, teams in pending_teams.items() for team_obj in teams if
                             (bookmaker not in exclude) and ((team_obj['league'], team_obj['abbr_name'],) not in exclude)]
            collection.insert_many(pending_teams)
    else:
        collection.insert_one(team_data)

    delete_duplicates(collection, 'abbr_name', 'league')


def insert_game(game_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[GAMES_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_teams.json') as file:
            pending_teams = json.load(file)
            pending_teams = [team_obj for bookmaker, teams in pending_teams.items() for team_obj in teams if
                             (bookmaker not in exclude) and ((team_obj['league'], team_obj['abbr_name'],) not in exclude)]
            collection.insert_many(pending_teams)
    else:
        game_data['game_time'] = datetime.strptime(game_data['game_time'], "%Y-%m-%d %H:%M:%S.%f")
        collection.insert_one(game_data)

    delete_duplicates(collection, 'time_processed', 'league')




# insert_subject()
# market = {
#     'name': 'Plus Minus',
#     'sport': 'Ice Hockey'
# }
# insert_market(
#     market_data=market
# )
# team = {
#     'abbr_name': 'MISS',
#     'full_name': "Ole Miss",
#     'league': 'NCAA'
# }
# insert_team(
#     team_data=team
# )
# NBA
# game = {
#         "home_team": {
#             "abbr_name": "MIL",
#             "id": "673a28bc816e0299a527379c"
#         },
#         "away_team": {
#             "abbr_name": "CHI",
#             "id": "673a28bc816e0299a5273798"
#         },
#         "game_time": "2024-11-19 19:00:00.00",
#         "box_score_url": "NBA_20241120_CHI@MIL",
#         "league": "NBA",
#         "source": "cbssports-nba",
#         "time_processed": "2024-11-19 08:58:13.284000"
#     }
# NHL
# game = {
#         "home_team": {
#             "abbr_name": "PHI",
#             "id": "673a28bc816e0299a527379c"
#         },
#         "away_team": {
#             "abbr_name": "CAR",
#             "id": "673a28bc816e0299a5273798"
#         },
#         "game_time": "2024-11-20 19:00:00.00",
#         "box_score_url": "NHL_20241120_CAR@PHI",
#         "league": "NHL",
#         "source": "cbssports-nhl",
#         "time_processed": "2024-11-19 08:58:13.284000"
#     }
# NFL
game = {
        "home_team": {
            "abbr_name": "ILL",
            "id": "673a28ddee7309a8917b7a26"
        },
        "away_team": {
            "abbr_name": "BAMA",
            "id": "673a28ddee7309a8917b7a23"
        },
        "game_time": "2024-11-20 19:00:00.00",
        "box_score_url": "NCAAB_20241120_ILL@BAMA",
        "league": "NCAAM",
        "source": "cbssports-ncaam",
        "time_processed": "2024-11-19 08:58:13.284000"
    }

insert_game(
    game_data=game
)
