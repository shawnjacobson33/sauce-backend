import json
from collections import defaultdict
from typing import Union

from app.database.db import get_client, DATABASE_NAME
from app.database.utils.definitions import SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME, TEAMS_COLLECTION_NAME


db = get_client()[DATABASE_NAME]


def delete_duplicates(collection, name: str, attribute: str):
    # DELETING DUPLICATES
    counter_dict = defaultdict(int)
    for doc in collection.find():
        if counter_dict[(doc[attribute], doc[name])] > 0:
            collection.delete_one({'_id': doc['_id']})

        counter_dict[(doc[attribute], doc[name])] += 1


def insert_subject(subject_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = db[SUBJECTS_COLLECTION_NAME]
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
    collection = db[MARKETS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_markets.json') as file:
            pending_markets = json.load(file)
            pending_markets = [market for bookmaker, markets in pending_markets.items() for market in markets if
                                (bookmaker not in exclude) and ((market['sport'], market['name'],) not in exclude)]
            collection.insert_many(pending_markets)
    else:
        collection.insert_one({
            'name': market_data['name'],
            'sport': market_data['sport']
        })

    delete_duplicates(collection, 'name', 'sport')


def insert_team(team_data: list[dict], insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = db[TEAMS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_teams.json') as file:
            pending_teams = json.load(file)
            pending_teams = [team for bookmaker, teams in pending_teams.items() for team in teams if
                             (bookmaker not in exclude) and ((team['league'], team['abbr_name'],) not in exclude)]
            collection.insert_many(pending_teams)
    else:
        collection.insert_many({
            team_data
        })

    delete_duplicates(collection, 'abbr_name', 'league')





# insert_subject()
# insert_market()
# insert_team()


# ncaaf_top_programs_abbr = [
#     'ALA', 'OSU', 'UGA', 'MICH', 'CLEM', 'TEX', 'LSU', 'OU', 'ND', 'PSU',
#     'USC', 'ORE', 'UF', 'TENN', 'AUB', 'WISC', 'MIA', 'FSU', 'MSU', 'IOWA',
#     'OKST', 'KY', 'UNC', 'MIZZ', 'ARIZ', 'TCU', 'KSU', 'SDSU', 'BAYL'
# ]
#
# ncaaf_top_programs_schools = [
#     'Alabama', 'Ohio State', 'Georgia', 'Michigan', 'Clemson', 'Texas',
#     'LSU', 'Oklahoma', 'Notre Dame', 'Penn State', 'USC', 'Oregon',
#     'Florida', 'Tennessee', 'Auburn', 'Wisconsin', 'Miami', 'Florida State',
#     'Michigan State', 'Iowa', 'Oklahoma State', 'Kentucky', 'North Carolina',
#     'Missouri', 'Arizona', 'TCU', 'Kansas State', 'San Diego State', 'Baylor'
# ]
#
#
# docs = list()
# for abbr, full in zip(ncaaf_top_programs_abbr, ncaaf_top_programs_schools):
#     doc = {
#         'abbr_name': abbr,
#         'full_name': full,
#         'league': 'NCAA'
#     }
#
#     docs.append(doc)