import os
from collections import defaultdict
from typing import Tuple
from pymongo import MongoClient

from app.database.utils import DATABASE_URL, DATABASE_NAME, BOOKMAKERS_COLLECTION_NAME


def get_db_creds() -> Tuple[str, str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.abspath(os.path.join(current_dir, 'security/db_creds.txt')), 'r') as f:
        data = f.readlines()
        return data[0].strip(), data[1]


def get_client():
    username, password = get_db_creds()
    client = MongoClient(DATABASE_URL.replace('username', username).replace('password', password),
                         uuidRepresentation='standard')
    return client


class Database:
    _db = get_client()[DATABASE_NAME]

    @classmethod
    def get(cls):
        return cls._db



def get_bookmaker(db, bookmaker: str) -> dict:
    return db[BOOKMAKERS_COLLECTION_NAME].find_one({'name': bookmaker})


def insert():
    db = get_client()[DATABASE_NAME]
    collection_name = 'markets-v4'
    collection = db[collection_name]

    collection.insert_one({
        'name': 'Average Yards Per Punt',
        'sport': 'Football'
    })

    # DELETING DUPLICATES
    attribute = 'sport' if 'markets' in collection_name else 'league'
    counter_dict = defaultdict(int)
    for doc in collection.find():
        if counter_dict[(doc[attribute], doc['name'])] > 0:
            collection.delete_one({'_id': doc['_id']})

        counter_dict[(doc[attribute], doc['name'])] += 1


# insert()

# db = get_client()[DATABASE_NAME]
# collection_name = 'teams-v1'
# collection = db[collection_name]
#
#
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
#
#
# collection.insert_many(docs)
#
#
# # DELETING DUPLICATES
# attribute = 'sport' if 'markets' in collection_name else 'league'
# counter_dict = defaultdict(int)
# for doc in collection.find():
#     if counter_dict[(doc[attribute], doc['abbr_name'])] > 0:
#         collection.delete_one({'_id': doc['_id']})
#
#     counter_dict[(doc[attribute], doc['abbr_name'])] += 1

