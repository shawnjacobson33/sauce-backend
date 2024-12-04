import json
import os
from collections import deque
from datetime import datetime
from typing import Tuple, Optional
from pymongo import MongoClient

from backend.app.database.configs import DATABASE_URL, DATABASE_NAME, SOURCES_COLLECTION_NAME, GAMES_COLLECTION_NAME


def get_db_creds() -> Tuple[str, str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.abspath(os.path.join(current_dir, '../configs/db_creds.txt')), 'r') as f:
        data = f.readlines()
        return data[0].strip(), data[1]


def get_db_session():
    username, password = get_db_creds()
    client = MongoClient(DATABASE_URL.replace('username', username).replace('password', password),
                         uuidRepresentation='standard')

    return client[DATABASE_NAME]


class MongoDB:
    _db = get_db_session()

    @classmethod
    def fetch_collection(cls, collection_name: str):
        if collection_name in cls._db.list_collection_names():
            return cls._db[collection_name]
        else:
            raise ValueError(f"Collection '{collection_name}' does not exist.")

    @classmethod
    def fetch_source(cls, source_name: str) -> Optional[dict]:
        # get a data source by name
        if source := cls.fetch_collection(SOURCES_COLLECTION_NAME).find_one({'name': source_name}):
            return source

    @classmethod
    def fetch_started_games(cls):
        # get the games collection
        games = cls.fetch_collection(GAMES_COLLECTION_NAME)
        # Step 1: Find the games that will be deleted
        if started_games := list(games.find({'game_time': {'$lt': datetime.now()}})):
            # delete any games that already occurred
            # games.delete_many({'game_time': {'$lt': datetime.now()}})

            # with open('del_games.json', 'w') as f:
            #     json.dump(started_games, f, indent=4, default=str)

            # return the started games data
            return started_games