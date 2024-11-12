import os
from datetime import datetime
from typing import Tuple, Optional
from pymongo import MongoClient

from app.database.utils import DATABASE_URL, DATABASE_NAME, SOURCES_COLLECTION_NAME, GAMES_COLLECTION_NAME


def get_db_creds() -> Tuple[str, str]:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.abspath(os.path.join(current_dir, 'security/db_creds.txt')), 'r') as f:
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
        if source := cls.fetch_collection(SOURCES_COLLECTION_NAME).find_one({'name': source_name}):
            return source

    @classmethod
    def evict_completed_games(cls):
        # get the games collection
        games = cls.fetch_collection(GAMES_COLLECTION_NAME)
        # delete any games that already occurred
        games.delete_many({'game_time': {'$lt': datetime.now()}})