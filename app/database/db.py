import os
from typing import Tuple, Optional
from pymongo import MongoClient

from app.database.utils import DATABASE_URL, DATABASE_NAME, SOURCES_COLLECTION_NAME


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


class MongoDB:
    _db = get_client()[DATABASE_NAME]

    @classmethod
    def get_session(cls):
        return cls._db


def get_source(db, source_name: str) -> Optional[dict]:
    if source := db[SOURCES_COLLECTION_NAME].find_one({'name': source_name}):
        return source


