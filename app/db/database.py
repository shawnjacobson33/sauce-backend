import os
from typing import Tuple
from pymongo import MongoClient

from app.db.utils.constants import DATABASE_NAME, DATABASE_URL


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
