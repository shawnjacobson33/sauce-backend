from typing import Tuple
from pymongo import MongoClient
import os

DATABASE_URL = 'mongodb+srv://username:password@sauce.hvhxg.mongodb.net/?retryWrites=true&w=majority&appName=Sauce'


def get_db_creds() -> Tuple[str, str]:
    with open(os.path.abspath('../utils/db_creds.txt'), 'r') as f:
        data = f.readlines()
        return data[0].strip(), data[1]


def get_client():
    username, password = get_db_creds()
    return MongoClient(DATABASE_URL.replace('username', username).replace('password', password), uuidRepresentation='standard')


def get_db():
    return get_client()['sauce']
