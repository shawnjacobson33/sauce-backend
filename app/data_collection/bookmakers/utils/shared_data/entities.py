import threading
from collections import defaultdict

from app import database as db
from app.data_collection.bookmakers.utils.cleaning import clean_subject
from app.data_collection.utils.definitions import IN_SEASON_LEAGUES, IN_SEASON_SPORTS

# some constants to interact with the database
DB = db.Database.get()
SUBJECTS_CURSOR = DB[db.SUBJECTS_COLLECTION_NAME]
MARKETS_CURSOR = DB[db.MARKETS_COLLECTION_NAME]


def get_structured_docs(docs: list[dict], is_markets: bool) -> dict:
    if not is_markets:
        # re-format the name so it can be more easily matched with bookmaker's subject names
        return {clean_subject(doc['name']): {'name': doc['name'], 'id': doc['_id']} for doc in docs}

    # otherwise return name and id dictionary
    return {doc['name']: doc['_id'] for doc in docs}


def structure_data(cursor) -> dict:
    # get collection being used
    is_markets = (cursor.name.split('-')[0] == 'markets')
    # get partitions to ensure valid keys (if there was a LeBron James in the NBA and MLB)
    partitions = IN_SEASON_SPORTS if is_markets else IN_SEASON_LEAGUES  # Markets use sports and Subjects use leagues
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = dict()
    # for each partition in the partitions predicated upon the cursor name
    for partition in partitions:
        # filter by league or sport and don't include the batch_id
        filtered_docs = cursor.find({f'{"sport" if is_markets else "league"}': partition})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_data[partition] = get_structured_docs(filtered_docs, is_markets)

    # return the fully structured and partitioned data
    return partitioned_data


class Subjects:
    _subjects_dict: dict = structure_data(SUBJECTS_CURSOR)  # Dictionary is much faster than any other data structure.
    _subjects_pending: dict = defaultdict(set)  # Hold data that needs to be evaluated manually before db insertion
    _lock = threading.Lock()

    @classmethod
    def get_dict(cls):
        return cls._subjects_dict

    @classmethod
    def get_pending_data(cls) -> dict:
        restructured_data = dict()
        for key, values in cls._subjects_pending.items():
            subjects = list()
            for value in values:
                value_dict = dict()
                for attribute in value:
                    value_dict[attribute[0]] = attribute[1]

                subjects.append(value_dict)

            restructured_data[key] = subjects

        return restructured_data

    @classmethod
    def update_dict(cls, key, value):
        with cls._lock:
            cls._subjects_dict[key] = value

    @classmethod
    def add_pending_data(cls, key: str, data: tuple):
        with cls._lock:
            cls._subjects_pending[key].add(data)


class Markets:
    _markets_dict: dict = structure_data(MARKETS_CURSOR)  # Dictionary is much faster than any other data structure.
    _markets_pending: dict = defaultdict(set)  # Hold data that needs to be evaluated manually before db insertion
    _lock = threading.Lock()

    @classmethod
    def get_dict(cls) -> dict:
        return cls._markets_dict

    @classmethod
    def get_pending_data(cls) -> dict:
        # converting sets to dictionaries, so it can be serialized into json
        restructured_data = dict()
        for key, values in cls._markets_pending.items():
            subjects = list()
            for value in values:
                value_dict = dict()
                for attribute in value:
                    value_dict[attribute[0]] = attribute[1]

                subjects.append(value_dict)

            restructured_data[key] = subjects

        return restructured_data

    @classmethod
    def update_dict(cls, key, value):
        with cls._lock:
            cls._markets_dict[key] = value

    @classmethod
    def add_pending_data(cls, key: str, data: tuple):
        with cls._lock:
            cls._markets_pending[key].add(data)