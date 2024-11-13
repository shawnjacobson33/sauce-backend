import threading
from collections import defaultdict

from app.backend import database as db
from app.backend.data_collection.utils.definitions import IN_SEASON_SPORTS


def get_structured_docs(docs: list[dict]) -> dict:
    # this is the default which is used by markets
    return {doc['name']: doc['_id'] for doc in docs}


def structure_data() -> dict:
    # get collection being used
    markets_cursor = db.MongoDB.fetch_collection(db.MARKETS_COLLECTION_NAME)
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = dict()
    # for each partition in the partitions predicated upon the cursor name
    for partition in IN_SEASON_SPORTS:
        # filter by league or sport and don't include the batch_id
        filtered_docs = markets_cursor.find({'sport': partition})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_data[partition] = get_structured_docs(filtered_docs)

    # return the fully structured and partitioned data
    return partitioned_data


def restructure_sets(data: dict) -> dict:
    restructured_data = dict()
    for key, values in data.items():
        subjects = list()
        for value in values:
            value_dict = dict()
            for attribute in value:
                value_dict[attribute[0]] = attribute[1]

            subjects.append(value_dict)

        restructured_data[key] = subjects

    return restructured_data


class Markets:
    _stored_data: dict = structure_data()  # Unique to Markets
    _valid_data: dict = defaultdict(set)
    _pending_data: dict = defaultdict(set)  # Hold data that needs to be evaluated manually before db insertion
    _lock1 = threading.Lock()
    _lock2 = threading.Lock()
    _lock3 = threading.Lock()

    @classmethod
    def get_stored_data(cls):
        return cls._stored_data

    @classmethod
    def get_pending_data(cls) -> dict:
        return restructure_sets(cls._pending_data)

    @classmethod
    def get_valid_data(cls) -> dict:
        return restructure_sets(cls._valid_data)

    @classmethod
    def update_stored_data(cls, key, value):
        with cls._lock1:
            cls._stored_data[key] = value

    @classmethod
    def update_pending_data(cls, key: str, data: tuple):
        with cls._lock2:
            cls._pending_data[key].add(data)

    @classmethod
    def update_valid_data(cls, key: str, data: tuple):
        with cls._lock3:
            cls._valid_data[key].add(data)