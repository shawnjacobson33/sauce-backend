import threading
from collections import defaultdict

from app import database as db
from app.data_collection.utils.definitions import IN_SEASON_LEAGUES

# some constants to interact with the database
DB = db.Database.get()
PARTITIONS = [league if 'NCAA' not in league else 'NCAA' for league in IN_SEASON_LEAGUES]  # Because all college team names are stored under 'NCAA' umbrella


def get_structured_docs(docs: list[dict]) -> dict:
    # use abbreviated names as keys
    structured_docs = {doc['abbr_name']: doc['_id'] for doc in docs}
    # also use full names as keys
    structured_docs.update({doc['full_name']: doc['_id'] for doc in docs})
    # return the structured documents
    return structured_docs


def structure_data() -> dict:
    # get collection being used
    teams_cursor = DB[db.TEAMS_COLLECTION_NAME]
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = dict()
    # for each partition in the partitions predicated upon the cursor name
    for partition in PARTITIONS:
        # filter by league or sport and don't include the batch_id
        filtered_docs = teams_cursor.find({'league': partition})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_data[partition] = get_structured_docs(filtered_docs)

    # return the fully structured and partitioned data
    return partitioned_data


def restructure_sets(data: dict) -> dict:
    restructured_data = dict()
    for key, values in data.items():
        teams = list()
        for value in values:
            value_dict = dict()
            for attribute in value:
                value_dict[attribute[0]] = attribute[1]

            teams.append(value_dict)

        restructured_data[key] = teams

    return restructured_data


class Teams:
    _stored_data: dict = structure_data()  # Unique to Teams
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