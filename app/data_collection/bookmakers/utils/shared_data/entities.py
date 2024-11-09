import threading
from collections import defaultdict

from app import database as db
from app.data_collection.bookmakers.utils.cleaning import clean_subject
from app.data_collection.utils.definitions import IN_SEASON_LEAGUES, IN_SEASON_SPORTS

# some constants to interact with the database
DB = db.Database.get()
SUBJECTS_CURSOR = DB[db.SUBJECTS_COLLECTION_NAME]
MARKETS_CURSOR = DB[db.MARKETS_COLLECTION_NAME]
TEAMS_CURSOR = DB[db.TEAMS_COLLECTION_NAME]


def get_structured_docs(docs: list[dict], cursor_name: str) -> dict:
    # get a different structure if subjects
    if cursor_name == 'subjects':
        # re-format the name so it can be more easily matched with bookmaker's subject names
        structured_docs = {clean_subject(doc['name']): {'name': doc['name'], 'id': doc['_id']} for doc in docs}

    # get a different structure if teams
    elif cursor_name == 'teams':
        # use abbreviated names as keys
        structured_docs = {doc['abbr_name']: doc['_id'] for doc in docs}
        # also use full names as keys
        structured_docs.update({doc['full_name']: doc['_id'] for doc in docs})

    else:
        # this is the default which is used by markets
        structured_docs = {doc['name']: doc['_id'] for doc in docs}

    # return the structured docs
    return structured_docs


def structure_data(cursor) -> dict:
    # get collection being used
    cursor_name = cursor.name.split('-')[0]
    # get partitions to ensure valid keys (if there was a LeBron James in the NBA and MLB)
    partitions = IN_SEASON_SPORTS if cursor_name == 'markets' else IN_SEASON_LEAGUES  # Markets use sports and Subjects use leagues
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = dict()
    # for each partition in the partitions predicated upon the cursor name
    for partition in partitions:
        # filter by league or sport and don't include the batch_id
        filtered_docs = cursor.find({f'{"sport" if cursor_name == "markets" else "league"}': partition})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_data[partition] = get_structured_docs(filtered_docs, cursor_name)

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


class Leagues:
    _stored_data: dict  # Dictionary is much faster than any other data structure.
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
    def update_valid_leagues(cls, bookmaker: str, league: str):
        with cls._lock1:
            cls._valid_data[bookmaker].add((('name', league),))


class Subjects:
    _stored_data: dict = structure_data(SUBJECTS_CURSOR)  # Unique to Subjects
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


class Markets:
    _stored_data: dict = structure_data(MARKETS_CURSOR)  # Unique to Markets
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


class Teams:
    _stored_data: dict = structure_data(TEAMS_CURSOR)  # Unique to Teams
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