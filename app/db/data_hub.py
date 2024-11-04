from app.db.database import Database
from app.product_data.data_collection.plugs.bookmakers.utils import clean_subject
from app.db.utils.constants import SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME
from app.product_data.data_collection.plugs.utils import IN_SEASON_LEAGUES, IN_SEASON_SPORTS

# some constants to interact with the database
DB = Database.get()
SUBJECTS_CURSOR = DB[SUBJECTS_COLLECTION_NAME]
MARKETS_CURSOR = DB[MARKETS_COLLECTION_NAME]


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
    _subjects_pending: list[dict] = list()  # Hold data that needs to be evaluated manually before db insertion

    @classmethod
    def get_dict(cls):
        return cls._subjects_dict

    @classmethod
    def get_pending_data(cls) -> list[dict]:
        return cls._subjects_pending

    @classmethod
    def update_dict(cls, key, value):
        cls._subjects_dict[key] = value

    @classmethod
    def update_pending_data(cls, data: dict):
        cls._subjects_pending.append(data)


class Markets:
    _markets_dict: dict = structure_data(MARKETS_CURSOR)  # Dictionary is much faster than any other data structure.
    _markets_pending: list[dict] = list()  # Hold data that needs to be evaluated manually before db insertion

    @classmethod
    def get_dict(cls) -> dict:
        return cls._markets_dict

    @classmethod
    def get_pending_data(cls) -> list[dict]:
        return cls._markets_pending

    @classmethod
    def update_dict(cls, key, value):
        cls._markets_dict[key] = value

    @classmethod
    def update_pending_data(cls, data: dict):
        cls._markets_pending.append(data)