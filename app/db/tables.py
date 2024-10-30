from pandas import DataFrame

from app.db.database import Database
from app.db.utils.constants import SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME
from app.product_data.data_collection.utils.constants import IN_SEASON_LEAGUES, IN_SEASON_SPORTS


# some constants to interact with the database
DB = Database.get()
SUBJECTS_CURSOR = DB[SUBJECTS_COLLECTION_NAME]
MARKETS_CURSOR = DB[MARKETS_COLLECTION_NAME]


def add_for_dict(doc: dict, store: dict):
    # get the entity's id and their attributes, if they both exist keep going
    if (entity_id := doc.get('_id')) and (attributes := doc.get('attributes')):
        # get the standardized name and add to subjects map
        default_name = doc.pop('name')
        # bookmakers sometimes have different formats for subject names
        alt_names = attributes.pop('alt_names')
        # get a slimmed down version of the entity's data (excluding the alt_names)
        entity_data = {'id': entity_id, 'attributes': attributes}
        # add the data for the standard subject name
        store[default_name] = entity_data
        # if there are alternate names
        if alt_names:
            # add the data for each of the alternate subject names
            for alt_name in alt_names:
                # store the alt name and the corresponding attributes
                store[alt_name] = entity_data

        # return the updated dictionary
        return store


def add_for_df(doc: dict, store: list):
    # get the entity's id and their attributes, if they both exist keep going
    if (entity_id := doc.get('_id')) and (subject_name := doc.get('name')) and (attributes := doc.get('attributes')):
        # bookmakers sometimes have different formats for subject names
        alt_names = attributes.pop('alt_names')
        # get a slimmed down version of the entity's data (excluding the alt_names)
        entity_data = {'id': entity_id, 'name': subject_name, **attributes}
        # add the data for the standard subject name
        store.append(entity_data)
        # if there are alternate names
        if alt_names:
            # add the data for each of the alternate subject names
            for alt_name in alt_names:
                # update the name to be the alternative name
                entity_data['name'] = alt_name
                # add the entity to the list
                store.append(entity_data)

        # return the updated list
        return store


def structure_data(cursor, dtype: str) -> dict:
    # get collection being used
    is_markets = (cursor.name.split('-')[0] == 'markets')
    # get partitions to ensure valid keys (if there was a LeBron James in the NBA and MLB)
    partitions = IN_SEASON_SPORTS if is_markets else IN_SEASON_LEAGUES  # Markets use sports and Subjects use leagues
    # initialize a dictionary to hold all the data partitioned
    partitioned_data = dict()
    # for each partition in the partitions predicated upon the cursor name
    for partition in partitions:
        # filter by league or sport and don't include the batch_id
        filtered_docs = cursor.find({f'attributes.{"sport" if is_markets else "league"}': partition}, {'batch_id': 0})
        # initialize a dictionary structure to hold all the data tha will be added partitioned data
        store = dict() if dtype == 'dict' else list()
        # for each document/row in the collection predicated upon the cursor name
        for doc in filtered_docs:
            # structure the data in a way tailored to how it will be stored and then add it to the data structure
            store = add_for_dict(doc, store) if dtype == 'dict' else add_for_df(doc, store)

        # set the data to its partition predicated upon the dtype
        partitioned_data[partition] = store if dtype == 'dict' else DataFrame(store)

    # return the fully structured and partitioned data
    return partitioned_data


class Subjects:
    _subjects_dict: dict = structure_data(SUBJECTS_CURSOR, dtype='dict')  # Dictionary is much faster than any other data structure.
    _subjects_df: DataFrame = structure_data(SUBJECTS_CURSOR, dtype='df')  # Used for expensive 'apply' method in standardization.

    @classmethod
    def get_dict(cls):
        return cls._subjects_dict

    @classmethod
    def get_df(cls):
        return cls._subjects_df

    @classmethod
    def update_dict(cls, key, value):
        cls._subjects_dict[key] = value

    @classmethod
    def update_df(cls, row):
        cls._subjects_df.loc[len(cls._subjects_df)] = row


class Markets:
    _markets_dict: dict = structure_data(MARKETS_CURSOR, dtype='dict')  # Dictionary is much faster than any other data structure.
    _markets_df: DataFrame = structure_data(MARKETS_CURSOR, dtype='df')  # Used for expensive 'apply' method in standardization.

    @classmethod
    def get_dict(cls):
        return cls._markets_dict

    @classmethod
    def get_df(cls):
        return cls._markets_df

    @classmethod
    def update_dict(cls, key, value):
        cls._markets_dict[key] = value

    @classmethod
    def update_df(cls, row):
        cls._markets_df.loc[len(cls._markets_df)] = row

