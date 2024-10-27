from pandas import DataFrame

from app.db.database import Database
from app.db.utils.constants import SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME
from app.product_data.data_sourcing.utils.constants import IN_SEASON_LEAGUES, IN_SEASON_SPORTS


# some constants to interact with the database
DB = Database.get()
SUBJECTS_CURSOR = DB[SUBJECTS_COLLECTION_NAME]
MARKETS_CURSOR = DB[MARKETS_COLLECTION_NAME]


def structure_data(cursor, dtype: str):
    # get collection being used
    is_markets = (cursor.name.split('_')[0] == 'MARKETS')
    # get partitions to ensure valid keys (if there was a LeBron James in the NBA and MLB)
    partitions = IN_SEASON_SPORTS if is_markets else IN_SEASON_LEAGUES  # Markets use sports and Subjects use leagues
    partitioned_data = dict()
    for partition in partitions:
        # filter by league or sport and don't include the batch_id
        data = cursor.find({f'attributes.{"sport" if is_markets else "league"}': partition}, {'batch_id': 0})
        dictionary = dict()
        for doc in data:
            entity_id, attributes = doc.get('_id'), doc.get('attributes')
            # get the standardized name and add to subjects map
            default_name = doc.pop('name')
            # bookmakers sometimes have different formats for subject names
            alt_names = attributes.pop('alt_names')
            entity_data = {'id': entity_id, 'attributes': attributes}
            # add the data for the standard subject name
            dictionary[default_name] = entity_data
            if alt_names:
                # add the data for each of the alternate subject names
                for alt_name in alt_names:
                    dictionary[alt_name] = entity_data

        # set the data to its partition
        partitioned_data[partition] = dictionary if dtype == 'dict' else DataFrame(dictionary)

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