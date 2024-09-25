from typing import Tuple, Optional, Union, Any
from pandas import DataFrame
from pymongo import MongoClient
from pymongo.collection import Collection
import os

from app.product_data.data_pipelines.utils import IN_SEASON_LEAGUES

DATABASE_URL = 'mongodb+srv://username:password@sauce.hvhxg.mongodb.net/?retryWrites=true&w=majority&appName=Sauce'


def get_db_creds() -> Tuple[str, str]:
    with open(os.path.abspath('../utils/db_creds.txt'), 'r') as f:
        data = f.readlines()
        return data[0].strip(), data[1]


def get_client():
    username, password = get_db_creds()
    return MongoClient(DATABASE_URL.replace('username', username).replace('password', password),
                       uuidRepresentation='standard')


def get_db():
    return get_client()['sauce']


def get_queries(has_leagues: bool) -> dict[Any, dict[str, Any]]:
    queries = {}
    # Drafters doesn't include leagues
    if has_leagues:
        for league in IN_SEASON_LEAGUES:
            queries[league] = {'attributes.league': league}

    return queries


def get_entities(collection: Collection, has_leagues: bool = True):
    def query_entities(q: dict = None):
        # TODO: Add more hashing mechanisms to improve search performance (by first letter of a subject's name)
        nested_entities, flattened_entities = {}, []
        # query by league otherwise just grab all subjects
        docs = collection.find(q) if q else collection.find()
        for doc in docs:
            entity_id, attributes = doc.get('_id'), doc.get('attributes')
            if attributes:
                # get the standardized name and add to subjects map
                std_name = doc.pop('name')
                # bookmakers sometimes have different formats for subject names
                alt_names = attributes.pop('alt_names')
                # add the data for the standard subject name
                nested_entities[std_name] = {'id': entity_id, 'attributes': attributes}
                flattened_entities.append({'id': entity_id, 'name': std_name, **attributes})
                if alt_names:
                    # add the data for each of the alternate subject names
                    for alt_name in alt_names:
                        nested_entities[alt_name] = {'id': entity_id, 'attributes': attributes}
                        flattened_entities.append({'id': entity_id, 'name': alt_name, **attributes})

        return {'n': nested_entities, 'f': DataFrame(flattened_entities)}

    # Drafters will have a different initialization to handle all subjects from all leagues.
    entities = {}
    if not has_leagues:
        return query_entities()

    # otherwise get every subject from every league but separate them.
    for league_name, query in get_queries(has_leagues).items():
        entities[league_name] = query_entities(query)

    return entities
