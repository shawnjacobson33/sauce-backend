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
    return MongoClient(DATABASE_URL.replace('username', username).replace('password', password), uuidRepresentation='standard')


def get_db():
    return get_client()['sauce']


def get_subjects(collection: Collection, has_leagues: bool = True) -> Union[
    dict[str, Union[dict[Any, dict[str, Optional[Any]]], DataFrame]], dict[
        Any, dict[str, Union[dict[Any, dict[str, Optional[Any]]], DataFrame]]]]:
    def get_queries() -> dict[Any, dict[str, Any]]:
        queries = {}
        # Drafters doesn't include leagues
        if has_leagues:
            for league in IN_SEASON_LEAGUES:
                queries[league] = {'attributes.league': league}

        return queries

    def query_subjects(q: dict = None) -> dict[
        str, Union[dict[Any, Union[dict[str, Optional[Any]], dict[str, Optional[Any]]]], DataFrame]]:

        # TODO: Add more hashing mechanisms to improve search performance (by first letter of a subject's name)

        nested_subjects, flattened_subjects = {}, []
        # query by league otherwise just grab all subjects
        docs = collection.find(q) if q else collection.find()
        for doc in docs:
            subject_id, attributes = doc.get('_id'), doc.get('attributes')
            if attributes:
                # get the standardized name and add to subjects map
                std_name = doc.pop('name')
                # bookmakers sometimes have different formats for subject names
                alt_names = attributes.pop('alt_names')
                # add the data for the standard subject name
                nested_subjects[std_name] = {'id': subject_id, 'attributes': attributes}
                flattened_subjects.append({'id': subject_id, 'name': std_name, **attributes})
                if alt_names:
                    # add the data for each of the alternate subject names
                    for alt_name in alt_names:
                        nested_subjects[alt_name] = {'id': subject_id, 'attributes': attributes}
                        flattened_subjects.append({'id': subject_id, 'name': alt_name, **attributes})

        return {'n': nested_subjects, 'f': DataFrame(flattened_subjects)}

    # Drafters will have a different initialization to handle all subjects from all leagues.
    subjects = {}
    if not has_leagues:
        return query_subjects()

    # otherwise get every subject from every league but separate them.
    for league_name, query in get_queries().items():
        subjects[league_name] = query_subjects(query)

    return subjects
