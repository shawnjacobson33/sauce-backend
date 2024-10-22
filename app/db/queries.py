from typing import Any
from pandas import DataFrame
from pymongo.collection import Collection

from app.db.utils.constants import SUBJECTS_COLLECTION_NAME, BOOKMAKERS_COLLECTION_NAME
from app.product_data.data_sourcing.utils.constants import IN_SEASON_LEAGUES, IN_SEASON_SPORTS


def get_queries(collection_name: str, has_grouping: bool) -> dict[Any, dict[str, Any]]:
    queries = {}
    # Groupings by league is more relevant to subjects not markets.
    if collection_name == SUBJECTS_COLLECTION_NAME:
        # Drafters doesn't include leagues
        if has_grouping:
            for league in IN_SEASON_LEAGUES:
                queries[league] = {'attributes.league': league}
    else:
        # Drafters doesn't include leagues
        if has_grouping:
            for sport in IN_SEASON_SPORTS:
                queries[sport] = {'attributes.sport': sport}

    return queries


def get_entities(collection: Collection, has_grouping: bool = True):
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

    # Drafters will have a different initialization to handle all entities from all groupings.
    entities = {}
    if not has_grouping:
        return query_entities()

    # otherwise get every entity from every grouping but separate them.
    for grouper_name, query in get_queries(collection.name, has_grouping).items():
        entities[grouper_name] = query_entities(query)

    return entities


def get_bookmaker(db, bookmaker: str) -> dict:
    return db[BOOKMAKERS_COLLECTION_NAME].find_one({'name': bookmaker})