from collections import defaultdict

from pymongo.synchronous.collection import Collection

from app.backend.data_collection.utils.cleaning import clean_subject, clean_market


def store_team(teams: defaultdict, team: dict) -> None:
    # give each team name type its id as a pair and update the dictionary
    teams[(team['league'], team['abbr_name'])] = {
        'id': str(team['_id']),
        'full_name': team['full_name'],
    }


def store_subject(store: defaultdict, subject: dict) -> None:
    # clean the subject name
    c_subject_name = clean_subject(subject['name'], subject['league'])
    # helps to standardize names across different sources
    attrs_stored = {
        'id': str(subject['_id']),
        'team': subject['team'],
    }
    # add key value pair...one with position identifier and one with team identifier because of varying available data for bookmakers
    store[(subject['league'], subject['position'], c_subject_name)] = attrs_stored
    # store subject data for the second unique identifier
    store[(subject['league'], subject['team'], c_subject_name)] = {key: val for key, val in attrs_stored.items() if key != 'team'}


def store_market(store: defaultdict, market: dict) -> None:
    # clean the subject name
    c_market_name = clean_market(market['name'], market['sport'])
    # need market ids because 'sport' is not stored with betting lines
    store[(market['sport'], c_market_name)] = str(market['_id'])


STORE_FUNC_MAP = {
    'teams': store_team,
    'subjects': store_subject,
    'markets': store_market,
}


def get_entities(collection: Collection) -> defaultdict:
    # initialize a dictionary to hold all the data partitioned
    in_mem_store = defaultdict(dict)
    # get the store func correlated with the collection name
    store_func = STORE_FUNC_MAP[collection.name.split('-')[0]]
    # for each partition in the partitions predicated upon the cursor name
    for entity in collection.find({}):
        # structure the documents and data based upon whether its markets or subjects data
        store_func(in_mem_store, entity)

    # return the fully structured and partitioned data
    return in_mem_store