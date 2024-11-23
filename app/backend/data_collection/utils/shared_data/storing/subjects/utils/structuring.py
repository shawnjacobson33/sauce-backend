from collections import defaultdict

from app.backend.data_collection.utils.cleaning import clean_subject
from app.backend.data_collection.utils.definitions import IN_SEASON_LEAGUES

# get league partitions
PARTITIONS = [league if 'NCAA' not in league else 'NCAA' for league in IN_SEASON_LEAGUES]  # Because all college team names are stored under 'NCAA' umbrella


def update_store(data_store: dict, subject: dict):
    # helps to standardize names across different sources
    cleaned_subject_name = clean_subject(subject['name'])
    # add key value pair...one with position identifier and one with team identifier because of varying available data for bookmakers
    data_store[subject['position']][cleaned_subject_name] = {
        'id': str(subject['_id']),
        'name': subject['name'],
        'team_id': subject['team_id'],
    }
    data_store[subject['team_id']][cleaned_subject_name] = {
        'id': str(subject['_id']),
        'name': subject['name']
    }


def store_subjects(docs: list[dict]) -> dict:
    # use abbreviated names as keys
    structured_docs = defaultdict(lambda: defaultdict(dict))
    # for each team
    for doc in docs:
        # update the structured docs with the subject
        update_store(structured_docs, doc)

    # return the structured documents
    return structured_docs


def get_subjects(collection) -> defaultdict:
    # initialize a dictionary to hold all the data partitioned
    partitioned_leagues = defaultdict(dict)
    # for each partition in the partitions predicated upon the cursor name
    for league in PARTITIONS:
        # filter by league or sport and don't include the batch_id
        filtered_subjects = collection.find({'league': league})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_leagues[league] = store_subjects(filtered_subjects)

    # return the fully structured and partitioned data
    return partitioned_leagues