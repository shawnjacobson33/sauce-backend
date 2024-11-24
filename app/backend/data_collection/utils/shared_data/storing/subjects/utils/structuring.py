from collections import defaultdict

from app.backend.data_collection.utils.cleaning import clean_subject


def store_subject(store: defaultdict, subject: dict) -> None:
    # clean the subject name
    c_subject_name = clean_subject(subject['name'], subject['league'])
    # helps to standardize names across different sources
    attrs_stored = {
        'id': str(subject['_id']),
        'team_id': subject['team_id'],
    }
    # add key value pair...one with position identifier and one with team identifier because of varying available data for bookmakers
    store[subject['league']][subject['position']][c_subject_name] = attrs_stored
    # store subject data for the second unique identifier
    store[subject['league']][subject['team_id']][c_subject_name] = {key: val for key, val in attrs_stored.items() if key != 'team_id'}


def get_subjects(collection) -> defaultdict:
    # initialize a dictionary to hold all the data partitioned
    in_mem_store = defaultdict(lambda: defaultdict(dict))
    # for each partition in the partitions predicated upon the cursor name
    for subject in collection.find({}):
        # structure the documents and data based upon whether its markets or subjects data
        store_subject(in_mem_store, subject)

    # return the fully structured and partitioned data
    return in_mem_store