import threading
import uuid
from collections import defaultdict

from pymongo import ReturnDocument

from app.backend import database as db
from app.backend.database import SUBJECTS_COLLECTION_NAME
from app.backend.data_collection.utils.cleaning import clean_subject
from app.backend.data_collection.utils.definitions import IN_SEASON_LEAGUES

# get the pointer to the subjects collection
subjects_cursor = db.MongoDB.fetch_collection(SUBJECTS_COLLECTION_NAME)
# get league partitions
PARTITIONS = [league if 'NCAA' not in league else 'NCAA' for league in IN_SEASON_LEAGUES]  # Because all college team names are stored under 'NCAA' umbrella


def update_subjects_store(data_store: dict, subject: dict):
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
        update_subjects_store(structured_docs, doc)

    # return the structured documents
    return structured_docs


def get_subjects():
    # initialize a dictionary to hold all the data partitioned
    partitioned_leagues = dict()
    # for each partition in the partitions predicated upon the cursor name
    for league in PARTITIONS:
        # filter by league or sport and don't include the batch_id
        filtered_subjects = subjects_cursor.find({'league': league})
        # structure the documents and data based upon whether its markets or subjects data
        partitioned_leagues[league] = store_subjects(filtered_subjects)

    # return the fully structured and partitioned data
    return partitioned_leagues


def delete_batch_id(subject_doc: dict):
    # check to see if the batch id field is in the document
    if 'batch_id' in subject_doc:
        # delete the batch id because it isn't relevant when comparing player attributes
        del subject_doc['batch_id']


def get_original_subject_doc(filter_condition: dict):
    # get the original subject document before updating
    original_subject = subjects_cursor.find_one(filter_condition)
    # return the original subject
    return original_subject


def attempt_to_update(filter_condition: dict, subject: dict, batch_id: str) -> dict:
    # don't want to keep inserting duplicates/and game info can change...so perform an upsert using the filter
    return subjects_cursor.find_one_and_update(
        filter_condition,
        {
            "$set": subject,
            "$setOnInsert": {'batch_id': batch_id}
        },
        upsert=True,
        return_document=ReturnDocument.AFTER
    )


def check_for_updates(data_store: dict, original_subject: dict, updated_subject: dict):
    # don't need the batch id when comparing player attributes
    delete_batch_id(original_subject)
    delete_batch_id(updated_subject)
    # for every field in the subject object
    for key in original_subject:
        # if any of the original subject's values don't match the returned subject from update
        if original_subject[key] != updated_subject[key]:
            # track any updates to subjects
            data_store[original_subject['league']][original_subject['position']][original_subject['name']] = {
                'orig_subject': original_subject,
                'updated_subject': updated_subject
            }
            # exit loop
            break


class Subjects:
    """
    {
        'NBA': {
            ('Jayson Tatum', 'SF'): {
                'id': '123314asd',
                'team_id': '-0iasd132',
            },
            ('Jayson Tatum', 'asdiooi124' <-- team id): {
                ''id': '123314asd',
                'team_id': '-0iasd132',
            },
            ...
        },
        ...
    }
    """
    _subjects: dict[str, dict[tuple[str, str], dict]] = get_subjects()
    _new_subjects: defaultdict[str, dict] = defaultdict(dict)
    _updated_subjects: defaultdict[str, dict] = defaultdict(dict)
    _lock1 = threading.Lock()
    _lock2 = threading.Lock()
    _lock3 = threading.Lock()

    @classmethod
    def get_subjects(cls, league: str = None) -> dict:
        return cls._subjects.get(league, cls._subjects)

    @classmethod
    def get_subject(cls, subject: dict):
        # gets the subjects associated with the league
        if league_filtered_subjects := cls._subjects.get(subject['league']):
            # gets the subject based upon whether the bookmaker gives out position or team data
            if attribute_filtered_subjects := league_filtered_subjects.get(subject.get('position', subject.get('team_id'))):
                # finally get the data where there is a name match
                return attribute_filtered_subjects.get(subject.get('name'))

    @classmethod
    def get_new_subjects(cls, league: str = None) -> dict:
        return cls._subjects.get(league, cls._subjects)

    @classmethod
    def get_updated_subjects(cls):
        return cls._subjects

    @classmethod
    def update_subjects(cls, subject: dict):
        """ONLY USED FOR ROSTER COLLECTION"""
        with cls._lock1:
            # create a new batch id to identify if a subject was inserted or not
            batch_id = str(uuid.uuid4())
            # create a unique identifier (primary key) to filter on
            filter_condition = {
                'name': subject['name'],
                'position': subject['position'],
                'league': subject['league']
            }
            # get the original subject document
            original_subject = get_original_subject_doc(filter_condition)
            # update a subject if a subject changes teams, jersey number, etc.
            returned_subject = attempt_to_update(filter_condition, subject, batch_id)
            # if the returned subject document now has the batch id just created...then it was inserted
            if returned_subject.get('batch_id') == batch_id:
                # update the subject with return id
                subject['id'] = str(returned_subject['_id'])
                # update both data stores with the new subject
                update_subjects_store(cls.get_subjects(subject['league']), subject)
                update_subjects_store(cls.get_new_subjects(subject['league']), subject)
                # this represents a count for new subjects
                return 1

            # check to see if the subject had any new and then update the data store if so
            check_for_updates(cls._updated_subjects, original_subject, returned_subject)
            # no new subjects
            return 0

    @classmethod
    def size(cls):
        return sum([len(subject) for subject in cls._subjects.values()])