import threading
from collections import defaultdict

from app.backend import database as db
from app.backend.database import SUBJECTS_COLLECTION_NAME
from app.backend.data_collection.utils.shared_data.storing.subjects.utils import get_subjects, update_subjects

# get the pointer to the subjects collection
collection = db.MongoDB.fetch_collection(SUBJECTS_COLLECTION_NAME)


class Subjects:
    """
    {
        'NBA': {
            'SF': {
                'Jayson Tatum': {
                    'id': '123314asd',
                    'name': 'Jayson Tatum',
                    'team_id': '-0iasd132',
                }
                ...
            },
            '-0iasd132' (team_id): {
                'Jayson Tatum': {
                    'id': '123314asd',
                    'name': 'Jayson Tatum',
                },
                ...
            },
            ...
        },
        ...
    }
    """
    _subjects: dict[str, dict[tuple[str, str], dict]] = get_subjects(collection)
    _lock1 = threading.Lock()

    @classmethod
    def get_store(cls, league: str = None) -> dict:
        return cls._subjects.get(league) if league else cls._subjects

    @classmethod
    def get_subject(cls, subject: dict) -> dict:
        # gets the subjects associated with the league
        if league_filtered_subjects := cls._subjects.get(subject['league']):
            # gets the subject based upon whether the bookmaker gives out position or team data
            if attribute_filtered_subjects := league_filtered_subjects.get(subject.get('position', subject.get('team_id'))):
                # finally get the data where there is a name match
                return attribute_filtered_subjects.get(subject.get('name'))

    @classmethod
    def update_store(cls, subject: dict) -> int:
        """ONLY USED FOR ROSTER Retrieving"""
        with cls._lock1:
            # update the data stores and database if needed
            return update_subjects(collection, cls.get_store(subject['league']), subject)

    @classmethod
    def count_unique_subjects(cls):
        # TODO: This is wrong somehow
        count = 0
        subject_tracker = defaultdict(int)
        for league_name, leagues in cls.get_store().items():
            for subj_attr, subject in leagues.items():
                for subject_name in subject:
                    if subject_tracker[(league_name, subj_attr, subject_name)] == 0:
                        count += 1

                    subject_tracker[(league_name, subj_attr, subject_name)] += 1

        return count