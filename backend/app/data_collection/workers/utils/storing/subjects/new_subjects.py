import threading
from collections import defaultdict

from app.backend.data_collection.workers.utils.cleaning import clean_subject


class NewSubjects:
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
    _new_subjects: defaultdict[str, dict] = defaultdict(dict)
    _lock1 = threading.Lock()

    @classmethod
    def get_store(cls, league: str = None) -> dict:
        return cls._new_subjects.get(league) if league else cls._new_subjects

    @classmethod
    def update_store(cls, subject: dict) -> None:
        with cls._lock1:
            # get the filtered subjects by league
            filtered_subjects = cls.get_store(subject['league'])
            # helps to standardize names across different sources
            cleaned_subject_name = clean_subject(subject['name'], subject['league'])
            # add key value pair...one with position identifier and one with team identifier because of varying available data for bookmakers
            filtered_subjects[subject['position']][cleaned_subject_name] = {
                'id': str(subject['_id']),
                'name': subject['name'],
                'team_id': subject['team_id'],
            }
            filtered_subjects[subject['team_id']][cleaned_subject_name] = {
                'id': str(subject['_id']),
                'name': subject['name']
            }
