import threading
from collections import defaultdict


class UpdatedSubjects:
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
    _updated_subjects: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _lock1 = threading.Lock()

    @classmethod
    def get_store(cls, league: str = None) -> dict:
        return cls._updated_subjects.get(league) if league else cls._updated_subjects

    @classmethod
    def update_store(cls, subject: dict, attr_change: dict) -> None:
        with cls._lock1:
            cls._updated_subjects[subject['league']][subject['position']][subject['name']] = attr_change
