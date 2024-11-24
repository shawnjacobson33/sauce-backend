import threading
from collections import defaultdict
from typing import Union

import pandas as pd

from app.backend import database as db
from app.backend.database import SUBJECTS_COLLECTION_NAME
from app.backend.data_collection.utils.shared_data.storing.subjects.utils import get_subjects, update_subjects

# get the pointer to the subjects collection
collection = db.MongoDB.fetch_collection(SUBJECTS_COLLECTION_NAME)


class Subjects:
    """
    {
        ('NBA', 'SG', 'Jayson Tatum'): {
            'id': 123asd,
            'name': 'Jayson Tatum',
            'team_id': 'jpij[jasd'
        },
        ('NBA', 'asdjhasd', 'Jayson Tatum'): {
            'id': 123asd,
            'name': 'Jayson Tatum',
        }
        ...
    }
    """
    _subjects: defaultdict = get_subjects(collection)
    _lock1 = threading.Lock()

    @classmethod
    def get_store(cls, dtype: str = None) -> Union[dict, pd.DataFrame]:
        if dtype == 'df':
            return pd.DataFrame(
                [[league, subject_data['id'], name, subject_data['team_id']]
                for league, league_data in cls._subjects.items()
                for attr, attr_data in league_data.items() if len(attr) < 5  # only want one attribute (pos in this case)
                for name, subject_data in attr_data.items()],
                columns=['league', 'id', 'name', 'team_id']
            )

        return cls._subjects

    @classmethod
    def update_store(cls, subject: dict) -> int:
        """ONLY USED FOR ROSTER Retrieving"""
        with cls._lock1:
            # update the data stores and database if needed
            return update_subjects(collection, cls._subjects, subject)

    @classmethod
    def count_unique_subjects(cls) -> int:
        count = 0
        for league, league_data in cls._subjects.items():
            for attr, attr_data in league_data.items():
                if len(attr) < 5:  # only want one attribute (pos in this case)
                    for subject in attr_data:
                        count += 1

        return count