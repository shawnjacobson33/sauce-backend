import threading
from collections import defaultdict
from typing import Union, Optional

import pandas as pd

from app.backend.database import MongoDB, SUBJECTS_COLLECTION_NAME
from app.backend.data_collection.utils.shared_data.storing.utils import get_entities
from app.backend.data_collection.utils.shared_data.storing.subjects.utils import update_subjects

# get the pointer to the subjects collection
subjects_c = MongoDB.fetch_collection(SUBJECTS_COLLECTION_NAME)


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
    _subjects: defaultdict = get_entities(subjects_c)
    _lock1 = threading.Lock()

    @classmethod
    def get_subjects(cls, dtype: str = None) -> Union[dict, pd.DataFrame]:
        if dtype == 'df':
            return pd.DataFrame(
                [[key[0], subject_data['id'], key[2], subject_data['team']]
                for key, subject_data in cls._subjects.items() if 'team' in subject_data], # only want one attribute (pos in this case)
                columns=['league', 'id', 'name', 'team']
            )

        return cls._subjects

    @classmethod
    def get_subject(cls, subject: dict) -> Optional[dict]:
        if spec_attr := subject.get('position', subject.get('team')):
            return cls._subjects.get((subject['league'], spec_attr, subject['name']))

    @classmethod
    def update_subjects(cls, subject: dict) -> int:
        """ONLY USED FOR ROSTER Retrieving"""
        with cls._lock1:
            # update the data stores and database if needed
            return update_subjects(subjects_c, cls._subjects, subject)

    @classmethod
    def count_unique_subjects(cls) -> int:
        count = 0
        for key, subject_data in cls._subjects.items():
            if len(key[1]) < 5:
                count += 1

        return count