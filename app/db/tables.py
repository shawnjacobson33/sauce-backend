import polars as pl

from app.db.database import Database
from app.db.utils.constants import SUBJECTS_COLLECTION_NAME


DB = Database.get()
SUBJECTS_CURSOR = DB[SUBJECTS_COLLECTION_NAME]


class Subjects:
    _subjects: pl.DataFrame = pl.DataFrame(list(SUBJECTS_CURSOR.find({}, {'batch_id': 0})))  # SUBJECTS_CURSOR is just a cursor...get all docs

    @classmethod
    def get(cls):
        return cls._subjects

