from pymongo.database import Database
from app.product_data.data_sourcing.utils import SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME


def watch_subjects(db: Database):
    with db[SUBJECT_COLLECTION_NAME].watch() as stream:
        print(f"Watching changes in collection: {SUBJECT_COLLECTION_NAME}")
        for change in stream:
            pass


def watch_markets(db: Database):
    with db[MARKETS_COLLECTION_NAME].watch() as stream:
        print(f"Watching changes in collection: {MARKETS_COLLECTION_NAME}")
        for change in stream:
            pass