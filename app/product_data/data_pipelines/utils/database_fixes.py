from app.product_data.data_pipelines.utils import get_db
from app.product_data.data_pipelines.utils.constants import SUBJECT_COLLECTION_NAME
from pymongo.collection import Collection

db = get_db()

subjects = db[SUBJECT_COLLECTION_NAME]


def remove_subjects(batch_id: str = None, bookmaker: str = None):
    if bookmaker:
        # remove newly inserted docs
        subjects.delete_many({'bookmakers.0.bookmaker_name': bookmaker})
        # remove newly embedded subjects
        for doc in subjects.find():
            for bookmaker_data in doc['bookmakers']:
                if bookmaker_data['bookmaker_name'] == bookmaker:
                    subjects.update_one({'_id': doc['_id']}, {'$pull': {'bookmakers': bookmaker_data}})
                    break

    if batch_id:
        subjects.delete_many({'batch_id': batch_id})
        for doc in subjects.find():
            for bookmaker_data in doc['bookmakers']:
                if ('batch_id' in bookmaker_data) and bookmaker_data['batch_id'] == batch_id:
                    subjects.update_one({'_id': doc['_id']}, {'$pull': {'bookmakers': bookmaker_data}})
                    break


def update_subjects(old_league: str, new_league: str):
    subjects.update_many({'subject_info.league': old_league}, {'$set': {'subject_info.league': new_league}})


def update_collection_field_names(collection: Collection):
    for doc in collection.find():
        new_doc = {
            'subject_info': {
                'name': doc['subject_info']['subject'],
                'league': doc['subject_info']['league'],
                'team': doc['subject_info']['subject_team'],
                'position': doc['subject_info']['position'],
                'jersey_number': doc['subject_info']['jersey_number']
            },
            'bookmakers': doc['bookmakers'],
        }
        if 'batch_id' in doc:
            new_doc['batch_id'] = doc['batch_id']

        db['new_subjects'].insert_one(new_doc)


# update_collection_field_names(db['subjects'])
remove_subjects(batch_id="8516e271-e238-42ec-9ece-cf58964cacb1")
# remove_subjects(bookmaker='ParlayPlay')
# update_subjects('UCL', 'SOCCER')
