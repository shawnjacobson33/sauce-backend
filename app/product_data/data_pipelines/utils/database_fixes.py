from pymongo.collection import Collection

from constants import SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME
from database import get_db

db = get_db()

subjects = db[SUBJECT_COLLECTION_NAME]
markets = db[MARKETS_COLLECTION_NAME]


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
# remove_subjects(batch_id="e4a8c59e-2a6f-4247-8631-784349ed68a7")
# remove_subjects(bookmaker='ParlayPlay')
# update_subjects('UCL', 'SOCCER')


# markets_v3 = db['markets_v3']
# batch_id = 'd550f72d-49a3-4e30-b692-727060207291'
# markets_v3.insert_one({
#     'batch_id': batch_id,
#     'name': 'Total Passing Yards',
#     'attributes': {
#         'league': 'NFL',
#         'alt_names': []
#     }})

# CHANGE FORMATTING ON MARKETS V2
# for doc in markets.find():
#     alt_names = doc.get('alt_names')
#     unset_operation = {'$unset': {'alt_names': ''}}
#     set_operation = {'$set': {'attributes': {'alt_names': alt_names}}}
#     combined_operation = {**unset_operation, **set_operation}
#     markets.update_one({'_id': doc['_id']}, combined_operation)


# USED TO CREATE RESTRUCTURED COLLECTION
# reformatted_docs = []
# for doc in markets.find():
#     markets = {'alt_names': []}
#     std_name_set = False
#     for bookmaker in doc:
#         if bookmaker not in {'SmartBettor', '_id'}:
#             if doc[bookmaker] and not std_name_set:
#                 markets['name'] = doc[bookmaker]
#                 std_name_set = not std_name_set
#             elif doc[bookmaker] not in markets['alt_names']:
#                 markets['alt_names'].append(doc[bookmaker])
#
#     reformatted_docs.append(markets)
#
# db['markets_v2'].insert_many(reformatted_docs)

# USED TO CREATE RESTRUCTURED COLLECTION
# reformatted_docs = []
# for doc in subjects.find():
#     std_subject_name = doc['subject_info']['name']
#     alt_names = []
#     for bookmaker in doc['bookmakers']:
#         if (bookmaker.get('subject') != std_subject_name) and (bookmaker.get('subject') not in alt_names):
#             alt_names.append(bookmaker.get('subject'))
#
#     reformatted_docs.append({
#         'batch_id': doc.get('batch_id'),
#         'name': std_subject_name,
#         'attributes': {
#             'league': doc['subject_info']['league'],
#             'team': doc['subject_info']['team'],
#             'position': doc['subject_info']['position'],
#             'jersey_number': doc['subject_info']['jersey_number'],
#             'alt_names': alt_names
#         }
#     })
#
# subjects_beta_v2 = db['subjects_beta_v2']
# subjects_beta_v2.insert_many(reformatted_docs)
