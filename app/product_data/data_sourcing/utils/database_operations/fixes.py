import uuid
from typing import Union

import pandas as pd
from pymongo.collection import Collection

from app.product_data.data_sourcing.utils import SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME, Market, Subject
from main import get_db

db = get_db()

subjects = db[SUBJECT_COLLECTION_NAME]
markets = db[MARKETS_COLLECTION_NAME]


# ************* REMOVE OPERATIONS ***************
def remove_last_batch(batch_id: str, collection: Collection):
    """Only deletes newly inserted documents"""
    collection.delete_many({'batch_id': batch_id})

def remove_entity(entity: str, collection: Collection):
    """Will remove entire doc"""
    collection.delete_one({'name': entity})

def remove_alt_entity(alt_entity: str, collection: Collection, other_removables: list = None):
    if other_removables is None:
        other_removables = []

    for doc in collection.find():
        for alt_name in doc['attributes']['alt_names']:
            if alt_name == alt_entity:
                update_operation = {
                    '$pull': {'attributes.alt_names': alt_entity},
                    '$set': {
                        f'attributes.{removable}': None for removable in other_removables
                    }
                }
                collection.update_one({'_id': doc['_id']}, update_operation)
                print(f"Successfully removed {alt_entity} from {doc['name']}'s alt_names!")
                break

    print(f"Failed to remove {alt_entity}!")

# ************** INSERT OPERATIONS ***************
def insert_entity(entity: Union[Subject, Market], collection: Collection):
    new_doc = {
        'name': entity.name,
        'attributes': {
            **entity.__dict__
        }
    }
    del new_doc['attributes']['name']
    collection.insert_one(new_doc)

# ************** UPDATE OPERATIONS ***************
def add_alt_entity(alt_entity: str, entity: Union[Subject, Market], collection: Collection):
    desired_doc = collection.find_one({**entity.__dict__})
    if desired_doc:
        collection.update_one({'_id': desired_doc['id']}, {'$push': {'attributes.alt_names': alt_entity}})
        print(f"Successfully added {alt_entity} to {entity.name}'s alt_names!")

    print(f"Failed to add {alt_entity}!")



# ***********************************************************************************

# remove_alt_entity('Tre Brown', subjects, ['team', 'position'])

# ***********************************************************************************












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


def check_alt_names(collection: Collection):
    data = []
    for doc in collection.find():
        if doc['attributes']['alt_names']:
            row = {'name': doc['name']}
            for i, alt_name in enumerate(doc['attributes']['alt_names']):
                row.update({f'alt_name_{i}': alt_name})

            data.append(row)

    return pd.DataFrame(data)


def check_for_duplicates(collection: Collection):
    subject_docs = collection.find()
    unique_subjects = dict()
    duplicates = []
    for doc in subject_docs:
        league = unique_subjects.get(doc['name'])
        if league != doc['attributes']['league']:
            unique_subjects[doc['name']] = doc['attributes']['league']
        else:
            duplicates.append(doc['name'])

    print(len(duplicates) == 0)
    return duplicates


# df = check_alt_names(subjects)
# docs = check_for_duplicates(subjects)

# teams = db['teams-v1']
# teams.insert_one({'batch_id': uuid.uuid4(), 'name': 'BAMA', 'attributes': {'league': 'NCAAF', 'alt_names': []}})

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


# for doc in subjects.find():
#     name = doc['name']
#     alt_names = doc['attributes']['alt_names']
#     if name in alt_names:
#         update_operation = {'$pull': {'attributes.alt_names': name}}
#         subjects.update_one({'_id': doc['_id']}, update_operation)


subjects.delete_many({'batch_id': '45251319-97b6-42b8-b5ca-e201b1a79d35'})
