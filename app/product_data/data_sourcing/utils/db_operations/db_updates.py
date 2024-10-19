from typing import Union

from pymongo.collection import Collection

from app.product_data.data_sourcing.utils import SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME, Market, Subject, \
    BOOKMAKERS_COLLECTION_NAME
from main import get_db

db = get_db()

subjects = db[SUBJECT_COLLECTION_NAME]
markets = db[MARKETS_COLLECTION_NAME]
bookmakers = db[BOOKMAKERS_COLLECTION_NAME]


# ************* REMOVE OPERATIONS ***************
def remove_last_batch(batch_id: str, collection: Collection):
    """Only deletes newly inserted documents"""
    collection.delete_many({'batch_id': batch_id})

def remove_entity(entity: str, collection: Collection):
    """Will remove entire doc"""
    collection.delete_one({'name': entity})
    print(f"Successfully removed {entity}!")

def remove_alt_entity(alt_entity: str, collection: Collection, other_removables: list = None, insert: bool = False):
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
                if insert:
                    new_doc = {'name': alt_entity, 'attributes': {**doc['attributes']}}
                    new_doc['attributes']['alt_names'] = []
                    collection.insert_one(new_doc)
                    print(f"Successfully inserted {alt_entity}!")

                return

    print(f"Failed to remove {alt_entity}!")

# ************** INSERT OPERATIONS ***************
def insert_entity(entity: Union[Subject, Market]):
    new_doc = {
        'name': entity.name,
        'attributes': {
            **entity.__dict__,
            'alt_names': []
        }
    }
    del new_doc['attributes']['name']
    subjects.insert_one(new_doc) if isinstance(entity, Subject) else markets.insert_one(new_doc)
    print(f"Successfully inserted {entity}!")

# ************** UPDATE OPERATIONS ***************
def add_alt_entity(alt_entity: str, entity: Union[Subject, Market], delete_old: bool = False):
    collection = subjects if isinstance(entity, Subject) else markets
    # will delete the alt_entity's doc if it was a mistake to create one
    if delete_old:
        remove_entity(alt_entity, collection)

    attributes = {f'attributes.{field}': attribute for field, attribute in entity.__dict__.items() if attribute and field != 'name'}
    for doc in collection.find(attributes):
        if doc['name'] == entity.name:
            collection.update_one({'_id': doc['_id']}, {'$push': {'attributes.alt_names': alt_entity}})
            print(f"Successfully added '{alt_entity}' to '{doc['name']}'s alt_names!")
            return

        for alt_name in doc['attributes']['alt_names']:
            if alt_name == entity.name:
                collection.update_one({'_id': doc['_id']}, {'$push': {'attributes.alt_names': alt_entity}})
                print(f"Successfully added '{alt_entity}' to '{doc['name']}'s alt_names!")
                return

    print(f"Failed to add {alt_entity}!")


def update_subject_position(old_position: str, new_position: str):
    subjects.update_many({'attributes.position': old_position}, {'$set': {'attributes.position': new_position}})

# ***********************************************************************************

# remove_alt_entity('Jeremiah Noga', subjects, other_removables=['team'], insert=True)
# remove_alt_entity('2PT Made', markets, insert=True)
# add_alt_entity('Patrick Garwo', Subject('Pat Garwo', 'NCAAF'), delete_old=True)
# add_alt_entity('First Qtr Points', Market('1Q Points', sport='Basketball'), delete_old=True)


update_subject_position('', 'D')
# insert_entity(Subject('Josh Hart', 'NBA', 'NYK'))
# insert_entity(Market('2PT Attempted', sport='Basketball'))

# ***********************************************************************************

# ODDS CONVERSION FORMULA:
# ----- Positive: (American odds / 100) + 1 = decimal odds
# ----- Negative: (100 / American odds) + 1 = decimal odds

# BOOKMAKER PAYOUTS DOC STRUCTURE
# fields:
# ------- bookmaker: str ('PrizePicks')
# ------- default_odds: dict
# ------------ legs: int (3)
# ------------ is_insured: bool (False)
# ------------ odds: float (2.00)
# ------- payouts: list
# ------------ {
# ----------------- legs: int (3)
# ----------------- is_insured: bool (False)
# ----------------- odds: float (2.00)
# ------------ }
# --------------------------------------------

# bookmaker_payouts.delete_one({'bookmaker': 'Drafters'})

# bookmakers.insert_one({
#     'name': 'OddsShopper',
#     'is_dfs': False,
#     'default_odds': {
#         'legs': 6,
#         'is_insured': False,
#         'odds': round(1 / ((1.0 / 10) ** (1.0/6)), 3)
#     }, 'payouts': [
#         {'legs': 2, 'is_insured': False, 'odds': round(1 / ((1.0 / 2) ** (1.0/2)), 3)},
#         {'legs': 3, 'is_insured': False, 'odds': round(1 / ((1.0 / 3) ** (1.0/3)), 3)},
#         {'legs': 4, 'is_insured': False, 'odds': round(1 / ((1.0 / 4) ** (1.0/4)), 3)},
#         {'legs': 5, 'is_insured': False, 'odds': round(1 / ((1.0 / 5) ** (1.0/5)), 3)},
#         # {'legs': 6, 'is_insured': False, 'odds': round(1 / ((1.0 / 10) ** (1.0/6)), 3)},
#         # {'legs': 7, 'is_insured': False, 'odds': round(1 / ((1.0 / ????) ** (1.0/7)), 3)},
#         # {'legs': 8, 'is_insured': False, 'odds': round(1 / ((1.0 / ????) ** (1.0/8)), 3)},
#         # {'legs': 9, 'is_insured': False, 'odds': round(1 / ((1.0 / ????) ** (1.0/9)), 3)},
#         # {'legs': 10, 'is_insured': False, 'odds': round(1 / ((1.0 / ????) ** (1.0/10)), 3)},
#         # {'legs': 3, 'is_insured': True, 'odds': 1.817},
#         # {'legs': 7, 'is_insured': False, 'odds': 1.746},
#         # {'legs': 8, 'is_insured': True, 'odds': 1.781},
#         # {'legs': 8, 'is_insured': False, 'odds': 1.775},
#     ]
# })
