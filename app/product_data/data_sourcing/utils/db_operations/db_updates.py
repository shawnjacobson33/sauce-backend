import pprint
from typing import Union
from pymongo.collection import Collection

from main import get_db
from app.product_data.data_sourcing.utils.objects import Market, Subject
from app.product_data.data_sourcing.utils.constants import SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME, \
    BOOKMAKERS_COLLECTION_NAME


db = get_db()

subjects = db[SUBJECT_COLLECTION_NAME]
markets = db[MARKETS_COLLECTION_NAME]
bookmakers = db[BOOKMAKERS_COLLECTION_NAME]


# ************* REMOVE OPERATIONS ***************
def remove_last_batch(batch_id: str, collection: Collection):
    """Only deletes newly inserted documents"""
    collection.delete_many({'batch_id': batch_id})

def remove_entity(entity: Union[Subject, Market], removable_attrs: list = None, insert_new: bool = False, add_to: Union[Subject, Market] = None):
    """Will remove entire doc or alt_name"""
    if not removable_attrs:
        removable_attrs = list()

    collection = subjects if isinstance(entity, Subject) else markets
    # filter by league or sport to avoid higher likelihood of unwanted deletes
    col_filter = {
        f'attributes.{"league" if isinstance(entity, Subject) else "sport"}': entity.league
        if isinstance(entity, Subject) else entity.sport
    }
    # attempt to delete an entire doc that contains the entity
    if not collection.delete_one({'name': entity.name, **col_filter}).deleted_count:
        # to find docs where the alt_names field contains entity's name
        col_filter['attributes.alt_names'] = {'$in': [entity.name]}
        # find a doc matching the criteria and apply operations
        result = collection.find_one_and_update(col_filter, {
            '$pull': {
                'attributes.alt_names': entity.name
            },
            '$set': {
                f'attributes.{attribute}': None for attribute in removable_attrs if attribute != 'name'
            }
        })  # to remove entity's name and desired attributes
        print(f"Successfully Removed {entity.name} from {result['name']}'s alt_names!")
        # print(f"{result['name']}'s Updated Attributes: ")
        # pprint.pprint(result['attributes'])

    else:
        print(f"Successfully Removed {entity.name}!")

    if insert_new:
        # insert a new doc for the entity with all attributes and an empty 'alt_names' list
        collection.insert_one({'name': entity.name, 'attributes': {**{
            f'attributes.{attribute}': value for attribute, value in entity.__dict__.items() if attribute != 'name'
        }, 'alt_names': list()}})
        print(f"Successfully Inserted {entity.name}!")

    if add_to:
        del col_filter['attributes.alt_names']
        result = collection.find_one_and_update({'name': add_to.name, **col_filter}, {
            '$push': {'attributes.alt_names': entity.name},
            '$set': {
                f'attributes.{attribute}': value for attribute, value in entity.__dict__.items()
                if attribute != 'name' and not add_to.__dict__[attribute]
            }
        })
        print(f"Successfully Added '{entity.name}' to '{result['name']}'s alt_names!")
        # print(f"{result.name}'s Updated Attributes: ")
        # pprint.pprint(result['attributes'])

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
def add_entity(alt_entity: Union[Subject, Market], new_entity: Union[Subject, Market], addable_attrs: list = None, delete_old: bool = False):
    if not addable_attrs:
        addable_attrs = list()

    collection = subjects if isinstance(new_entity, Subject) else markets
    # will delete the alt_entity's doc if it was a mistake to create one
    if delete_old:
        remove_entity(alt_entity)

    # filter by league or sport to avoid higher likelihood of unwanted deletes
    col_filter = {
        f'attributes.{"league" if isinstance(new_entity, Subject) else "sport"}': new_entity.league
        if isinstance(new_entity, Subject) else new_entity.sport
    }
    result = collection.find_one_and_update({'name': new_entity.name, **col_filter}, {'$push': {'attributes.alt_names': alt_entity.name}})
    if not result:
        # to find docs where the alt_names field contains entity's name
        col_filter['attributes.alt_names'] = {'$in': [new_entity.name]}
        result = collection.find_one_and_update(col_filter, {
            '$push': {
                'attributes.alt_names': new_entity.name
            },
            '$set': {
                f'attributes.{attribute}': value for attribute, value in new_entity.__dict__.items()
                if attribute != 'name' and attribute in addable_attrs
            }
        })

        print(f"Successfully added '{alt_entity.name}' to '{result['name']}'s alt_names!")

    print(f"Successfully added '{alt_entity.name}' to '{result['name']}'s alt_names!")


def update_attribute(name: str, old_attribute: str, new_attribute: str, collection: Collection):
    collection.update_many({f'attributes.{name}': old_attribute}, {'$set': {f'attributes.{name}': new_attribute}})

# ***********************************************************************************
# remove_last_batch('43f9bbc5-0853-4ec2-b1a8-3bee4b2073a3', subjects)

# Subjects:
remove_entity(entity=Subject('Dane Jackson', 'NFL', team='CAR', position='CB', jersey_number=None), insert_new=True)
# add_entity(alt_entity=Subject('Nicolas Claxton', 'NBA', team=None, position=None, jersey_number=None), new_entity=Subject('Nic Claxton', 'NBA', team='BKN', position='C', jersey_number='33'), delete_old=True)

# update_attribute(name='position', old_attribute='', new_attribute='D', collection=subjects)
# insert_entity(Subject('Josh Hart', 'NBA', 'NYK'))

# Markets:
# remove_entity(entity=Market('TO', sport='Basketball'), insert_new=False, add_to=Market('Turnovers', sport='Basketball'))
# add_entity(alt_entity=Market('First Qtr Assists', sport='Basketball'), new_entity=Market('1Q Assists', sport='Basketball'), delete_old=True)

# update_attribute(name='sport', old_attribute='Basketball', new_attribute='BBall', collection=markets)
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
