from typing import Optional, Union

import pandas as pd
from Levenshtein import distance

from app.db.tables import Subjects, Markets, SUBJECTS_CURSOR, MARKETS_CURSOR
from app.product_data.data_collection.utils.objects import Subject, Market, Team
from app.product_data.data_collection.utils.standardizing.constants import UPDATE_DATABASE_IS_ALLOWED, \
    PROHIBITED_STANDARDIZATION_USERS


# Define the mapping between entity types and their respective constructors and distance thresholds
ENTITY_MAP = {
    Market: {
        'cursor': MARKETS_CURSOR,
        'data_store': Markets,
        'get_partition': (lambda e: e.sport),
        'get_obj': (lambda e: Market(e.name, sport=e.sport)),
        'dist_threshold': 3
    },
    Subject: {
        'cursor': SUBJECTS_CURSOR,
        'data_store': Subjects,
        'get_partition': (lambda e: e.league),
        'get_obj': (lambda e: Subject(e.name, e.league, e.team, e.position, e.jersey_number)),
        'dist_threshold': 4,
    },
}


def get_filtered_data(entity: Union[Market, Subject, Team], dtype: str) -> Union[dict, pd.DataFrame]:
    # get partition lambda function
    get_partition = ENTITY_MAP[type(entity)]['get_partition']
    try:
        # get entity's actual partition value
        partition = get_partition(entity)

    # because the bookmaker Drafters doesn't include league or sport data in their responses
    except AttributeError:
        # unbelievable
        raise AttributeError("FUCK DRAFTERS!!!")

    # get the dataframe
    data_store = ENTITY_MAP[type(entity)]['data_store']
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = data_store.get_dict() if dtype == 'dict' else data_store.get_df()
    # filter it by partition
    return structured_data_store[partition]


def first_search(entity: Union[Market, Subject, Team], user: Optional[str]) -> Optional[str]:
    # filter by entity's partition (see ENTITY_MAP) at module level
    filtered_data = get_filtered_data(entity, dtype='dict')
    # get match if it exists
    if match := filtered_data.get(entity.name):
        # in a production environment don't want to risk database invalidity. Also, some bookmakers have too
        # high of variance in the data they transmit compared to the average bookmaker.
        if UPDATE_DATABASE_IS_ALLOWED and user not in PROHIBITED_STANDARDIZATION_USERS:
            if update_operation := get_update_operations(entity, match):
                # update database
                ENTITY_MAP[type(entity)]['cursor'].update_one({'_id': match['id']}, update_operation)

        # output message for match
        output_msg(entity, msg_type='match')
        # return the similar entity's matching id
        return match['id']


def second_search(entity: Union[Market, Subject, Team], n_entities: int = 1) -> Optional[str]:
    # filter by entity's partition (see ENTITY_MAP) at module level
    filtered_data = get_filtered_data(entity, dtype='df')
    # get the most similar entities
    top_n_similar_entities = get_most_similar_entities(entity, filtered_data, n_entities)
    # Get the constructor and distance_threshold based on the entity type
    constructor, distance_threshold = ENTITY_MAP[type(entity)]['get_obj'], ENTITY_MAP[type(entity)]['dist_threshold']
    # unpack rows into flatter structure and bring together data into abstracted objects
    most_similar_entity_objs = [constructor(sim_entity) for sim_entity in top_n_similar_entities.itertuples(index=False)]
    # get the most similar, similar entity has to exist
    most_similar_entity = top_n_similar_entities.iloc[0]
    # must exist
    if not most_similar_entity.empty:
        # Levenshtein distance between entity and most similar entity
        distance_between_entities = most_similar_entity['distance']
        # has to meet threshold
        if distance_between_entities < distance_threshold:
            # in a production environment probably don't want to risk database invalidity.
            if UPDATE_DATABASE_IS_ALLOWED:
                # distance not needed anymore
                del most_similar_entity['distance']
                # since these are referring to the same entity, the entity will be added to 'alt_names'
                if update_operations := get_update_operations(entity, most_similar_entity, add_alt=True):
                    # update database
                    ENTITY_MAP[type(entity)]['cursor'].update_one({'_id': most_similar_entity['id']}, update_operations)

            # write to log
            output_msg(entity, most_similar_entity_objs, distance_between_entities, msg_type='similar')
            # return entity id
            return most_similar_entity['id']

        # write to log for unfounded matches or for inserts to db
        output_msg(entity, most_similar_entity_objs, distance_between_entities, msg_type='insert' if
                    UPDATE_DATABASE_IS_ALLOWED else 'not_found')


def get_update_operations(entity: Union[Market, Subject, Team], entity_match: dict, add_alt: bool = False) -> dict:
    # Prepare the update document for conditional field updates
    update_fields = {}
    # Check and update fields only if they are not set or re-update 'team' and 'jersey_number' always.
    for field, value in entity.__dict__.items():
        # This will reduce the maintenance work when a player gets traded or signed to another team.
        if (not entity_match['attributes'].get(field)) or (field in {'team', 'jersey_number'}) and (field != 'name'):
            # Set the field in the document to the value of the corresponding argument
            update_fields[f'attributes.{field}'] = value

    # If there are fields to update, prepare the $set operation
    push_operation, set_operation = {}, {'$set': update_fields} if update_fields else {}
    # only need to add the alt name for a successful similarity match
    if add_alt:
        # Append a new alt name to the alt_names list in the subject's doc
        push_operation = {
            '$push': {
                'attributes.alt_names': entity.name
            }
        }
        # update the data store
        update_data_store(entity)

    # Combine set and push operations in one update query
    return {**set_operation, **push_operation}


def insert_new_entity(entity: Union[Market, Subject, Team]):
    # grab entity's attributes
    entity_data = entity.__dict__
    # doc to insert
    new_doc = {
        'name': entity.name,
        'attributes': {
            **{field_name: data for field_name, data in
               entity_data.items() if field_name not in {'id', 'name'}},
            'alt_names': list()
        }
    }
    # grab the id for later usage
    entity_data['id'] = ENTITY_MAP[type(entity)]['cursor'].insert_one(new_doc).inserted_id
    # update cache
    update_data_store(entity)
    # return new id
    return entity_data['id']


def output_msg(entity: Union[Market, Subject, Team], most_similar_entities: Optional[list[Union[Market, Subject, Team]]] = None, total_distance: Optional[float] = None, msg_type: str = None):
    if msg_type == 'match':
        print(f'FOUND {entity.name}: SUCCESS -> {entity}')

    elif msg_type == 'similar':
        print(f'FOUND SIMILAR {"SUBJECT" if isinstance(entity, Subject) else "MARKET"}: SUCCESSFUL MATCH -> {total_distance}')
        print(f'********************** {entity} **********************')
        print(f'---------------------- TOP {len(most_similar_entities)} MOST SIMILAR ----------------------')
        for rank, similar_entity in enumerate(most_similar_entities, 1):
            print(f'---------------------- {rank}. {similar_entity} ----------------------')

    elif msg_type == 'insert' and most_similar_entities:
        print(f'INSERTING {"SUBJECT" if isinstance(entity, Subject) else "MARKET"}: FAILED MATCH -> {total_distance}')
        print(f'********************** {entity} **********************')
        print(f'---------------------- TOP {len(most_similar_entities)} MOST SIMILAR ----------------------')
        for rank, similar_entity in enumerate(most_similar_entities, 1):
            print(f'---------------------- {rank}. {similar_entity} ----------------------')

    elif msg_type == 'not_found':
        if not most_similar_entities:
            print(f'NOT FOUND {entity.name}: FAIL -> {entity}')
        else:
            print(
                f'NOT FOUND {"SUBJECT" if isinstance(entity, Subject) else "MARKET"}: FAILED MATCH -> {total_distance}')
            print(f'********************** {entity} **********************')
            print(f'---------------------- TOP {len(most_similar_entities)} MOST SIMILAR ----------------------')
            for rank, similar_entity in enumerate(most_similar_entities, 1):
                print(f'---------------------- {rank}. {similar_entity} ----------------------')


def get_subject_distances(subject, r):
    # so weights can be adjusted to be more strict if there is fewer data points on a subject
    num_data_points = 0
    # initialize the overall distance with the subjects distance ---- weight goes down to 0.75 ----
    total_distance = 0
    # check similarity between teams
    if subject.team and r['team']:
        num_data_points += 1
        # More variance with team name formatting in college football so more leniency on distance for them
        weight = 1 if r['league'] not in {'NCAAF'} else 0.75
        total_distance += distance(r['team'], subject.team) * weight

    # check similarity between positions
    if subject.position and r['position']:
        num_data_points += 1
        # More variance with position formatting especially in MLB and NBA/WNBA so more lenient on distance
        total_distance += distance(r['position'], subject.position) * 0.75

    # check similarity between jersey_numbers
    if subject.jersey_number and r['jersey_number']:
        num_data_points += 1
        # shouldn't be many cases where jersey numbers don't match but subjects do so punish distance more
        total_distance += distance(r['jersey_number'], subject.jersey_number) * 2

    # add the distance between names to total_distance
    total_distance += distance(r['name'], subject.name) * (1 - num_data_points * 0.0625)
    # finally return the total distance between the two entities
    return total_distance


def get_most_similar_entities(entity: Union[Market, Subject, Team], df_store: pd.DataFrame, n_entities: int) -> pd.DataFrame:
    # get Levenshtein distances between each stored entity in the filtered data and the current entity
    if isinstance(entity, Subject):
        # use a custom, more intricate method for finding distances...incorporating other attributes
        df_store['distance'] = df_store.apply(lambda r: get_subject_distances(entity, r), axis=1)
    else:
        # since only attributes are 'sport' and 'alt_names' for Markets, only compute across names
        df_store['distance'] = df_store.apply(lambda r: distance(entity.name, r['name']), axis=1)

    # get the top 'n' similar entities
    return df_store.nsmallest(n_entities, 'distance')


def update_data_store(entity: Union[Market, Subject, Team]) -> None:
    # get shared data structure
    data_store = ENTITY_MAP[type(entity)]['data_store']
    # entity dictionary of data
    entity_dict = entity.__dict__
    # add a new entity to the shared dictionary
    data_store.update_dict(entity.name, {
        'id': entity_dict['id'],
        'attributes': {
            field_name: data for field_name, data in
            entity_dict.items() if field_name not in {'id', 'name', 'distance'}
        }
    })
    # add a new entity to the end of the shared df
    data_store.update_df(entity)
