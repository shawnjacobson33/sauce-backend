from typing import Optional, Any, Union

import pandas as pd
from pymongo.collection import Collection
from pymongo.database import Database
from Levenshtein import distance

from app.product_data.data_sourcing.utils import Subject, Market, SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME, \
    get_entities, Team, TEAMS_COLLECTION_NAME



class DataStandardizer:
    def __init__(self, batch_id: str, db: Database, has_grouping: bool = True):
        self.batch_id = batch_id
        self.market_collection = db[MARKETS_COLLECTION_NAME]
        self.subject_collection = db[SUBJECT_COLLECTION_NAME]
        self.team_collection = db[TEAMS_COLLECTION_NAME]
        self.subjects = get_entities(self.subject_collection, has_grouping=has_grouping)
        self.markets = get_entities(self.market_collection, has_grouping=has_grouping)
        # self.teams = get_entities(self.team_collection, has_grouping=has_grouping)

    def get_team_id(self, team: Team) -> Optional[str]:
        filtered_teams = self._get_filtered_data(team, self.teams)
        team_id = self._first_search(team, self.team_collection, filtered_teams)
        if team_id:
            return team_id

        team_id = self._second_search(team, self.team_collection, filtered_teams)
        if team_id:
            return team_id

        return self._insert_new_entity(team, self.team_collection)

    def get_market_id(self, market: Market, user: str = None) -> Optional[str]:
        filtered_markets = self._get_filtered_data(market, self.markets)
        market_id = self._first_search(market, self.market_collection, filtered_markets)
        if user not in {'OddsShopper'}:
            if market_id:
                return market_id

            market_id = self._second_search(market, self.market_collection, filtered_markets)
            if market_id:
                return market_id

            return self._insert_new_entity(market, self.market_collection)

        elif not market_id:
            # only applicable for PROHIBITED_BOOKMAKERS
            DataStandardizer._output_msg(market, msg_type='not_found')

    def get_subject_id(self, subject: Subject, user: str = None):
        filtered_subjects = self._get_filtered_data(subject, self.subjects)
        subject_id = self._first_search(subject, self.subject_collection, filtered_subjects)
        if user not in {'OddsShopper'}:
            if subject_id:
                return subject_id

            subject_id = self._second_search(subject, self.subject_collection, filtered_subjects)
            if subject_id:
                return subject_id

            return self._insert_new_entity(subject, self.subject_collection)

        elif not subject_id:
            # only applicable for PROHIBITED_BOOKMAKERS
            DataStandardizer._output_msg(subject, msg_type='not_found')

    def _first_search(self, entity: Union[Market, Subject, Team], collection: Collection, in_mem_data: Union[dict, pd.DataFrame]) -> Optional[str]:
        if isinstance(entity, Market):
            nested_markets = in_mem_data['n'] if 'n' in in_mem_data else in_mem_data
            match = nested_markets.get(entity.name)
        elif isinstance(entity, Subject):
            nested_subjects = in_mem_data['n'] if 'n' in in_mem_data else in_mem_data
            match = nested_subjects.get(entity.name)
        else:
            nested_teams = in_mem_data['n'] if 'n' in in_mem_data else in_mem_data
            match = nested_teams.get(entity.name)

        if match:
            update_operation = self._get_update_operations(entity, match)
            if update_operation:
                collection.update_one({'_id': match['id']}, update_operation)

            DataStandardizer._output_msg(entity, msg_type='match')
            return match['id']

    def _second_search(self, entity: Union[Market, Subject, Team], collection: Collection, in_mem_data: Union[dict, pd.DataFrame]) -> Optional[str]:
        flattened_entities = in_mem_data['f'] if 'f' in in_mem_data else in_mem_data
        sm_data = DataStandardizer._get_most_similar_entity(entity, flattened_entities)
        if isinstance(entity, Market):
            most_similar_entity = Market(sm_data['name'], sport=sm_data['sport'])
            distance_threshold = 3
        elif isinstance(entity, Subject):
            most_similar_entity = Subject(sm_data['name'], sm_data['league'], sm_data['team'], sm_data['position'], sm_data['jersey_number'])
            distance_threshold = 4
        else:
            most_similar_entity = Team(sm_data['name'], league=sm_data['league'])
            distance_threshold = 2

        distance_between_entities = sm_data['distance']
        if not sm_data.empty and distance_between_entities < distance_threshold:
            del sm_data['distance']
            update_operations = self._get_update_operations(entity, sm_data, add_alt=True)
            if update_operations:
                collection.update_one({'_id': sm_data['id']}, update_operations)

            DataStandardizer._output_msg(entity, most_similar_entity, distance_between_entities, msg_type='similar')
            return sm_data['id']

        DataStandardizer._output_msg(entity, most_similar_entity, distance_between_entities, msg_type='insert')

    @staticmethod
    def _get_filtered_data(entity: Union[Market, Subject, Team], in_mem_data: dict):
        grouper = entity.sport if isinstance(entity, Market) else entity.league
        # filter by league and cover the case where, for example Drafters, a bookmaker doesn't provide a league
        filtered_entities = in_mem_data.get(grouper, {}) if grouper else in_mem_data
        # case where there are no markets for an inputted league -- flattened the data
        if not filtered_entities:
            for sub_entities in in_mem_data.values():
                filtered_entities.update(sub_entities)

        return filtered_entities

    @staticmethod
    def _get_most_similar_entity(entity: Union[Market, Subject, Team], in_mem_data: Union[pd.DataFrame]):
        def get_distances(r):
            # so weights can be adjusted to be more strict if there is fewer data points on a subject
            num_data_points = 0
            # initialize the overall distance with the subjects distance ---- weight goes down to 0.75 ----
            total_distance = 0
            if isinstance(entity, Subject):
                if entity.team and r['team']:
                    num_data_points += 1
                    # More variance with team name formatting in college football so more leniency on distance for them
                    weight = 1 if r['league'] not in {'NCAAF'} else 0.75
                    total_distance += distance(r['team'], entity.team) * weight

                if entity.position and r['position']:
                    num_data_points += 1
                    # More variance with position formatting especially in MLB and NBA/WNBA so more lenient on distance
                    total_distance += distance(r['position'], entity.position) * 0.75

                if entity.jersey_number and r['jersey_number']:
                    num_data_points += 1
                    # shouldn't be many cases where jersey numbers don't match but subjects do so punish distance more
                    total_distance += distance(r['jersey_number'], entity.jersey_number) * 2

            total_distance += distance(r['name'], entity.name) * (1 - num_data_points * 0.0625)
            return total_distance

        in_mem_data['distance'] = in_mem_data.apply(get_distances, axis=1)
        return in_mem_data.sort_values(by='distance').iloc[0]

    def _get_update_operations(self, entity: Union[Market, Subject, Team], match_data: dict[Any], add_alt: bool = False) -> dict:
        # Prepare the update document for conditional field updates
        update_fields = {}
        # Check and update fields only if they are not set
        for field, value in entity.__dict__.items():
            # because nested keeps it in a dict 'attributes' and flattened doesn't
            if 'attributes' in match_data:
                if not match_data['attributes'].get(field) and field != 'name':
                    # Set the field in the document to the value of the corresponding argument
                    update_fields[f'attributes.{field}'] = value
            else:
                if not match_data.get(field) and field != 'name':
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
            # update the nested and flattened data
            self._update_in_mem_data(entity, match_data)

        # Combine set and push operations in one update query
        return {**set_operation, **push_operation}

    def _update_in_mem_data(self, entity: Union[Market, Subject, Team], data: dict) -> None:
        if isinstance(entity, Subject):
            # add a new subject (format) to the end of the df
            flattened_subjects = self.subjects[entity.league]['f'] if entity.league else self.subjects['f']
            flattened_subjects.loc[len(flattened_subjects)] = data

            nested_subjects = self.subjects[entity.league]['n'] if entity.league else self.subjects['n']
            nested_subjects[entity.name] = {
                'id': data['id'],
                'attributes': {
                    field_name: data for field_name, data in
                    data.items() if field_name not in {'id', 'name', 'distance'}
                }
            }
        else:
            # add a new subject (format) to the end of the df
            flattened_markets = self.markets[entity.sport]['f'] if entity.sport else self.markets['f']
            flattened_markets.loc[len(flattened_markets)] = data

            nested_markets = self.markets[entity.sport]['n'] if entity.sport else self.markets['n']
            nested_markets[entity.name] = {
                'id': data['id'],
                'attributes': {
                    field_name: data for field_name, data in
                    data.items() if field_name not in {'id', 'name', 'distance'}
                }
            }

    def _insert_new_entity(self, entity: Union[Market, Subject, Team], collection: Collection):
        # grab its attributes
        entity_data = entity.__dict__
        # doc to insert
        new_doc = {
            'batch_id': self.batch_id,
            'name': entity.name,
            'attributes': {
                **{field_name: data for field_name, data in
                   entity_data.items() if field_name not in {'id', 'name'}},
                'alt_names': []
            }
        }
        # grab the id for later usage
        entity_data['id'] = collection.insert_one(new_doc).inserted_id
        # update cache
        self._update_in_mem_data(entity, entity_data)
        return entity_data['id']

    @staticmethod
    def _output_msg(entity: Union[Market, Subject, Team], similar_entity: Optional[Union[Market, Subject, Team]] = None, total_distance: Optional[float] = None, msg_type: str = None):
        if msg_type == 'match':
            print(f'FOUND {entity.name}: SUCCESS -> {entity}')
        elif msg_type == 'similar':
            print(f'FOUND SIMILAR {"SUBJECT" if isinstance(entity, Subject) else "MARKET"}: SUCCESSFUL MATCH -> {total_distance}')
            print(f'********************** {entity} **********************')
            print(f'---------------------- {similar_entity} ----------------------')
        elif msg_type == 'insert':
            print(f'INSERTING {"SUBJECT" if isinstance(entity, Subject) else "MARKET"}: FAILED MATCH -> {total_distance}')
            print(f'********************** {entity} **********************')
            print(f'---------------------- {similar_entity} ----------------------')
        elif msg_type == 'not_found':
            print(f'NOT FOUND {entity.name}: FAIL -> {entity}')
