from typing import Optional, Any, Union

from pymongo.database import Database
from Levenshtein import distance

from app.product_data.data_pipelines.utils import Subject, Market, SUBJECT_COLLECTION_NAME as SCN, \
    MARKETS_COLLECTION_NAME as MCN, get_entities


class DataStandardizer:
    def __init__(self, batch_id: str, db: Database, has_grouping: bool = True):
        self.batch_id = batch_id
        self.market_collection = db[MCN]
        self.subject_collection = db[SCN]
        self.subjects = get_entities(self.subject_collection, has_grouping=has_grouping)
        self.markets = get_entities(self.market_collection, has_grouping=has_grouping)
        self.nested_subjects, self.flattened_subjects = None, None
        self.nested_markets, self.flattened_markets = None, None

    def get_market_id(self, market: Market) -> Optional[str]:
        filtered_markets = self._get_filtered_data(market)
        self.nested_markets = filtered_markets['n']
        market_match = self.nested_markets.get(market.name)
        if market_match:
            update_operation = self._get_update_operations(market, market_match)
            if update_operation:
                self.market_collection.update_one({'_id': market_match['id']}, update_operation)

            DataStandardizer._output_msg(market, msg_type='match')
            return market_match['id']

        self.flattened_markets = filtered_markets['f']
        sm_data = self._get_most_similar_entity(market)
        most_similar_market = Market(sm_data['name'], sport=sm_data['sport'])
        market_distance = sm_data['distance']
        if not sm_data.empty and market_distance < 3:
            del sm_data['distance']
            update_operations = self._get_update_operations(market, sm_data, add_alt=True)
            if update_operations:
                self.market_collection.update_one({'_id': sm_data['id']}, update_operations)

            DataStandardizer._output_msg(market, most_similar_market, market_distance, msg_type='similar')
            return sm_data['id']

        DataStandardizer._output_msg(market, most_similar_market, market_distance, msg_type='insert')
        return self._insert_new_entity(market)

    def get_subject_id(self, subject: Subject):
        # get filtered subjects based upon league -- the condition is for 'Drafters' who doesn't include a league
        filtered_subjects = self._get_filtered_data(subject)
        # make reference to it because it will be used in updates to it later (self because both nest and flat
        # structures need to be updated everytime there is a new subject record.)
        self.nested_subjects = filtered_subjects['n']
        # 1st Search - existing subject (format)
        subject_match = self.nested_subjects.get(subject.name)
        if subject_match:
            update_operations = self._get_update_operations(subject, subject_match)
            if update_operations:
                self.subject_collection.update_one({'_id': subject_match['id']}, update_operations)

            DataStandardizer._output_msg(subject, msg_type='match')
            return subject_match['id']

        # make reference to it because it will be used in updates to it later.
        self.flattened_subjects = filtered_subjects['f']
        # 2nd Search - most similar
        ss_data = self._get_most_similar_entity(subject)
        most_similar_subject = Subject(ss_data['name'], ss_data['league'], ss_data['team'], ss_data['position'], ss_data['jersey_number'])
        # Threshold is flexible to change
        subject_distance = ss_data['distance']
        if not ss_data.empty and subject_distance < 4:
            # over time this will improve query speeds because it will find the subject in the first search.
            # it can be predicted with certain that this subject name will be inserted into 'alt_names'
            # don't want the distance to be updated into a subject's doc
            del ss_data['distance']
            update_operations = self._get_update_operations(subject, ss_data, add_alt=True)
            if update_operations:
                self.subject_collection.update_one({'_id': ss_data['id']}, update_operations)

            DataStandardizer._output_msg(subject, most_similar_subject, subject_distance, msg_type='similar')
            return ss_data['id']

        # If no good match, insert a new subject
        DataStandardizer._output_msg(subject, most_similar_subject, subject_distance, msg_type='insert')
        return self._insert_new_entity(subject)

    def _get_filtered_data(self, entity: Union[Market, Subject]):
        if isinstance(entity, Subject):
            # filter by league and cover the case where, for example Drafters, a bookmaker doesn't provide a league
            filtered_subjects = self.subjects.get(entity.league, {}) if entity.league else self.subjects
            # case where there are no markets for an inputted league -- flattened the data
            if not filtered_subjects:
                for sub_subjects in self.subjects.values():
                    filtered_subjects.update(sub_subjects)

            return filtered_subjects

        # filter by league and cover the case where, for example Drafters, a bookmaker doesn't provide a league
        filtered_markets = self.markets.get(entity.sport, {}) if entity.sport else self.markets
        # case where there are no markets for an inputted league -- flattened the data
        if not filtered_markets:
            for sub_markets in self.markets.values():
                filtered_markets.update(sub_markets)

        return filtered_markets

    def _get_most_similar_entity(self, entity: Union[Market, Subject]):
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

        if isinstance(entity, Subject):
            self.flattened_subjects['distance'] = self.flattened_subjects.apply(get_distances, axis=1)
            return self.flattened_subjects.sort_values(by='distance').iloc[0]

        self.flattened_markets['distance'] = self.flattened_markets.apply(get_distances, axis=1)
        return self.flattened_markets.sort_values(by='distance').iloc[0]

    def _get_update_operations(self, entity: Union[Market, Subject], match_data: dict[Any], add_alt: bool = False) -> dict:
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

    def _update_in_mem_data(self, entity: Union[Market, Subject], data: dict) -> None:
        if isinstance(entity, Subject):
            # add a new subject (format) to the end of the df
            self.flattened_subjects.loc[len(self.flattened_subjects)] = data
            # restructure the already flattened data into a nested form.
            self.nested_subjects[entity.name] = {
                'id': data['id'],
                'attributes': {
                    field_name: data for field_name, data in
                    data.items() if field_name not in {'id', 'name', 'distance'}
                }
            }
        else:
            # add a new subject (format) to the end of the df
            self.flattened_markets.loc[len(self.flattened_markets)] = data
            # restructure the already flattened data into a nested form.
            self.nested_markets[entity.name] = {
                'id': data['id'],
                'attributes': {
                    field_name: data for field_name, data in
                    data.items() if field_name not in {'id', 'name', 'distance'}
                }
            }

    def _insert_new_entity(self, entity: Union[Market, Subject]):
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
        # insert the doc based upon type of entity
        if isinstance(entity, Subject):
            result = self.subject_collection.insert_one(new_doc)
        else:
            result = self.market_collection.insert_one(new_doc)

        # grab the id for later usage
        entity_data['id'] = result.inserted_id
        # update cache
        self._update_in_mem_data(entity, entity_data)
        return result.inserted_id

    @staticmethod
    def _output_msg(entity: Union[Market, Subject], similar_entity: Optional[Union[Market, Subject]] = None, total_distance: Optional[float] = None, msg_type: str = None):
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
