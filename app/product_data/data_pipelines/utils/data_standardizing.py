from typing import Optional, Any, Union, Dict

from pandas import DataFrame
from pymongo.database import Database
from pymongo.collection import Collection
import pandas as pd
import Levenshtein as lev

from app.product_data.data_pipelines.utils import Subject
from app.product_data.data_pipelines.utils.constants import IN_SEASON_LEAGUES, SUBJECT_COLLECTION_NAME as SCN, \
    MARKETS_COLLECTION_NAME as MCN


def get_subjects(collection: Collection, has_leagues: bool = True) -> Union[
    dict[str, Union[dict[Any, dict[str, Optional[Any]]], DataFrame]], dict[
        Any, dict[str, Union[dict[Any, dict[str, Optional[Any]]], DataFrame]]]]:
    def get_queries() -> dict[Any, dict[str, Any]]:
        queries = {}
        # Drafters doesn't include leagues
        if has_leagues:
            for league in IN_SEASON_LEAGUES:
                queries[league] = {'data.league': league}

        return queries

    def query_subjects(q: dict = None) -> dict[
        str, Union[dict[Any, Union[dict[str, Optional[Any]], dict[str, Optional[Any]]]], DataFrame]]:

        # TODO: Add more hashing mechanisms to improve search performance (by first letter of a subject's name)

        nested_subjects, flattened_subjects = {}, []
        # query by league otherwise just grab all subjects
        docs = collection.find(q) if q else collection.find()
        for doc in docs:
            subject_id, attributes = doc.get('_id'), doc.get('subject_info')
            if attributes:
                # get the standardized name and add to subjects map
                std_name = attributes.pop('name')
                # bookmakers sometimes have different formats for subject names
                alt_names = attributes.pop('alt_names')
                # add the data for the standard subject name
                nested_subjects[std_name] = {'id': subject_id, 'attributes': attributes}
                flattened_subjects.append({'id': subject_id, 'name': std_name, **attributes})
                if alt_names:
                    # add the data for each of the alternate subject names
                    for alt_name in alt_names:
                        nested_subjects[alt_name] = {'id': subject_id, 'attributes': attributes}
                        flattened_subjects.append({'id': subject_id, 'name': alt_name, **attributes})

        return {'n': nested_subjects, 'f': pd.DataFrame(flattened_subjects)}

    # Drafters will have a different initialization to handle all subjects from all leagues.
    subjects = {}
    if not has_leagues:
        return query_subjects()

    # otherwise get every subject from every league but separate them.
    for league_name, query in get_queries().items():
        subjects[league_name] = query_subjects(query)

    return subjects


class DataStandardizer:
    def __init__(self, batch_id: str, db: Database, has_leagues: bool = True):
        self.batch_id = batch_id
        self.market_collection = db[MCN]
        self.subject_collection = db[SCN]
        self.subjects = get_subjects(self.subject_collection, has_leagues=has_leagues)
        self.nested_subjects, self.flattened_subjects = None, None

    def get_market_id(self, market: str) -> Optional[str]:
        try:
            # TODO: Implement a standardizing procedure similar to the subject id process.
            # TODO: i.e. you shouldn't have to rely on a bookmaker name to standardize market names.
            # query = {self.bookmaker: market}
            query = {}
            market_id = self.market_collection.find_one(query, {'_id': 1})
            return market_id.get('_id') if market_id else None
        except Exception as e:
            print(f"An error occurred while retrieving market ID: {e}")
            return None

    def get_subject_id(self, subject: Subject):
        # get filtered subjects based upon league -- the condition is for 'Drafters' who doesn't include a league
        filtered_subjects = self.subjects[subject.league] if subject.league else self.subjects
        # make reference to it because it will be used in updates to it later (self because both nest and flat
        # structures need to be updated everytime there is a new subject record.)
        self.nested_subjects = filtered_subjects['n']
        # 1st Search - existing subject (format)
        subject_match = self.nested_subjects.get(subject.name)
        if subject_match:
            print(f'FOUND SUBJECT: SUCCESS -> {subject}')
            self._update_subject_document(subject, subject_match)
            return subject_match['id']

        # make reference to it because it will be used in updates to it later.
        flattened_filtered_subjects = filtered_subjects['f']
        # 2nd Search - most similar
        most_similar_subject_data = DataStandardizer._get_most_similar_subject(flattened_filtered_subjects, subject)
        most_similar_subject = Subject(most_similar_subject_data['name'], most_similar_subject_data['league'],
                                       most_similar_subject_data['team'], most_similar_subject_data['position'],
                                       most_similar_subject_data['jersey_number'])
        # Threshold is flexible to change
        if not most_similar_subject_data.empty and most_similar_subject_data['distance'] < 4:
            # over time this will improve query speeds because it will find the subject in the first search.
            # it can be predicted with certain that this subject name will be inserted into 'alt_names'
            self._update_subject_document(subject, most_similar_subject_data, add_alt=True)
            print(f'FOUND SIMILAR SUBJECT: SUCCESSFUL MATCH -> {most_similar_subject_data["distance"]}')
            print(f'********************** {subject} **********************')
            print(f'---------------------- {most_similar_subject} ----------------------')
            return most_similar_subject_data['id']

        # 3rd Search - most similar subject alt name
        # Def. Search Mechanism: the subjects db will now only have subject info, and any different
        # variation of subject's name, position, team, will be stored in a 'alt' key-value pair list
        # ** Consider baking the alternate name distances into the 2nd Search Mechanism instead of having
        # a completely new search.
        # TODO: Implement 3rd Searching Mechanism and get rid of individual bookmaker formatting
        # TODO: Modify the 'update_subject_document' method so that it updates 'alt' info in 'subject_info'
        # TODO: Modify the 'insert_subject_document' method so that it only inserts 'subject_info' with some alt info

        # If no good match, insert a new subject
        print(f'INSERTING {subject.name}: FAILED MATCH -> {most_similar_subject_data["distance"]}')
        print(f'********************** {subject} **********************')
        print(f'---------------------- {most_similar_subject} ----------------------')
        return self._insert_new_subject(subject)

    @staticmethod
    def _get_most_similar_subject(subjects, subject: Subject):
        def get_distances(r):
            # so weights can be adjusted to be more strict if there is fewer data points on a subject
            num_data_points = 0
            # initialize the overall distance with the subjects distance ---- weight goes down to 0.75 ----
            distance = 0
            if subject.team and r['team']:
                num_data_points += 1
                # More variance with team name formatting in college football...so more leniency on distance for them
                weight = 1 if r['league'] not in {'NCAAF'} else 0.75
                distance += lev.distance(r['team'], subject.team) * weight

            if subject.position and r['position']:
                num_data_points += 1
                # More variance with position formatting especially in MLB and NBA/WNBA...so more lenient on distance
                distance += lev.distance(r['position'], subject.position) * 0.75

            if subject.jersey_number and r['jersey_number']:
                num_data_points += 1
                # shouldn't be many cases where jersey numbers don't match but subjects do...so punish distance more
                distance += lev.distance(r['jersey_number'], subject.jersey_number) * 2

            distance += lev.distance(r['name'], subject.name) * (1 - num_data_points * 0.0625)
            return distance

        subjects['distance'] = subjects.apply(get_distances, axis=1)
        return subjects.sort_values(by='distance').iloc[0]

    def _update_subject_document(self, subject: Subject, similar_subject_data: dict[Any], add_alt: bool = False):
        # Prepare the update document for conditional field updates
        update_fields = {}
        # Check and update fields only if they are not set
        for field, value in subject.__dict__.items():
            if not similar_subject_data.get(field):
                # Set the field in the document to the value of the corresponding argument
                update_fields[f'attributes.{field}'] = value

        # If there are fields to update, prepare the $set operation
        push_operation, set_operation = {}, {'$set': update_fields} if update_fields else {}
        # only need to add the alt name for a successful similarity match
        if add_alt:
            # Append a new alt name to the alt_names list in the subject's doc
            push_operation = {
                '$push': {
                    'attributes.alt_names': subject.name
                }
            }
            # update the nested and flattened data
            self._update_in_memory_data(subject, similar_subject_data)

        # Combine set and push operations in one update query
        combined_update = {**set_operation, **push_operation}
        # Perform the update on the collection
        self.subject_collection.update_one({'_id': similar_subject_data['id']}, combined_update)

    def _update_in_memory_data(self, subject: Subject, data: dict) -> None:
        # add a new subject (format) to the end of the df
        self.flattened_subjects.loc[len(self.flattened_subjects)] = data

        # restructure the already flattened data into a nested form.
        nested_subject_data = dict(id=data['id'], attributes={field_name: data for field_name, data in data.items()
                                                              if field_name not in {'id', 'name'}})
        # add the subject
        self.nested_subjects[subject.name] = nested_subject_data

    def _insert_new_subject(self, subject: Subject):
        result = self.subject_collection.insert_one({
            'batch_id': self.batch_id,
            'name': subject.name,
            'attributes': {
                'league': subject.league,
                'team': subject.team,
                'position': subject.position,
                'jersey_number': subject.jersey_number,
                'alt_names': []
            }
        })
        return result.inserted_id
