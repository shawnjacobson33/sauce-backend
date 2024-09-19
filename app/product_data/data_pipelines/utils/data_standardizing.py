from typing import Optional, Any

from pandas import DataFrame
from pymongo.database import Database
from pymongo.collection import Collection
import pandas as pd
import Levenshtein as lev

from app.product_data.data_pipelines.utils import Subject
from app.product_data.data_pipelines.utils.constants import LEAGUES, SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME


def get_subjects(collection: Collection, bookmaker: str) -> dict[Any, dict[str, DataFrame]]:
    def get_queries():
        queries = {}
        if bookmaker != 'Drafters':
            for league in LEAGUES:
                queries[league] = {'subject_info.league': league}

        return queries

    subjects = {}
    for league_name, query in get_queries().items():
        all_subjects = []
        bookmaker_specific_subjects = {}
        for doc in collection.find(query):
            subject_id, subject_info = doc.get('_id'), doc.get('subject_info')
            if subject_info:
                subject_info.update({'subject_id': subject_id})
                all_subjects.append(subject_info)

            bookmakers = doc.get('bookmakers')
            if bookmakers:
                for bookmaker_info in bookmakers:
                    if bookmaker_info.get('bookmaker_name') == bookmaker:
                        subject = bookmaker_info.get('subject')
                        if subject:
                            bookmaker_specific_subjects[subject] = subject_id

        subjects[league_name] = {'GEN': pd.DataFrame(all_subjects), bookmaker: bookmaker_specific_subjects}

    return subjects


class DataStandardizer:
    def __init__(self, batch_id: str, bookmaker: str, db: Database):
        self.batch_id = batch_id
        self.bookmaker = bookmaker
        self.market_collection = db[MARKETS_COLLECTION_NAME]
        self.subject_collection = db[SUBJECT_COLLECTION_NAME]
        self.subjects = get_subjects(self.subject_collection, bookmaker)

    def get_market_id(self, market: str) -> Optional[str]:
        try:
            query = {self.bookmaker: market}
            market_id = self.market_collection.find_one(query, {'_id': 1})
            return market_id.get('_id') if market_id else None
        except Exception as e:
            print(f"An error occurred while retrieving market ID: {e}")
            return None

    def get_subject_id(self, subject: Subject):
        # get filtered subjects based upon league
        league_specific_subjects = self.subjects[subject.league] if subject.league else None

        # 1st Search - based upon data already stored in relation to the bookmaker
        bookmaker_specific_subjects = league_specific_subjects[self.bookmaker]
        subject_id = bookmaker_specific_subjects.get(subject.name)
        if subject_id:
            print(f'FOUND {self.bookmaker.upper()} SUBJECT: {subject}, SUCCESS')
            return subject_id

        # 2nd Search - most similar
        most_similar_subject_info = DataStandardizer._get_most_similar_subject(league_specific_subjects['GEN'], subject)
        most_similar_subject = Subject(most_similar_subject_info['name'], most_similar_subject_info['league'],
                                       most_similar_subject_info['team'], most_similar_subject_info['position'],
                                       most_similar_subject_info['jersey_number'])
        # Threshold is flexible to change
        if not most_similar_subject_info.empty and most_similar_subject_info['distance'] < 4:
            # over time this will improve query speeds because it will find the subject in the first search.
            self._update_subject_document(subject, most_similar_subject_info)
            print(f'FOUND SIMILAR SUBJECT: SUCCESSFUL MATCH -> {most_similar_subject_info["distance"]}')
            print(f'********************** {subject} **********************')
            print(f'---------------------- {most_similar_subject} ----------------------')
            return most_similar_subject_info['subject_id']

        # If no good match, insert a new subject
        print(f'INSERTING {subject.name}: FAILED MATCH -> {most_similar_subject_info["distance"]}')
        print(f'********************** {subject} **********************')
        print(f'---------------------- {most_similar_subject} ----------------------')
        return self._insert_new_subject(subject)

    @staticmethod
    def _get_most_similar_subject(subjects: DataFrame, subject: Subject):
        def get_distances(row):
            # so weights can be adjusted to be more strict if there is fewer data points on a subject
            num_data_points = row[['team', 'position', 'jersey_number']].notnull().sum()
            # initialize the overall distance with the subjects distance ---- weight goes down to 0.75 ----
            total_distance = lev.distance(row['name'], subject.name) * (1 - num_data_points * 0.0625)
            if subject.team and row['team']:
                # More variance with team name formatting in college football...so more leniency on distance for them
                weight = 1 if row['league'] not in {'NCAAF'} else 0.75
                total_distance += lev.distance(row['team'], subject.team) * weight

            if subject.position and row['position']:
                # More variance with position formatting especially in MLB and NBA/WNBA...so more lenient on distance
                total_distance += lev.distance(row['position'], subject.position) * 0.75

            if subject.jersey_number and row['jersey_number']:
                # shouldn't be many cases where jersey numbers don't match but subjects do...so punish distance more
                total_distance += lev.distance(row['jersey_number'], subject.jersey_number) * 2

            return total_distance

        subjects['distance'] = subjects.apply(get_distances, axis=1)
        return subjects.sort_values(by='distance').iloc[0]

    def _update_subject_document(self, subject: Subject, similar_subject_info: dict[Any]):
        # Prepare the update document for conditional field updates
        update_fields = {}
        # Check and update fields only if they are not set
        for field, value in subject.__dict__.items():
            if not similar_subject_info.get(field):
                # Set the field in the document to the value of the corresponding argument
                update_fields[f'subject_info.{field}'] = value

        # If there are fields to update, prepare the $set operation
        set_operation = {'$set': update_fields} if update_fields else {}
        # Always append new bookmaker info regardless of other conditions
        push_operation = {
            '$push': {
                'bookmakers': {
                    'batch_id': self.batch_id,
                    'bookmaker_name': self.bookmaker,
                    'subject': subject.name
                }
            }
        }
        # Update current map of subject formats and ids to the bookmaker
        bookmaker_specific_subjects = self.subjects.get(self.bookmaker)
        if bookmaker_specific_subjects:
            bookmaker_specific_subjects[subject.name] = similar_subject_info.get('subject_id')
        # Combine set and push operations in one update query
        combined_update = {**set_operation, **push_operation}
        # Perform the update on the collection
        self.subject_collection.update_one({'_id': similar_subject_info['subject_id']}, combined_update)

    def _insert_new_subject(self, subject: Subject):
        result = self.subject_collection.insert_one({
            'batch_id': self.batch_id,
            'subject_info': {
                'name': subject.name,
                'league': subject.league,
                'team': subject.team,
                'position': subject.position,
                'jersey_number': subject.jersey_number,
            },
            'bookmakers': [
                {
                    'batch_id': self.batch_id,
                    'bookmaker_name': self.bookmaker,
                    'subject': subject.name
                }
            ]
        })
        return result.inserted_id
