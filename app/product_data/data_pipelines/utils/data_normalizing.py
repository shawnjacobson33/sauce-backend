from typing import Optional
import random
from pymongo.database import Database
import Levenshtein as lev


def get_bookmakers():
    return [
        'BoomFantasy', 'Champ', 'Dabble', 'Drafters', 'DraftKingsPick6', 'HotStreak', 'MoneyLine', 'OddsShopper',
        'OwnersBox', 'ParlayPlay', 'Rebet', 'Sleeper', 'SuperDraft', 'ThriveFantasy', 'Underdog Fantasy', 'VividPicks'
    ]


class DataNormalizer:
    def __init__(self, bookmaker: str, db: Database):
        self.bookmaker = bookmaker
        self.market_collection = db['markets']
        self.subject_collection = db['subjects']

    def get_market_id(self, market: str) -> Optional[str]:
        try:
            query = {self.bookmaker: market}
            market_id = self.market_collection.find_one(query, {'_id': 1})
            return market_id.get('_id') if market_id else None
        except Exception as e:
            print(f"An error occurred while retrieving market ID: {e}")
            return None

    def _build_first_query(self, league):
        query = {}
        if league:
            query['league'] = league

        return query

    def _find_subject(self, target_subject, league):
        """A first attempt to quickly find the id associated with a particular subject."""
        query = self._build_first_query(league)
        projection = {'bookmakers': 1}
        cursor = self.subject_collection.find(query, projection)
        bookmaker_specific_data = [
            {'_id': doc['_id'], 'subject_info': bookmaker['subject_info']}
            for doc in cursor
            for bookmaker in doc['bookmakers']
            if bookmaker['bookmaker_name'] == self.bookmaker
        ]
        subject_id_mapping = {
            data['subject_info']['subject']: data['_id'] for data in bookmaker_specific_data
        }
        return subject_id_mapping.get(target_subject)

    def _find_most_similar_subject(self, target_subject, league=None, subject_team=None, position=None, number=None):
        query = self._build_second_query(league)
        projection = {'bookmakers': 1}
        cursor = self.subject_collection.find(query, projection)
        return self._select_best_match(cursor, target_subject, subject_team, position, number)

    def _build_second_query(self, league):
        query = {}
        if league:
            query['league'] = league
        return query

    def _get_random_bookmakers(self, k: int = 3):
        bookmakers = get_bookmakers()
        bookmakers.remove(self.bookmaker)
        return random.sample(bookmakers, k=k)

    def _select_best_match(self, cursor, target_subject, subject_team, position, number):
        best_match, best_doc = None, None
        min_distance = float('inf')

        for doc in cursor:
            # only need one bookmaker's player info for each id
            subject_id = doc['_id']
            bookmakers = doc.get('bookmakers')
            if bookmakers:
                subject_info = bookmakers[0].get('subject_info')
                if subject_info:
                    distances = self._calculate_distances(subject_info, target_subject, subject_team, position, number)
                    total_distance = sum(distances.values())
                    if total_distance < min_distance:
                        min_distance = total_distance
                        best_match, best_doc = self._build_match(subject_id, subject_info, distances), doc

                    if total_distance < 2:
                        break

        return best_match, best_doc

    def _calculate_distances(self, subject_info, target_subject, subject_team, position, number):
        return {
            'subject_distance': lev.distance(target_subject, subject_info.get('subject', '')),
            'subject_team_distance': lev.distance(subject_team, subject_info.get('subject_team', '')) if subject_team else 0,
            'position_distance': lev.distance(position, subject_info.get('position', '')) if position else 0,
            'jersey_number_distance': lev.distance(number, subject_info.get('jersey_number', '')) if number else 0
        }

    def _build_match(self, subject_id, subject_info, distances):
        match = {k: v for k, v in subject_info.items() if k in ['subject', 'subject_team', 'position', 'jersey_number']}
        match['_id'] = subject_id
        match.update(distances)
        return match

    def get_subject_id(self, target_subject, league=None, subject_team=None, position=None, number=None):
        # 1st Search - based upon data already stored in relation to the bookmaker
        subject_id = self._find_subject(target_subject, league)
        if subject_id:
            print(f'FOUND {self.bookmaker.upper()} SUBJECT: {target_subject} {subject_team}, SUCCESS')
            return subject_id

        # 2nd Search - most similar
        most_similar_subject, most_similar_doc = self._find_most_similar_subject(target_subject, league, subject_team, position, number)
        if most_similar_subject and self._is_good_match(most_similar_subject):
            # over time this will improve query speeds because it will find the subject in the first search.
            self._update_subject_document(most_similar_doc, target_subject, league, subject_team, position, number)
            print(f'FOUND SIMILAR SUBJECT: {target_subject} {subject_team}, {most_similar_subject} SUCCESS')
            return most_similar_subject['_id']

        if most_similar_subject and float('inf') not in most_similar_subject:
            print(f'INSERTED NEW SUBJECT: {target_subject} {subject_team}, {most_similar_subject} FAIL')
        else:
            print(f'INSERTED NEW SUBJECT: {target_subject} {subject_team}, None Found With Given Criterion FAIL')

        # If no good match, insert a new subject
        return self._insert_new_subject(target_subject, league, subject_team, position, number)

    def _is_good_match(self, match):
        # Create thresholds for the most similar match to meet
        subject_threshold = int(len(match['subject']) * 0.30)
        subject_team_threshold = int(len(match['subject_team']) * 0.50) if match['subject_team'] else 0
        position_threshold = int(len(match['position']) * 0.50) if match['position'] else 0
        jersey_number_threshold = int(len(match['jersey_number']) * 0.50) if match['jersey_number'] else 0

        # Check if both subject and subject_team distances are within their respective thresholds
        is_subject_good = (match['subject_distance'] <= subject_threshold) or (match['subject_distance'] < 2)
        is_subject_team_good = (match['subject_team_distance'] <= subject_team_threshold) or (match['subject_team_distance'] < 2)
        is_position_good = (match['position_distance'] <= position_threshold) or (match['position_distance'] < 2)
        is_jersey_number_good = (match['jersey_number_distance'] <= jersey_number_threshold) or (match['jersey_number_distance'] < 2)

        # Return True if both subject and subject team and position and jersey number matches are good
        return is_subject_good and is_subject_team_good and is_position_good and is_jersey_number_good

    def _update_subject_document(self, most_similar_doc, subject, league, subject_team, position, number):
        # Store all local variables in a dictionary for easy access
        args = locals()
        # Prepare the update document for conditional field updates
        update_fields = {}
        # Check and update fields only if they are not set
        for field in ['subject', 'league', 'subject_team', 'position', 'jersey_number']:
            # Convert 'jersey_number' to 'number' to match the argument name
            arg_name = 'number' if field == 'jersey_number' else field
            if not most_similar_doc.get(field):
                # Set the field in the document to the value of the corresponding argument
                update_fields[field] = args[arg_name]

        # If there are fields to update, prepare the $set operation
        if update_fields:
            set_operation = {'$set': update_fields}
        else:
            set_operation = {}

        # Always append new bookmaker info regardless of other conditions
        push_operation = {
            '$push': {
                'bookmakers': {
                    'bookmaker_name': self.bookmaker,
                    'subject_info': {
                        'subject': subject,
                        'league': league,
                        'subject_team': subject_team,
                        'position': position,
                        'jersey_number': number
                    }
                }
            }
        }
        # Combine set and push operations in one update query
        combined_update = {**set_operation, **push_operation}
        # Perform the update on the collection
        self.subject_collection.update_one({'_id': most_similar_doc['_id']}, combined_update)

    def _insert_new_subject(self, subject, league, subject_team, position, number):
        new_document = {
            'subject': subject,
            'league': league,
            'subject_team': subject_team,
            'position': position,
            'jersey_number': number,
            'bookmakers': [
                {
                    'bookmaker_name': self.bookmaker,
                    'subject_info': {
                        'subject': subject,
                        'league': league,
                        'subject_team': subject_team,
                        'position': position,
                        'jersey_number': number
                    }
                }
            ]
        }
        result = self.subject_collection.insert_one(new_document)
        return result.inserted_id
