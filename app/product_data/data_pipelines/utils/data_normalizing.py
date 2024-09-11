import math
from typing import Optional
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

    def _build_query_1(self, league, target_subject):
        # Only include the 'subject_info.league' in the query if 'league' is provided and not empty
        if target_subject:
            query = {'bookmakers.bookmaker_name': self.bookmaker}
            query.update({'bookmakers.subject': target_subject})

            if league:
                query.update({'subject_info.league': league})

            # If no league is provided, return an empty dictionary, or handle differently as per requirements
            return query

    def _build_query_2(self, league):
        # Only include the 'subject_info.league' in the query if 'league' is provided and not empty
        if league:
            return {'subject_info.league': league}
        # If no league is provided, return an empty dictionary, or handle differently as per requirements
        return {}

    def _find_subject(self, target_subject, league):
        """A first attempt to quickly find the id associated with a particular subject."""
        query = self._build_query_1(league, target_subject)
        if query:
            doc = self.subject_collection.find_one(query)
            if doc:
                return doc.get('_id')

    def _find_most_similar_subject(self, target_subject, league=None, subject_team=None, position=None, number=None):
        query = self._build_query_2(league)
        projection = {'subject_info': 1}
        cursor = self.subject_collection.find(query, projection) if query else self.subject_collection.find(projection)
        return self._select_best_match(cursor, target_subject, subject_team, position, number)

    def _select_best_match(self, cursor, target_subject, subject_team, position, number):
        best_match, best_doc = None, None
        min_distance = float('inf')

        for doc in cursor:
            # only need one bookmaker's player info for each id
            subject_id = doc['_id']
            subject_info = doc.get('subject_info')
            if subject_info:
                distances = self._calculate_distances(subject_info, target_subject, subject_team, position, number)
                total_distance = self._calculate_overall_distance(distances)
                if total_distance < min_distance:
                    min_distance = total_distance
                    best_match, best_doc = self._build_match(subject_id, subject_info, distances), doc

                if total_distance < 2:
                    break

        return best_match, best_doc

    def _calculate_distances(self, subject_info, subject, subject_team, position, jersey_number):
        args = locals()
        distances = dict()
        for field in ['subject', 'subject_team', 'position', 'jersey_number']:
            target_field_value, stored_field_value = args[field], subject_info.get(field)
            if target_field_value and stored_field_value:
                distances[f'{field}_distance'] = lev.distance(str(target_field_value), str(stored_field_value))

        return distances

    def _calculate_overall_distance(self, distances):
        total_weighted_distance = 0
        for distance_type, distance in distances.items():
            # Put the most weight on the distance between subject names and less, split evenly, across other factors
            total_weighted_distance += distance * 0.65 if distance_type == 'subject_distance' else distance * 0.35 / len(distances)

        return math.floor(total_weighted_distance)

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
        if most_similar_subject and self._is_good_match(most_similar_subject, league):
            # over time this will improve query speeds because it will find the subject in the first search.
            self._update_subject_document(most_similar_doc, target_subject, league, subject_team, position, number)
            print(f'FOUND SIMILAR SUBJECT: {target_subject} {league} {subject_team} {position} {number}, {most_similar_subject} SUCCESS')
            return most_similar_subject['_id']

        if most_similar_subject and float('inf') not in most_similar_subject:
            print(f'INSERTED NEW SUBJECT: {target_subject} {league} {subject_team} {position} {number}, {most_similar_subject} FAIL')
        else:
            print(f'INSERTED NEW SUBJECT: {target_subject} {league} {subject_team} {position} {number}, None Found With Given Criterion FAIL')

        # If no good match, insert a new subject
        return self._insert_new_subject(target_subject, league, subject_team, position, number)

    def _is_good_match(self, match, league):
        is_good = True
        thresholds = dict()
        first_distance_check = 0
        # get subject distance and threshold
        subject_distance = match.get('subject_distance')
        if subject_distance < 2:
            return is_good

        subject_threshold = 3 if league not in {'CS', 'LOL', 'VAL', 'DOTA'} else 0
        first_distance_check += subject_distance

        # Create leniency with the subject threshold with respect to the distance of the subject team
        if 'subject_team_distance' in match:
            subject_team_distance = match.get('subject_team_distance')
            subject_threshold += (10 / 0.5 if not subject_team_distance else subject_team_distance) * 0.1
            first_distance_check += subject_team_distance

            # Quick check to speed up checking process
            if first_distance_check < 3:
                return is_good

        # Create leniency with the subject threshold with respect to the distance of the position
        if 'position_distance' in match:
            position_distance = match.get('position_distance')
            subject_threshold += (10 / 0.5 if not position_distance else position_distance) * 0.05

        # Create leniency with the subject threshold with respect to the distance of the position
        if 'jersey_number_distance' in match:
            jersey_number_distance = match.get('jersey_number_distance')
            subject_threshold += (10 / 0.5 if not jersey_number_distance else jersey_number_distance) * 0.05

        # Assign thresholds
        thresholds['subject_threshold'] = subject_threshold
        thresholds['subject_team_threshold'] = 3 if league in {'NCAAF'} else 1
        thresholds['position_threshold'] = 1
        thresholds['jersey_number_threshold'] = 0

        # Check every distance with every threshold
        for field in ['subject', 'subject_team', 'position', 'jersey_number']:
            is_good &= (match[f'{field}_distance'] <= thresholds[f'{field}_threshold']) if f'{field}_distance' in match else is_good

        return is_good

    def _update_subject_document(self, most_similar_doc, subject, league, subject_team, position, number):
        # Store all local variables in a dictionary for easy access
        args = locals()
        # Prepare the update document for conditional field updates
        update_fields = {}
        # Check and update fields only if they are not set
        for field in ['subject', 'league', 'subject_team', 'position', 'jersey_number']:
            arg_name = 'number' if field == 'jersey_number' else field
            if not most_similar_doc.get('subject_info').get(field):
                # Set the field in the document to the value of the corresponding argument
                update_fields[f'subject_info.{field}'] = args[arg_name]

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
                    'subject': subject
                }
            }
        }
        # Combine set and push operations in one update query
        combined_update = {**set_operation, **push_operation}
        # Perform the update on the collection
        self.subject_collection.update_one({'_id': most_similar_doc['_id']}, combined_update)

    def _insert_new_subject(self, subject, league, subject_team, position, jersey_number):
        new_document = {
            'subject_info': {
                'subject': subject,
                'league': league,
                'subject_team': subject_team,
                'position': position,
                'jersey_number': jersey_number,
            },
            'bookmakers': [
                {
                    'bookmaker_name': self.bookmaker,
                    'subject': subject
                }
            ]
        }
        result = self.subject_collection.insert_one(new_document)
        return result.inserted_id
