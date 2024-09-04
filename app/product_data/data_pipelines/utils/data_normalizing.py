from typing import Optional

from pymongo.database import Database
import Levenshtein as lev


class DataNormalizer:
    def __init__(self, db: Database):
        self.market_collection = db['markets']
        self.subject_collection = db['subjects']

    def get_market_id(self, bookmaker: str, market: str) -> Optional[str]:
        try:
            query = {bookmaker: market}
            market_id = self.market_collection.find_one(query, {'_id': 1})
            return market_id.get('_id') if market_id else None
        except Exception as e:
            print(f"An error occurred while retrieving market ID: {e}")
            return None

    def _find_most_similar_subject(self, target_subject, league=None, subject_team=None):
        """Find the most similar subject within the specified league."""
        query = {}
        if league:
            query['league'] = league
        if subject_team:
            query['subject_team'] = subject_team

        cursor = self.subject_collection.find(query, {'_id': 1, 'subject': 1})
        best_match = ('', '', float('inf'))  # subject_id, subject, distance

        for doc in cursor:
            distance = lev.distance(target_subject, doc['subject'])
            if distance < best_match[2]:
                best_match = (doc['_id'], doc['subject'], distance)

        return best_match

    def get_subject_id(self, target_subject, league=None, subject_team=None, position=None):
        """Normalize the subject by checking the database for a similar subject or inserting a new one."""
        most_similar_subject = self._find_most_similar_subject(target_subject, league, subject_team)

        # Calculate threshold based on the length of the found subject
        threshold = int(len(most_similar_subject[1]) * 0.30)  # 30% of the string length
        if most_similar_subject[2] <= threshold:
            return most_similar_subject[0]  # Return _id of the similar subject

        print(f'Inserted New Subject: {target_subject}, {most_similar_subject}')
        return 0
        # # Insert new document if no similar subject is found within the threshold
        # new_document = {
        #     'subject': target_subject,
        #     'league': league,
        #     'subject_team': subject_team,
        #     'position': position
        # }
        # result = self.subject_collection.insert_one(new_document)
        # return result.inserted_id  # Return _id of the newly inserted document
