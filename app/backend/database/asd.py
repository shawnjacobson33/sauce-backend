from collections import defaultdict

from pymongo.synchronous.collection import Collection

from app.backend.database import get_db_session, SUBJECTS_COLLECTION_NAME

MongoDB = get_db_session()

def delete_duplicates(collection: Collection, name: str, attribute: str):
    # DELETING DUPLICATES
    counter_dict = defaultdict(int)
    count = 0
    for doc in collection.find():
        count += 1
        if counter_dict[(doc[attribute], doc[name], doc['team_id'])] > 0:
            print(doc)

        counter_dict[(doc[attribute], doc[name], doc['team_id'])] += 1

# delete_duplicates(MongoDB.get_collection(SUBJECTS_COLLECTION_NAME), 'name', 'league')

# MongoDB.get_collection(SUBJECTS_COLLECTION_NAME).delete_many({'league': 'NCAAM'})