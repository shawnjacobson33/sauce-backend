from collections import defaultdict

from pymongo.synchronous.collection import Collection

from app.backend.database import MongoDB, SUBJECTS_COLLECTION_NAME, TEAMS_COLLECTION_NAME

# teams = {str(doc['_id']): doc['abbr_name'] for doc in MongoDB.fetch_collection(TEAMS_COLLECTION_NAME).find()}
# subjects_cursor = MongoDB.fetch_collection(SUBJECTS_COLLECTION_NAME)
# for doc in subjects_cursor.find():
#     if 'team_id' in doc:
#         subjects_cursor.update_one(
#             filter={'_id': doc['_id']},
#             update={
#                 '$set': {'team': teams[doc['team_id']]},
#                 '$unset': {'team_id': ''}
#             }
#         )





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


# BETTING LINE DOCUMENT SCHEMA
# NOTES
# - Indicate the player's team by always putting it first in game_info
# - Does it make sense to store EV for the current algo?

# betting_line = {
#     'league': 'NBA',
#     'game_info': 'BOS @ ATL',
#     'market': 'Points',
#     'player': "Jayson Tatum",
#     'label': 'Over',
#     'history': {
#         'PrizePicks': [
#             {
#                 'timestamp': '2024-11-24 11:41:47.175330',
#                 'line': 25.5,
#                 'ev_(ALGO NAME)': -0.05609238
#             },
#             {
#                 'timestamp': '2024-11-24 11:43:47.175330',
#                 'line': 26.0,
#                 'mult': 1.5,
#                 'ev_(ALGO NAME)': -0.20998
#             }
#         ],
#         'DraftKings': [
#             {
#                 'timestamp': '2024-11-24 11:41:47.175330',
#                 'line': 26.5,
#                 'odds': 2.25,
#                 'im_prb': 0.582,
#                 'ev_(ALGO NAME)': -0.05609238
#             },
#             {
#                 'timestamp': '2024-11-24 11:43:47.175330',
#                 'line': 26.0,
#                 'odds': 2.18,
#                 'im_prb': 0.565,
#                 'ev_(ALGO NAME)': -0.20998
#             }
#         ],
#     },
#     'result': True
# }