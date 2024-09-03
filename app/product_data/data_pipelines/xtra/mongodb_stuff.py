from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')

db = client['sauce']

subjects = db['subjects']

prop_lines = pd.read_json('../data_samples/prizepicks_data.json')

prizepicks_subjects = prop_lines[['league', 'subject_team', 'position', 'subject']].drop_duplicates().sort_values(by='subject')

prizepicks_subjects['subject'] = prizepicks_subjects['subject'].apply(lambda x: x.strip())

subjects.insert_many(prizepicks_subjects)

#
# for i in range(1, len(unique_markets) + 1):
#     markets.insert_one({'OddsShopper': unique_markets[i-1]})
