from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')

db = client['sauce']

subjects = db['subjects']

initial_document = {
    'league': None,
    'BoomFantasy': None,
    'Champ': None,
    'Dabble': None,
    'Drafters': None,
    'DraftKingsPick6': None,
    'HotStreak': None,
    'MoneyLine': None,
    'OddsShopper': None,
    'OwnersBox': None,
    'ParlayPlay': None,
    'Payday': None,
    'PrizePicks': None,
    'Rebet': None,
    'Sleeper': None,
    'SuperDraft': None,
    'ThriveFantasy': None,
    'Underdog Fantasy': None,
    'VividPicks': None
}

subjects.insert_one(initial_document)

#
# prop_lines = pd.read_json('../data_samples/prizepicks_data.json')
#
# prizepicks_subjects = prop_lines[['league', 'subject_team', 'position', 'subject']].drop_duplicates().sort_values(
#     by='subject')
#
# filtered_subjects = prizepicks_subjects[~prizepicks_subjects['subject'].str.contains('\+|\/', regex=True)]
#
# filtered_subjects['subject'] = filtered_subjects['subject'].apply(lambda x: x.strip())
#
# filtered_subjects['league'] = filtered_subjects['league'].apply(lambda x: 'GOLF' if x == 'EUROGOLF' else x)
#
# records = filtered_subjects.to_dict(orient='records')
#
# subjects.insert_many(records)



#
# for i in range(1, len(unique_markets) + 1):
#     markets.insert_one({'OddsShopper': unique_markets[i-1]})
