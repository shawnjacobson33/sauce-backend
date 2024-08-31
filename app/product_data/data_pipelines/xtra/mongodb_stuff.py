from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

db = client['sauce']

markets = db['markets']

markets.insert_one({'Champ': 'Moneyline'})




#
# for i in range(1, len(unique_markets) + 1):
#     markets.insert_one({'OddsShopper': unique_markets[i-1]})
