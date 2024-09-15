from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

local_db = client['sauce']

local_subjects = local_db['subjects']


def transfer_data(local_uri, atlas_uri, db_name, collection_name):
    # Connect to the local MongoDB
    local_client = MongoClient(local_uri)
    local_db = local_client[db_name]
    local_collection = local_db[collection_name]

    # Connect to MongoDB Atlas
    atlas_client = MongoClient(atlas_uri)
    atlas_db = atlas_client[db_name]
    atlas_collection = atlas_db[collection_name]

    # Fetch data from local collection
    data_to_transfer = list(local_collection.find())

    # Insert data into Atlas collection
    if data_to_transfer:
        atlas_collection.insert_many(data_to_transfer)
        print(f"Transferred {len(data_to_transfer)} documents from local to Atlas.")
    else:
        print("No data found to transfer.")


def remove_subjects(bookmaker: str):
    # remove newly inserted docs
    local_subjects.delete_many({'bookmakers.0.bookmaker_name': bookmaker})
    # remove newly embedded subjects
    for doc in local_subjects.find():
        for bookmaker_data in doc['bookmakers']:
            if bookmaker_data['bookmaker_name'] == bookmaker:
                local_subjects.update_one({'_id': doc['_id']}, {'$pull': {'bookmakers': bookmaker_data}})
                break


# # Configuration
# local_uri = 'mongodb://localhost:27017/'
# atlas_uri = "mongodb+srv://username:password@sauce.hvhxg.mongodb.net/?retryWrites=true&w=majority&appName=Sauce"
# db_name = 'sauce'
# collection_name = 'subjects'
#
# # Function call
# transfer_data(local_uri, atlas_uri, db_name, collection_name)

remove_subjects('Drafters')
