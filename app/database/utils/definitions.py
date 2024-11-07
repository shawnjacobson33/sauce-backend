APP_NAME = 'Sauce'
DATABASE_NAME = 'sauce'
DATABASE_URL = f'mongodb+srv://username:password@{DATABASE_NAME}.hvhxg.mongodb.net/?retryWrites=true&w=majority&appName={APP_NAME}'
SUBJECTS_COLLECTION_NAME = 'subjects-v4'
MARKETS_COLLECTION_NAME = 'markets-v4'
TEAMS_COLLECTION_NAME = 'teams-v1'
BOOKMAKERS_COLLECTION_NAME = 'bookmakers-v1'

# Don't Change
ALL_COLLECTION_NAMES = [SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME]
