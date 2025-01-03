import sqlite3


class SQLite:
    _cursor = sqlite3.connect('sports_data.db').cursor()

    @classmethod
    def cursor(cls):
        return cls._cursor

    @classmethod
    def fetch(cls, table: str):
        return cls._cursor.execute("""
            SELECT * FROM ?
        """, (table,)).fetchall()


# for doc in MongoDB.fetch_collection(SOURCES_COLLECTION_NAME).find():
#     if 'is_dfs' in doc and 'default_odds' in doc:
#         SQLite.cursor().execute("""
#                 INSERT INTO bookmaker_sources (name, dflt_odds, payouts) VALUES (?, ?, ?)
#             """, (doc['name'], json.dumps(doc['default_odds']), json.dumps(doc['payouts'])))
#
#
# SQLite.cursor().connection.commit()
# bookmaker_sources = SQLite.cursor().execute("""
#     SELECT * FROM bookmaker_sources
# """)
#
# print(bookmaker_sources.fetchall())

