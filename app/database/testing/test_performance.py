import time

import pandas as pd
import polars as pl
from Levenshtein import distance

from app.db.data_hub import Subjects


# TESTING PERFORMANCE OF DIFFERENT LIBRARIES/DATA STRUCTURES FOR SUBJECT ID LOOKUPS
def test_subject_id_lookup():
    data = [{'id': doc['_id'], 'name': doc['name'], **doc['attributes']} for doc in SUBJECTS_CURSOR.find({}, {'batch_id': 0})]

    # TESTING CURSOR
    t0 = time.time()
    for doc in data:
        data_filter = {'name': doc['name'], 'attributes.league': doc['league'], 'attributes.team': doc['team'], 'attributes.position': doc['position'], 'attributes.jersey_number': doc['jersey_number']}
        subject_id = SUBJECTS_CURSOR.find(data_filter, {'_id': 1})

    t1 = time.time()
    cursor_time = t1-t0
    print("Cursor:", cursor_time)

    # TESTING DICTIONARY
    dictionary = {(doc['name'], doc['league'], doc['team'], doc['position'], doc['jersey_number']): doc['id'] for doc in data}
    t0 = time.time()
    for doc in data:
        subject_id = dictionary[(doc['name'], doc['league'], doc['team'], doc['position'], doc['jersey_number'])]

    t1 = time.time()
    dict_time = t1 - t0
    print("Dictionary:", dict_time)

    # TESTING PANDAS
    pandas_df = pd.DataFrame(data)
    pandas_df.set_index(['name', 'league', 'team', 'position', 'jersey_number'], inplace=True)
    t0 = time.time()
    for doc in data:
        subject_id = pandas_df.loc[(doc['name'], doc['league'], doc['team'], doc['position'], doc['jersey_number'])]['id']

    t1 = time.time()
    pandas_time = t1 - t0
    print("Pandas:", pandas_time)

    # TESTING POLARS
    polars_df = pl.DataFrame(data)
    t0 = time.time()
    for doc in data:
        subject_id = polars_df.filter(
            (pl.col('league') == doc['league']) &
            (pl.col('name') == doc['name']) &
            (pl.col('team') == doc['team']) &
            (pl.col('position') == doc['position']) &
            (pl.col('jersey_number') == doc['jersey_number'])
        ).select('id')

    t1 = time.time()
    polars_time = t1 - t0
    print("Polars:", polars_time)


# TESTING PERFORMANCE OF DIFFERENT LIBRARIES/DATA STRUCTURES FOR SUBJECT ID LOOKUPS
def test_applying_similarity_scores():
    def get_distances(r, doc):
        # so weights can be adjusted to be more strict if there is fewer data points on a subject
        num_data_points = 0
        # initialize the overall distance with the subjects distance ---- weight goes down to 0.75 ----
        total_distance = 0
        if doc['team'] and r['team']:
            num_data_points += 1
            # More variance with team name formatting in college football so more leniency on distance for them
            weight = 1 if r['league'] not in {'NCAAF'} else 0.75
            total_distance += distance(r['team'], doc['team']) * weight

        if doc['position'] and r['position']:
            num_data_points += 1
            # More variance with position formatting especially in MLB and NBA/WNBA so more lenient on distance
            total_distance += distance(r['position'], doc['position']) * 0.75

        if doc['jersey_number'] and r['jersey_number']:
            num_data_points += 1
            # shouldn't be many cases where jersey numbers don't match but subjects do so punish distance more
            total_distance += distance(r['jersey_number'], doc['jersey_number']) * 2

        total_distance += distance(r['name'], doc['name']) * (1 - num_data_points * 0.0625)
        return total_distance

    data = [{'id': doc['_id'], 'name': doc['name'], **doc['attributes']} for doc in
            SUBJECTS_CURSOR.find({}, {'batch_id': 0})]

    # TESTING PANDAS
    pandas_df = pd.DataFrame(data)
    t0 = time.time()
    for doc in data:
        pandas_df['distance'] = pandas_df.apply(get_distances, axis=1, doc=doc)
        similar_subject = pandas_df.sort_values(by='distance').iloc[0]

    t1 = time.time()
    pandas_time = t1 - t0
    print("Pandas:", pandas_time)

    # TESTING POLARS


test_subject_id_lookup()
# test_applying_similarity_scores()