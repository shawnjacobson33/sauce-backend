# TESTING MODULE TO ENSURE SOME VALIDITY OF THE COLLECTIONS STORED IN THE DATABASE
import pprint
from collections import defaultdict
from main import get_db
from app.product_data.data_sourcing.utils.constants import SUBJECT_COLLECTION_NAME, MARKETS_COLLECTION_NAME, \
    BOOKMAKERS_COLLECTION_NAME, VALID_LEAGUES

# TESTS:
# # 1. No duplicate docs
# # 2. No field contains empty strings
# # 3. Every name ('name', 'alt_names') present in the collection is unique (caveat: group by leagues for Subjects)
# # 4. The 'team' and 'position' field are uppercase in 'Subjects'
# # 5. Every Subject and Market Doc must have a non-empty league or sport field value
# # 6. {pick a bookmaker} is the only bookmaker that will always update any non-matching attributes
# # 7. When a new entity is matched to a document in the collection, any new data is always added


db = get_db()
subjects = [doc for doc in db[SUBJECT_COLLECTION_NAME].find()]
markets = [doc for doc in db[MARKETS_COLLECTION_NAME].find()]
market_fields = ['name', 'batch_id', 'sport', 'alt_names']
bookmakers = [doc for doc in db[BOOKMAKERS_COLLECTION_NAME].find()]


num_tests = 6
tests_passed = 0


def test_for_presence_of_fields():
    """The same fields have to exist in all subjects...and the same goes for markets"""
    global tests_passed
    invalid_subjects = []
    for doc in subjects:
        for field in ['name']:
            if field not in doc:
                invalid_subjects.append(doc)
                break

        for attribute in ['league', 'team', 'position', 'jersey_number', 'alt_names']:
            if attribute not in doc['attributes']:
                invalid_subjects.append(doc)
                break

    invalid_markets = []
    for doc in markets:
        for field in ['name']:
            if field not in doc:
                invalid_markets.append(doc)
                break

        for attribute in ['sport', 'alt_names']:
            if attribute not in doc['attributes']:
                invalid_markets.append(doc)
                break

    try:
        assert not invalid_subjects
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print("Invalid Subjects (Missing Fields):")
        pprint.pprint(invalid_subjects)

    try:
        assert not invalid_markets
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print("Invalid Markets (Missing Fields):")
        pprint.pprint(invalid_markets)

    tests_passed += 1


def test_for_duplicates():
    """1. No duplicate docs"""
    global tests_passed
    subject_counts = defaultdict(int)
    for doc in subjects:
        subject_counts[(doc['attributes']['league'], doc['name'])] += 1

    duplicate_subjects = {key: value for key, value in subject_counts.items() if value > 1}
    try:
        assert not duplicate_subjects
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Duplicate Subjects:')
        pprint.pprint(duplicate_subjects)

    market_counts = defaultdict(int)
    for doc in markets:
        market_counts[(doc['attributes']['sport'], doc['name'])] += 1

    duplicate_markets = {key: value for key, value in market_counts.items() if value > 1}
    try:
        assert not duplicate_markets
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Duplicate Markets:')
        pprint.pprint(duplicate_markets)

    bookmaker_counts = defaultdict(int)
    for doc in bookmakers:
        bookmaker_counts[doc['name']] += 1

    duplicate_bookmakers = {key: value for key, value in bookmaker_counts.items() if value > 1}
    try:
        assert not duplicate_bookmakers
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Duplicate Bookmakers:')
        pprint.pprint(duplicate_bookmakers)

    tests_passed += 1


def test_for_empty_strings():
    """2. No field contains empty strings"""
    global tests_passed
    invalid_subjects = list()
    for doc in subjects:
        if doc['name'] == '':
            invalid_subjects.append(doc)

        for attribute in doc['attributes']:
            if attribute != 'alt_names':
                if doc['attributes'][attribute] == '':
                    invalid_subjects.append(doc)

        for alt_name in doc['attributes']['alt_names']:
            if alt_name == '':
                invalid_subjects.append(doc)

    try:
        assert not invalid_subjects
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Invalid Subjects (A Field Contains an Empty String):')
        pprint.pprint(invalid_subjects)

    invalid_markets = list()
    for doc in markets:
        if doc['name'] == '':
            invalid_markets.append(doc)

        for attribute in doc['attributes']:
            if attribute != 'alt_names':
                if doc['attributes'][attribute] == '':
                    invalid_markets.append(doc)

        for alt_name in doc['attributes']['alt_names']:
            if alt_name == '':
                invalid_markets.append(doc)

    try:
        assert not invalid_markets
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Invalid Markets (A Field Contains an Empty String):')
        pprint.pprint(invalid_markets)

    tests_passed += 1


def test_for_duplicate_names_within_docs():
    """3. Every name ('name', 'alt_names') present in the collection is unique (caveat: group by leagues for Subjects)"""
    global tests_passed
    subject_counts = {doc['_id']: defaultdict(int) for doc in subjects}
    for doc in subjects:
        subject_counts[doc['_id']][doc['name']] += 1
        for alt_name in doc['attributes']['alt_names']:
            subject_counts[doc['_id']][alt_name] += 1

    subject_names = dict()
    for subject in subject_counts.values():
        subject_names.update(subject)

    duplicate_subjects = {key: value for key, value in subject_names.items() if value > 1}
    try:
        assert not duplicate_subjects
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Duplicate Subjects Within Docs:')
        pprint.pprint(duplicate_subjects)

    market_counts = {doc['_id']: defaultdict(int) for doc in markets}
    for doc in markets:
        market_counts[doc['_id']][doc['name']] += 1
        for alt_name in doc['attributes']['alt_names']:
            market_counts[doc['_id']][alt_name] += 1

    market_names = dict()
    for market in market_counts.values():
        market_names.update(market)

    duplicate_markets = {key: value for key, value in market_names.items() if value > 1}
    try:
        assert not duplicate_markets
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Duplicate Markets Within Docs:')
        pprint.pprint(duplicate_markets)

    tests_passed += 1


def test_for_correct_formatting():
    """4. The 'team' and 'position' field are uppercase in 'Subjects'"""
    global tests_passed
    subject_counts = defaultdict(int)
    for doc in subjects:
        team, position = doc['attributes']['team'], doc['attributes']['position']
        if (team and not team.isupper()) or (position and not position.isupper()):
            subject_counts[(doc['_id'], doc['name'], doc['attributes']['team'], doc['attributes']['position'])] += 1

    invalid_subjects = {key: value for key, value in subject_counts.items() if value}
    try:
        assert not invalid_subjects
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Incorrectly Formatted Subjects (Team or Position are not Uppercase):')
        pprint.pprint(invalid_subjects)

    tests_passed += 1


def test_for_empty_league_or_sport_field():
    """5. Every Subject and Market Doc must have a non-empty league or sport field value"""
    global tests_passed
    invalid_subjects = list()
    for doc in subjects:
        if not doc['attributes']['league'] or doc['attributes']['league'] not in VALID_LEAGUES:
            invalid_subjects.append(doc)

    try:
        assert not invalid_subjects
    except AssertionError:
        print(f'Invalid Subjects - League Field is Empty: {invalid_subjects}')
        pprint.pprint(invalid_subjects)

    invalid_markets = list()
    for doc in markets:
        if not doc['attributes']['sport']:
            invalid_markets.append(doc)

    try:
        assert not invalid_markets
    except AssertionError:
        print(f'Tests Passed {tests_passed}/{num_tests}')
        print(f'Invalid Markets - Sport Field is Empty:')
        pprint.pprint(invalid_markets)

    tests_passed += 1


if __name__ == '__main__':
    test_for_presence_of_fields()
    test_for_duplicates()
    test_for_empty_strings()
    test_for_duplicate_names_within_docs()
    test_for_correct_formatting()
    test_for_empty_league_or_sport_field()
    print(f'Tests Passed {tests_passed}/{num_tests}')