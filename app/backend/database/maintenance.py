import json
from collections import defaultdict
from datetime import datetime
from typing import Union

from app.backend.database import GAMES_COLLECTION_NAME
from app.backend.database.db import get_db_session
from app.backend.database.utils.definitions import SUBJECTS_COLLECTION_NAME, MARKETS_COLLECTION_NAME, TEAMS_COLLECTION_NAME

MongoDB = get_db_session()

def delete_duplicates(collection, name: str, attribute: str):
    # DELETING DUPLICATES
    counter_dict = defaultdict(int)
    for doc in collection.find():
        if counter_dict[(doc[attribute], doc[name])] > 0:
            collection.delete_one({'_id': doc['_id']})

        counter_dict[(doc[attribute], doc[name])] += 1


def insert_subject(subject_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[SUBJECTS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_subjects.json') as file:
            pending_subjects = json.load(file)
            pending_subjects = [subject for bookmaker, subjects in pending_subjects.items() for subject in subjects if
                             (bookmaker not in exclude) and ((subject['league'], subject['name'],) not in exclude)]
            collection.insert_many(pending_subjects)
    else:
        collection.insert_one({
            'name': subject_data['name'],
            'league': subject_data['league'],
            'team': subject_data['team'],
            'position': subject_data['position'],
            'jersey_number': subject_data['jersey_number']
        })

    delete_duplicates(collection, 'name', 'league')


def insert_market(market_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[MARKETS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_markets.json') as file:
            pending_markets = json.load(file)
            pending_markets = [market for bookmaker, markets in pending_markets.items() for market in markets if
                                (bookmaker not in exclude) and ((market['sport'], market['name'],) not in exclude)]
            collection.insert_many(pending_markets)
    else:
        collection.insert_one(market_data)

    delete_duplicates(collection, 'name', 'sport')


def insert_team(team_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[TEAMS_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_teams.json') as file:
            pending_teams = json.load(file)
            pending_teams = [team_obj for bookmaker, teams in pending_teams.items() for team_obj in teams if
                             (bookmaker not in exclude) and ((team_obj['league'], team_obj['abbr_name'],) not in exclude)]
            collection.insert_many(pending_teams)
    else:
        collection.insert_one(team_data)

    delete_duplicates(collection, 'abbr_name', 'league')


def insert_game(game_data: dict = None, insert_all: bool = False, exclude: list[Union[tuple[str, str], str]] = None):
    collection = MongoDB[GAMES_COLLECTION_NAME]
    if insert_all:
        with open('../../data_collection/utils/reports/pending_teams.json') as file:
            pending_teams = json.load(file)
            pending_teams = [team_obj for bookmaker, teams in pending_teams.items() for team_obj in teams if
                             (bookmaker not in exclude) and ((team_obj['league'], team_obj['abbr_name'],) not in exclude)]
            collection.insert_many(pending_teams)
    else:
        game_data['game_time'] = datetime.strptime(game_data['game_time'], "%Y-%m-%d %H:%M:%S.%f")
        collection.insert_one(game_data)

    delete_duplicates(collection, 'time_processed', 'league')




# insert_subject()
# market = {
#     'name': 'Plus Minus',
#     'sport': 'Ice Hockey'
# }
# insert_market(
#     market_data=market
# )
# team = {
#     'abbr_name': 'MISS',
#     'full_name': "Ole Miss",
#     'league': 'NCAA'
# }
# insert_team(
#     team_data=team
# )
# game = {
#         "away_team": {
#             "league": "NBA",
#             "abbr_name": "MEM",
#             "id": "672e0516e3b845de5fe88748"
#         },
#         "game_time": "2024-11-15 22:00:48.845000",
#         "home_team": {
#             "league": "NBA",
#             "abbr_name": "GSW",
#             "id": "672e0516e3b845de5fe88741"
#         },
#         "box_score_url": "NBA_20241115_MEM@GS",
#         "league": "NBA",
#         "source": "cbssports-nba",
#         "time_processed": "2024-11-15 20:09:50.423000"
#     }
#
# insert_game(
#     game_data=game
# )



from bs4 import BeautifulSoup
import requests

subjects = MongoDB.get_collection(SUBJECTS_COLLECTION_NAME)
teams = MongoDB.get_collection(TEAMS_COLLECTION_NAME)
team_url = 'https://www.cbssports.com/college-basketball/teams/'



def get_teams(division: str):
    return parse_team_names(requests.get(team_url).text, division)


def parse_team_names(html_content, division):
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    teams_list = list()
    for table, span in zip(tables, soup.find_all('span', {'class': 'TeamLogoNameLockup-name'})):
        if division == span.text.strip():
            for row in table.find_all('tr')[1:]:
                link_elem = row.find('span', {'class': 'TeamName'}).find('a')
                abbr_name, full_name = link_elem.get('href').split("/")[3], link_elem.text
                teams_list.append((abbr_name, full_name))

    return teams_list


def get_subjects(league: str, abbr_team: str, full_team: str, f_league: str = None, f_abbr_team: str = None):
    roster_url = 'https://www.cbssports.com/{}/teams/{}/{}/roster/'
    roster_url = roster_url.format(league.lower(), abbr_team, '-'.join(full_team.lower().split()))
    response = requests.get(roster_url)
    if league == 'NBA' or f_league == 'NCAA':
        return parse_basketball_subjects(response.text, roster_url, f_league, f_abbr_team)
    elif league == 'NFL':
        return parse_football_subjects(response.text, f_abbr_team)
    elif league == 'NHL':
        return parse_ice_hockey_subjects(response.text, f_abbr_team)


def parse_basketball_subjects(html_content, url: str, f_league: str = None, f_abbr_team: str = None):
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    subjects_data = list()
    url_comps = url.split('/')
    filter_condition = {'league': url_comps[3].upper() if not f_league else f_league, 'abbr_name': url_comps[5] if not f_abbr_team else f_abbr_team}
    team_id = str(teams.find_one(filter_condition, {'_id': 1})['_id'])
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        name = cells[1].find('span', {'class': 'CellPlayerName--long'}).find('a').text.strip()
        subject = {
            'jersey_number': cells[0].text.strip(),
            'name': name,
            'position': cells[2].text.strip(),
            'league': url_comps[3].upper() if not f_league else f"{f_league}M",
            'team_id': team_id
        }
        subjects_data.append(subject)


    return subjects_data


def parse_football_subjects(html_content, f_abbr_team: str = None):
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')  # NFL HAS 3 TABLES (off, def, spec)
    subjects_data = list()
    for table in tables:
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            position = cells[2].text.strip()
            if position not in {'OT', 'G', 'C', 'LS', 'OG', 'OL'}:
                url_comps = url.split('/')
                name = cells[1].find('span', {'class': 'CellPlayerName--long'}).find('a').text.strip()
                subject = {
                    'jersey_number': cells[0].text.strip(),
                    'name': name,
                    'position': position,
                    'league': url_comps[3].upper(),
                    'team_id': str(teams.find_one({'league': url_comps[3].upper(), 'abbr_name': url_comps[5] if not f_abbr_team else f_abbr_team}, {'_id': 1})['_id'])
                }
                subjects_data.append(subject)


    return subjects_data


def parse_ice_hockey_subjects(html_content, f_abbr_team: str = None):
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')  # NFL HAS 3 TABLES (off, def, spec)
    subjects_data = list()
    url_comps = url.split('/')
    team_id = str(
        teams.find_one({'league': url_comps[3].upper(), 'abbr_name': url_comps[5] if not f_abbr_team else f_abbr_team},
                       {'_id': 1})['_id'])
    for table in tables:
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            subject = {
                'jersey_number': cells[0].text.strip(),
                'name': cells[1].find('span', {'class': 'CellPlayerName--long'}).find('a').text.strip(),
                'position': cells[2].text.strip(),
                'league': url_comps[3].upper(),
                'team_id': team_id
            }
            subjects_data.append(subject)


    return subjects_data



# team_names = get_teams(division='ACC')
actual_team_names = ['LVILLE', 'STNFRD']
expected_team_names = ['LOU', '']

for abbr, full in team_names:
    try:
        if data := get_subjects('college-basketball', abbr, full, 'NCAA'):
            subjects.insert_many(data)
            print(abbr, full, 'collected')
        else:
            print(abbr, full, 'url')

    except Exception:
        print(abbr, full, 'database')

# subjects.delete_many({'league': 'NCAAM'})

