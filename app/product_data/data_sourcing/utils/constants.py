from datetime import datetime

SUBJECT_COLLECTION_NAME = 'subjects-v3'
MARKETS_COLLECTION_NAME = 'markets-v3'
TEAMS_COLLECTION_NAME = 'teams-v3'

BOOKMAKERS = [
    'BoomFantasy', 'Champ', 'Dabble', 'Drafters', 'DraftKingsPick6', 'HotStreak', 'MoneyLine', 'OddsShopper',
    'OwnersBox', 'ParlayPlay', 'Rebet', 'Sleeper', 'SuperDraft', 'ThriveFantasy', 'Underdog Fantasy', 'VividPicks'
]
VALID_LEAGUES = ['NFL', 'NBA', 'NCAAF', 'WNBA', 'MLB', 'KBO', 'NCAAB', 'NHL', 'NPB']
LEAGUES_SCHEDULE = {
    'NFL': '9-2',
    'NCAAF': '8-1',
    'NBA': '10-6',
    'WNBA': '5-9',
    'MLB': '3-10',
    'NHL': '10-5',
    'NCAAB': '11-3',
    'KBO': '3-10'
}
LEAGUE_SPORT_MAP = {
    'NFL': 'Football',
    'NCAAF': 'Football',
    'NBA': 'Basketball',
    'WNBA': 'Basketball',
    'NCAAB': 'Basketball',
    'MLB': 'Baseball',
    'KBO': 'Baseball',
    'NHL': 'Ice Hockey',
}


def get_in_season_leagues() -> list:
    current_month = datetime.now().month
    in_season_leagues = []
    for league, in_season_months in LEAGUES_SCHEDULE.items():
        in_season_months = in_season_months.split('-')
        if (int(in_season_months[0]) <= current_month <= 12) or (1 <= current_month <= int(in_season_months[-1])):
            in_season_leagues.append(league)

    return in_season_leagues


IN_SEASON_LEAGUES = get_in_season_leagues()
IN_SEASON_SPORTS = [LEAGUE_SPORT_MAP[league] for league in IN_SEASON_LEAGUES]


names = "Kyle Monangai", "Cam Porter", "Kelley Joiner", "Ta'ron Keith", "Nathan Carter", "Montorie Foster"