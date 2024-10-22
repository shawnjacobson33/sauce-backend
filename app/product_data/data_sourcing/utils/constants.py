from datetime import datetime


BOOKMAKERS = [
    'BoomFantasy', 'Champ', 'Dabble', 'Drafters', 'DraftKingsPick6', 'HotStreak', 'MoneyLine', 'OddsShopper',
    'OwnersBox', 'ParlayPlay', 'Rebet', 'Sleeper', 'SuperDraft', 'ThriveFantasy', 'Underdog Fantasy', 'VividPicks'
]
VALID_LEAGUES = ['NFL', 'NBA', 'NCAAF', 'WNBA', 'MLB', 'NCAAB', 'NHL']
LEAGUES_SCHEDULE = {
    'NFL': '9-2',
    'NCAAF': '8-1',
    'NBA': '10-6',
    'WNBA': '5-9',
    'MLB': '3-10',
    'NHL': '10-5',
    'NCAAB': '11-3',
}
LEAGUE_SPORT_MAP = {
    'NFL': 'Football',
    'NCAAF': 'Football',
    'NBA': 'Basketball',
    'WNBA': 'Basketball',
    'NCAAB': 'Basketball',
    'MLB': 'Baseball',
    'NHL': 'Ice Hockey',
}
FANTASY_SCORE_MAP = {
    'NBA': 'Basketball Fantasy Points',
    'WNBA': 'Basketball Fantasy Points',
    'WNBA1Q': 'Basketball Fantasy Points',
    'WNBA2Q': 'Basketball Fantasy Points',
    'WNBA3Q': 'Basketball Fantasy Points',
    'WNBA4Q': 'Basketball Fantasy Points',
    'WNBA1H': 'Basketball Fantasy Points',
    'WNBA2H': 'Basketball Fantasy Points',
    'NCAAB': 'Basketball Fantasy Points',
    'NFL': 'Football Fantasy Points',
    'NFL1Q': 'Football Fantasy Points',
    'NFL2Q': 'Football Fantasy Points',
    'NFL3Q': 'Football Fantasy Points',
    'NFL4Q': 'Football Fantasy Points',
    'NFL1H': 'Football Fantasy Points',
    'NFL2H': 'Football Fantasy Points',
    'CFB': 'Football Fantasy Points',
    'CFB1Q': 'Football Fantasy Points',
    'CFB2Q': 'Football Fantasy Points',
    'CFB3Q': 'Football Fantasy Points',
    'CFB4Q': 'Football Fantasy Points',
    'CFB1H': 'Football Fantasy Points',
    'CFB2H': 'Football Fantasy Points',
    'NCAAF': 'Football Fantasy Points',
    'NCAAFB': 'Football Fantasy Points',
    'MLB': 'Baseball Fantasy Points',
    'NHL': 'Ice Hockey Fantasy Points',
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
