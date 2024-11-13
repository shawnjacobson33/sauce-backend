from datetime import datetime

# ********************************************* LEAGUES ***************************************************

VALID_LEAGUES = ['NFL', 'NBA', 'NCAAF', 'WNBA', 'MLB', 'NCAAM', 'NCAAW', 'NHL']
LEAGUES_SCHEDULE = {
    'NFL': '9-2',
    'NCAAF': '8-1',
    'NBA': '10-6',
    'WNBA': '5-9',
    'MLB': '3-10',
    'NHL': '10-5',
    'NCAAM': '11-3',
    'NCAAW': '11-3',
}
LEAGUE_SPORT_MAP = {
    'NFL': 'Football',
    'NCAAF': 'Football',
    'NBA': 'Basketball',
    'WNBA': 'Basketball',
    'NCAAM': 'Basketball',
    'NCAAW': 'Basketball',
    'MLB': 'Baseball',
    'NHL': 'Ice Hockey',
}


def get_in_season_leagues() -> list:
    current_month = datetime.now().month
    in_season_leagues = []

    for league, in_season_months in LEAGUES_SCHEDULE.items():
        start_month, end_month = map(int, in_season_months.split('-'))

        if start_month <= end_month:
            # Non-wrapping range (e.g., March to October)
            if start_month <= current_month <= end_month:
                in_season_leagues.append(league)
        else:
            # Wrapping range (e.g., September to February)
            if current_month >= start_month or current_month <= end_month:
                in_season_leagues.append(league)

    return in_season_leagues


IN_SEASON_LEAGUES = get_in_season_leagues()
IN_SEASON_SPORTS = [LEAGUE_SPORT_MAP[league] for league in IN_SEASON_LEAGUES]
