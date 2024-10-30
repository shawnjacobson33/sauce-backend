import re


LEAGUE_MAP = {
    # BOOM FANTASY
    'NCAAFB': 'NCAAF',  # and HotStreak
    'MTEN': 'TENNIS',
    'WTEN': 'TENNIS',
    'HDOG': 'HOTDOG',
    'NASCARMANUAL': 'RACING',
    'F1': 'RACING',
    'NASCAR': 'RACING',
    'INDYCAR': 'RACING',
    # HotStreak
    'VALORANT': 'VAL',
    'CCT EUROPE': 'CS',
    'ESL PRO LEAGUE': 'CS',
    'THUNDERPICK WORLD CHAMPIONSHIP': 'CS',
    'YALLA COMPASS': 'CS',
    'CCT SOUTH AMERICA': 'CS',
    'CCT NORTH AMERICA': 'CS',
    'ELISA INVITATIONAL': 'CS',
    'ESL CHALLENGER LEAGUE': 'CS',
    'EUROPEAN PRO LEAGUE': 'CS',
    'WINLINE INSIGHT': 'DOTA',
    'FASTCUP': 'CS',
    'LCK': 'LOL',
    'CS2': 'CS',
    'CSGO': 'CS',
    # OddsShopper
    'WTA': 'TENNIS',
    'ATP': 'TENNIS',
    'TENNIS W': 'TENNIS',
    'TENNIS M': 'TENNIS',
    # PrizePicks
    'MLBLIVE': 'MLB',
    'UFC': 'MMA',
    'EPL': 'SOCCER',
    'MLS': 'SOCCER',
    'UCL': 'SOCCER',
    'CHAMPIONSLEAGUE': 'SOCCER',
    'LALIGA': 'SOCCER',
    'BUNDES': 'SOCCER',
    'SERIEA': 'SOCCER',
    'LIGUE1': 'SOCCER',
    'EUROGOLF': 'GOLF',
    'LIVGOLF': 'GOLF',
    'AUSSIE': 'AFL',
    'DOTA2': 'DOTA',
    'ESEA': 'CS',
    # Draftkings Pick6
    'PGA TOUR': 'GOLF',
    'PGA': 'GOLF',
    'WSOCCER': 'SOCCER',
    'MSOCCER': 'SOCCER',
    'FIFA': 'SOCCER',
}
POSITION_MAP = {
    'Forward': 'F',
    'Pitcher': 'P',
    'Defender': 'D',
    'Hitter': 'H',
    'Guard': 'G',
    'Forward-Center': 'F-C',
}
SUBJECT_SUFFIXES = [
    'Jr.', 'jr.', 'jr', 'Jr', 'Sr.', 'sr.', 'sr', 'Sr', 'III', 'II', 'IV', 'V'
]


def clean_league(league: str) -> str:
    # run the .replace() command for college football instead of hashing it into a dictionary because PrizePicks
    # weird ass segments league names for each quarter, half, full game. Ex: CFB1Q, CFB1H, CFB
    cleaned_league = league.strip().upper().replace('CFB', 'NCAAF')
    return LEAGUE_MAP.get(cleaned_league, cleaned_league)


def clean_subject(subject: str) -> str:
    # Define suffixes in a list with regex patterns to ensure they are standalone with spaces
    for suffix in SUBJECT_SUFFIXES:
        subject = re.sub(fr' {suffix}$', '', subject)

    return subject.strip()


def clean_position(position: str) -> str:
    """Small formatting fixes for positions"""
    return POSITION_MAP.get(position, position)
