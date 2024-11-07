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
    'CBB': 'NCAAM',
    'WCBB': 'NCAAW',
    'CFB': 'NCAAF',
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


def clean_league(league: str) -> str:
    # run the .replace() command for college football instead of hashing it into a dictionary because PrizePicks
    # league names for each quarter, half, full game. Ex: CFB1Q, CFB1H, CFB
    cleaned_league = league.strip().upper()
    return LEAGUE_MAP.get(cleaned_league, cleaned_league)