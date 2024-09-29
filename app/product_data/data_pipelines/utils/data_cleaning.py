import re


leagues_mapping = {
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


def clean_league(league: str):
    # run the .replace() command for college football instead of hashing it into a dictionary because PrizePicks
    # weird ass segments league names for each quarter, half, full game. Ex: CFB1Q, CFB1H, CFB
    cleaned_league = league.strip().upper().replace('CFB', 'NCAAF')
    return leagues_mapping.get(cleaned_league, cleaned_league)


def clean_market(market: str):
    market_map = {'Hitter Fantasy Score': 'Baseball Fantasy Points'}
    # Initialize some values to reduce redundancy
    n, is_uppercase = len(market), market.isupper()
    # 'Total' is unnecessary to include in a market name -- located here so the next line will strip any extra space
    cleaned_market = market.replace('Total', '').replace('Plus', '+')
    # BoomFantasy annoyingly uses underscore as spaces
    cleaned_market = cleaned_market.replace('_', ' ')
    # For example 'RBI' is fine to be uppercase but BATTER WALKS and BASES isn't
    if ((' ' in cleaned_market) or n >= 4) and (cleaned_market.islower() or is_uppercase):
        # Payday capitalizes every letter in the market name
        cleaned_market = cleaned_market.title()

    # remove any words in parentheses
    if cleaned_market[-1] == ')':
        # include all but up to the space and open parenthesis
        cleaned_market = cleaned_market[:cleaned_market.index(' (')]

    if n < 4 and not is_uppercase:
        cleaned_market = cleaned_market.upper()

    # Don't move because the .title() needs to go first
    cleaned_market = cleaned_market.replace(' And ', ' + ').replace('Player ', '')
    # Use regex to find '+' that doesn't already have spaces around it
    cleaned_market = re.sub(r'(?<!\s)\+(?!\s)', ' + ', cleaned_market)
    # Use regex to add a space before uppercase letters that are immediately after lowercase letters
    cleaned_market = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_market)

    return market_map.get(cleaned_market.strip(), cleaned_market.strip())


def clean_subject(subject: str):
    # Define suffixes in a list with regex patterns to ensure they are standalone with spaces
    suffixes = [
        'Jr.', 'jr.', 'jr', 'Jr', 'Sr.', 'sr.', 'sr', 'Sr', 'III', 'II', 'IV', 'V'
    ]
    for suffix in suffixes:
        subject = re.sub(fr' {suffix}$', '', subject)

    return subject.strip()


print("AST Tackles".isupper())