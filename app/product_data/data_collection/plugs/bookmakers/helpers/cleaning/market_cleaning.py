from typing import Optional


MARKET_MAP = {
    'Basketball': {
      # Points
        'POINTS': 'Points',  # BoomFantasy
      # Rebounds
        'REBOUNDS': 'Rebounds',  # BoomFantasy
      # Assists
        'ASSISTS': 'Assists',  # BoomFantasy
      # Points + Rebounds
        'Pts + Reb': 'Points + Rebounds',  # ParlayPlay
        'Pts + Rebs': 'Points + Rebounds',  # OwnersBox
        'pr': 'Points + Rebounds',  # HotStreak
        'POINTS_AND_REBOUNDS': 'Points + Rebounds',  # BoomFantasy
      # Points + Assists
        'Pts + Ast': 'Points + Assists',  # ParlayPlay
        'Pts + Asts': 'Points + Assists',  # OwnersBox
        'pa': 'Points + Assists',  # HotStreak
        'POINTS_AND_ASSISTS': 'Points + Assists',  # BoomFantasy
      # Rebounds + Assists
        'Reb + Ast': 'Rebounds + Assists',  # ParlayPlay
        'Reb + Asts': 'Rebounds + Assists',  # OwnersBox
        'ra': 'Rebounds + Assists',  # HotStreak
        'REBOUNDS_AND_ASSISTS': 'Rebounds + Assists',  # BoomFantasy
        'Assists + Rebounds': 'Rebounds + Assists',  # DraftKings Pick6
      # Points + Rebounds + Assists
        'Pts + Reb + Ast': 'Points + Rebounds + Assists',  # ParlayPlay, BetOnline
        'Pts + Rebs + Asts': 'Points + Rebounds + Assists',  # OwnersBox
        'pra': 'Points + Rebounds + Assists',  # HotStreak
        'POINTS_REBOUNDS_ASSISTS': 'Points + Rebounds + Assists',  # BoomFantasy
        'Points + Assists + Rebounds': 'Points + Rebounds + Assists',  # DraftKings Pick6
      # 3-Pointers Made
        'Three Point Field Goals Made': '3-Pointers Made',  # BetOnline
        'MADE_THREE_POINTERS': '3-Pointers Made',  # BoomFantasy
        '3PT Made': '3-Pointers Made',  # Dabble, ParlayPlay
        '3-Pointers Made': '3-Pointers Made',  # DraftKings Pick6, OwnersBox
        '3-Pointers': '3-Pointers Made',  # OddsShopper
      # 3-Pointers Attempted
        '3PT Attempted': '3-Pointers Attempted',  # Dabble
      # 2-Pointers Made
        '2PT Made': '2-Pointers Made',  # Dabble
      # 2-Pointers Attempted
        '2PT Attempted': '2-Pointers Attempted',  # Dabble
      # Field Goals Made
      # Field Goals Attempted
        'FG Attempted': 'Field Goals Attempted',  # Dabble
      # Free Throws Made
        'FT Made': 'Free Throws Made',  # Dabble
      # Free Throws Attempted
        'FT Attempted': 'Free Throws Attempted',  # Dabble
      # Double Doubles
        'Double Double': 'Double Doubles',  # ParlayPlay
        'Total Double Doubles': 'Double Doubles',  # OddsShopper
      # Triple Doubles
        'Triple Double': 'Triple Doubles',  # ParlayPlay
        'Total Triple Doubles': 'Triple Doubles',  # OddsShopper
      # Turnovers
        'TURNOVERS': 'Turnovers',  # BoomFantasy
      # Steals
        'STEALS': 'Steals',  # BoomFantasy
      # Blocks
        'BLOCKS': 'Blocks',  # BoomFantasy
        'Blocked Shots': 'Blocks',  # OwnersBox
      # Blocks + Steals
        'BLOCKS_AND_STEALS': 'Blocks + Steals',  # BoomFantasy
        'stocks': 'Blocks + Steals',  # HotStreak
        'Steals + Blocks': 'Blocks + Steals',  # DraftKings Pick6, OddsShopper, OwnersBox
        'Stl + Blk': 'Blocks + Steals',  # ParlayPlay
        # Fantasy Points
          # TODO: RECHECK BOOKMAKERS
    },
    'Football': {
        # Passing Yards
          'PASSING_YARDS': 'Passing Yards',  # BoomFantasy
          'Total Passing Yards': 'Passing Yards',  # OddsShopper
          'Pass Yds': 'Passing Yards',  # ParlayPlay
        # Passing Attempts
          'Pass Attempts': 'Passing Attempts',  # BetOnline, ParlayPlay
          'PASSING_ATTEMPTS': 'Passing Attempts',  # BoomFantasy
          'Total Passing Attempts': 'Passing Attempts',  # OddsShopper
        # Completions
          'Completions': 'Completions',  # DraftKingsPick6, OwnersBox
          'Passing Completions': 'Completions',  # Dabble
          'Pass Completions': 'Completions',  # BetOnline, ParlayPlay
          'PASSING_COMPLETIONS': 'Completions',  # BoomFantasy
          'Total Pass Completions': 'Completions',   # OddsShopper
        # Passing Touchdowns
          'Passing Touchdowns': 'Passing Touchdowns',  # MoneyLine, OwnersBox
          'Passing TDs': 'Passing Touchdowns',  # BetOnline, ParlayPlay
          'PASSING_TOUCHDOWNS': 'Passing Touchdowns',  # BoomFantasy
          'Total Passing Touchdowns': 'Passing Touchdowns',  # OddsShopper
          'Pass TDs': 'Passing Touchdowns',  # ParlayPlay
        # Interceptions Thrown -- IMPORTANT FOR SOME BOOKMAKERS THEY FORMAT Interceptions as Passing Interceptions, AND SOME DO Interceptions as actual Interceptions caught.
          'Pass Interceptions': 'Interceptions Thrown',  # BetOnline
          'INTERCEPTIONS_THROWN': 'Interceptions Thrown',  # BoomFantasy
          'Interceptions Thrown': 'Interceptions Thrown',  # DraftKingsPick6
          'Total Interceptions Thrown': 'Interceptions Thrown',  # OddsShopper
        # Longest Passing Completion
          'Longest Passing Completion': 'Longest Passing Completion',  # OddsShopper
          'Longest Passing Completion (Yards)': 'Longest Passing Completion',  # Dabble
          'Longest Pass': 'Longest Passing Completion',  # ParlayPlay
        # Rushing Yards
          'RUSHING_YARDS': 'Rushing Yards',  # BoomFantasy
          'Total Rushing Yards': 'Rushing Yards',  # OddsShopper
          'Rush Yds': 'Rushing Yards',  # ParlayPlay
        # Carries
          'Carries': 'Carries',  # BetOnline
          'RUSHING_ATTEMPTS': 'Carries',  # BoomFantasy
          'Rushing Attempts': 'Carries',  # Dabble, OwnersBox
          'Total Rushing Attempts': 'Carries',  # OddsShopper
          'Rush Attempts': 'Carries',  # ParlayPlay
        # Rushing Touchdowns
          'RUSHING_TOUCHDOWNS': 'Rushing Touchdowns',  # BoomFantasy
          'Total Rushing Touchdowns': 'Rushing Touchdowns',  # OddsShopper
        # Longest Rush
          'Longest Rush': 'Longest Rush',  # OddsShopper, OwnersBox, ParlayPlay
          'Longest Rush (Yards)': 'Longest Rush',  # Dabble
        # Receiving Yards
          'RECEIVING_YARDS': 'Receiving Yards',  # BoomFantasy
          'Rec Yds': 'Receiving Yards',  # ParlayPlay
        # Targets
        # Receptions
          'RECEPTIONS': 'Receptions',  # BoomFantasy
          'Total Receptions': 'Receptions',  # OddsShopper
        # Receiving Touchdowns
          'RECEIVING_TOUCHDOWNS': 'Receiving Touchdowns',  # BoomFantasy
          'Total Receiving Touchdowns': 'Receiving Touchdowns',  # OddsShopper
        # Longest Reception
          'Longest Reception': 'Longest Reception',  # OddsShopper, OwnersBox, ParlayPlay
          'Longest Reception (Yards)': 'Longest Reception',  # Dabble
        # Passing + Rushing Touchdowns
        # Passing + Rushing Yards
          'Passing Yards + Rushing Yards': 'Passing + Rushing Yards',  # Dabble
          'Total Pass + Rush Yards': 'Passing + Rushing Yards',  # OddsShopper
          'Total Passing + Rushing Yards': 'Passing + Rushing Yards',  # OddsShopper
          'Pass + Rush Yards': 'Passing + Rushing Yards',  # OwnersBox, ParlayPlay
        # Rushing + Receiving Yards
          'Rushing + Receiving Yards': 'Rushing + Receiving Yards',  # MoneyLine
          'Receiving Yards + Rushing Yards': 'Rushing + Receiving Yards',  # Dabble
          'Rush + Rec Yards': 'Rushing + Receiving Yards',  # DraftKingsPick6, OwnersBox, ParlayPlay
          'Total Rush + Rec Yards': 'Rushing + Receiving Yards',  # OddsShopper
          'Total Rushing + Receiving Yards': 'Rushing + Receiving Yards',  # OddsShopper
        # Rushing + Receiving Touchdowns
          'Rushing + Receiving Touchdowns': 'Rushing + Receiving Touchdowns',  # MoneyLine
          'Receiving + Rushing Touchdowns': 'Rushing + Receiving Touchdowns',  # Dabble
          'Rush + Rec TDs': 'Rushing + Receiving Touchdowns',  # DraftKingsPick6, OwnersBox
          'Total Rushing + Receiving TDs': 'Rushing + Receiving Touchdowns',  # OddsShopper
          'Rush + Rec Td': 'Rushing + Receiving Touchdowns',  # ParlayPlay
        # Total Touchdowns -- Includes Defensive Players
          'Total Touchdowns': 'Total Touchdowns',  # OddsShopper
          'TOTAL_TOUCHDOWNS': 'Total Touchdowns',  # BoomFantasy
          'Total Passing + Rushing + Receiving TDs': 'Total Touchdowns',  # OddsShopper
        # Fantasy Points
          # TODO: RECHECK BOOKMAKERS
          'Fantasy Points': 'Fantasy Points',  # ParlayPlay
        # Sacks
          'Total Sacks': 'Sacks',  # OddsShopper
        # Total Tackles
          'Tackles': 'Total Tackles',  # BetOnline
          'Tackles + Assists': 'Total Tackles',  # DraftKingsPick6, OwnersBox
          'Total Tackles + Assists': 'Total Tackles',  # OddsShopper
          'Tackles S+A': 'Total Tackles',  # ParlayPlay
        # Tackle Assists
          'Tackle Assists': 'Tackle Assists',  # Dabble
          'Total Assists': 'Tackle Assists',   # OddsShopper
        # Solo Tackles
          'Solo Tackles': 'Solo Tackles',  # OwnersBox, ParlayPlay
          'Tackle Tackles': 'Solo Tackles',  # OddsShopper
        # Interceptions -- IMPORTANT FOR SOME BOOKMAKERS THEY FORMAT Interceptions as Passing Interceptions, AND SOME DO Interceptions as actual Interceptions caught.
           # Dabble, OwnersBox -- "Interceptions" TODO: IMPLEMENT LOGIC
           # ParlayPlay -- "Interception"
        # Kicking Points
          'Kicking Points': 'Kicking Points',  # DraftKingsPick6, OwnersBox
          'Total Kicking Points': 'Kicking Points',  # OddsShopper
        # Extra Points Made
          'Total Extra Points Made': 'Extra Points Made',  # OddsShopper
          'XP Made': 'Extra Points Made',  # OwnersBox
        # Field Goals Made
          'Total Field Goals Made': 'Field Goals Made',  # OddsShopper
          'FG Made': 'Field Goals Made',  # OwnersBox
        # First Touchdown Scorer
          'First Touchdown Scorer': 'First Touchdown Scorer',  # OddsShopper
        # Last Touchdown Scorer
          'Last Touchdown Scorer': 'Last Touchdown Scorer',  # OddsShopper
    },
    'hot_streak': {
        'pa': 'Points + Assists',
        'pr': 'Points + Rebounds',
        'ra': 'Rebounds + Assists',
        'pra': 'Points + Rebounds + Assists',
        'stocks': 'Steals + Blocks'
    }, 'odds_shopper': {
        'Total Rushing + Receiving Yards': 'Total Rush + Rec Yards',
        'Total Passing + Rushing Yards': 'Total Pass + Rush Yards',
        'Total Passing + Rushing + Receiving TDs': 'Total Pass + Rush + Rec TDs',
    }
}
FANTASY_KEY_WORDS = {'fantasy_points', 'Fantasy Points', 'Fantasy Score', 'Player Fantasy Score', 'Fantasy'}
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
PERIOD_CLASSIFIER_MAP = {
    'firstQuarter': '1Q',
    'firstHalf': '1H'
}  # for boom fantasy


def clean_period_classifier(period_classifier: Optional[str]) -> Optional[str]:
    # if it exists in the dictionary map, get it and keep executing
    cleaned_period_classifier = PERIOD_CLASSIFIER_MAP.get(period_classifier)
    # get the cleaned period classifier
    formatted_cleaned_period = f'{cleaned_period_classifier} ' if cleaned_period_classifier else ""
    # return the formatted and cleaned period classifier
    return formatted_cleaned_period


def capitalize_first_letters(market: str) -> str:
    # capitalize first letter of each word (POINTS REBOUNDS -> Points Rebounds)
    return market.title()


def remove_parentheses_words(market: str) -> str:
    # get rid of any clarifying words in parentheses (Longest Passing Completion (Yards) -> Longest Passing Completion)
    return market.split(' (')[0] if '(' in market else market


def remove_underscores(market: str) -> str:
    return market.replace('_', ' ')


def remove_the_word_total(market: str) -> str:
    # get rid of any unnecessary words like 'Total' (Total Bases -> Bases)
    return market.replace('Total ', '')


def map_fantasy_points_market(market: str, league: str) -> str:
    # checks for existence of league and then checks for any match of a 'Fantasy' market key word in the market name
    if league and any(word for word in FANTASY_KEY_WORDS if word in market):
        # get a more general market name for fantasy markets for better comparability
        return FANTASY_SCORE_MAP.get(league, market)

    # just return back the market name if not a fantasy market
    return market


def clean_boom_fantasy(market: str, period_classifier: Optional[str]) -> str:
    """Cleans market name specifically for the way Boom Fantasy formats their market names"""

    # replace underscores with spaces (POINTS_REBOUNDS)
    cleaned_market = remove_underscores(market)
    # capitalize first letters of each word
    cleaned_market = capitalize_first_letters(cleaned_market)
    # replace 'And' with '+' for better looking format
    cleaned_market = cleaned_market.replace(' And ', ' + ')
    # re-format the market to include the period classifier at the beginning of it if exists (Points Rebounds -> 1Q Points Rebounds)
    cleaned_market = f'{clean_period_classifier(period_classifier)}{cleaned_market}'
    # return the cleaned market name
    return cleaned_market


def clean_dabble(market: str) -> str:
    """Cleans market name specifically for the way Dabble formats their market names"""

    # capitalize first letter of each word (Batter  bases -> Batter  Bases)
    cleaned_market = capitalize_first_letters(market)
    # clean up any double spaces (Batter  Bases -> Batter Bases
    cleaned_market = cleaned_market.replace('  ', ' ')
    # get rid of parentheses words
    cleaned_market = remove_parentheses_words(cleaned_market)
    # remove the word 'Total'
    cleaned_market = remove_the_word_total(cleaned_market)
    # return the cleaned market name
    return cleaned_market


def clean_draft_kings_pick_6(market: str, league: str) -> str:
    """Cleans market name specifically for the way Draft Kings Pick6 formats their market names"""

    # capitalize first letter of each word (Shots on Goal -> Shots On Goal)
    cleaned_market = capitalize_first_letters(market)
    # get rid of parentheses words
    cleaned_market = remove_parentheses_words(cleaned_market)
    # remove the word 'Total'
    cleaned_market = remove_the_word_total(cleaned_market)
    # get a more comparable fantasy points market name
    cleaned_market = map_fantasy_points_market(cleaned_market, league)
    # return the cleaned market name
    return cleaned_market


def clean_hot_streak(market: str, league: str) -> str:
    """Cleans market name specifically for the way Hot Streak formats their market names"""

    # replace underscores with spaces (fantasy_points -> fantasy points)
    cleaned_market = remove_underscores(market)
    # get a more comparable fantasy points market name
    cleaned_market = map_fantasy_points_market(cleaned_market, league)
    # map some poorly formatted markets (pa -> Points + Assists)
    cleaned_market = MARKET_MAP['hot_streak'].get(cleaned_market, market)
    # capitalize first letter of each word (points -> Points)
    cleaned_market = capitalize_first_letters(cleaned_market)
    # return the cleaned market name
    return cleaned_market


def clean_money_line(market: str, league: str) -> str:
    """Cleans market name specifically for the way MoneyLine formats their market names"""

    # get a more comparable fantasy points market name
    cleaned_market = map_fantasy_points_market(market, league)
    # return the cleaned market name
    return cleaned_market


def clean_odds_shopper(market: str, league: str) -> str:
    """Cleans market name specifically for the way OddsShopper formats their market names"""

    # different market names for the same market for NFL and NCAAF -- need to map
    cleaned_market = MARKET_MAP['odds_shopper'].get(market, market)
    # remove the word 'Total'
    cleaned_market = remove_the_word_total(cleaned_market)
    # get a more comparable fantasy points market name
    cleaned_market = map_fantasy_points_market(cleaned_market, league)
    # return the cleaned market name
    return cleaned_market


def clean_parlay_play(market: str, league: str) -> str:
    """Cleans market name specifically for the way ParlayPlay formats their market names"""

    # capitalize first letter of each word (Shots on Goal -> Shots On Goal)
    cleaned_market = capitalize_first_letters(market)
    # get a more comparable fantasy points market name
    cleaned_market = map_fantasy_points_market(cleaned_market, league)
    # return the cleaned market name
    return cleaned_market


# a map for all specialized cleaner functions for each bookmaker
BOOKMAKER_CLEANER_MAP = {
    'boom_fantasy': clean_boom_fantasy,
    'dabble': clean_dabble,
    'draft_kings_pick_6': clean_draft_kings_pick_6,
    'hot_streak': clean_hot_streak,
    'money_line': clean_money_line,
    'odds_shopper': clean_odds_shopper,
    'parlay_play': clean_parlay_play,
}


def clean_market(market: str, bookmaker: str, **kwargs) -> str:
    # # get the specialized cleaner
    # if special_cleaner := BOOKMAKER_CLEANER_MAP.get(bookmaker):
    #     # return the cleaned market
    #     return special_cleaner(market, **kwargs)

    # no specialized cleaner? Just return the original market name
    return market



    # market = f'{period_classifier} {market}'
    # # checks for existence of league and then checks for any match of a 'Fantasy' market key word in the market name
    # if league and any(word for word in FANTASY_KEY_WORDS if word in market):
    #     # get a more general market name for fantasy markets for better comparability
    #     market = FANTASY_SCORE_MAP.get(league, market)
    #
    # # Initialize some values to reduce redundancy
    # n, is_uppercase = len(market), market.isupper()
    # # BoomFantasy annoyingly uses underscore as spaces
    # cleaned_market = market.replace('_', ' ')
    # # 'Total' is unnecessary to include in a market name -- located here so the next line will strip any extra space
    # cleaned_market = re.sub('total', '', cleaned_market, flags=re.IGNORECASE)
    # cleaned_market = re.sub('player', '', cleaned_market, flags=re.IGNORECASE)
    # cleaned_market = re.sub(' and ', ' + ', cleaned_market, flags=re.IGNORECASE)
    # # hotstreak does this
    # cleaned_market = re.sub(' plus ', ' + ', cleaned_market, flags=re.IGNORECASE)
    # # For example 'RBI' is fine to be uppercase but BATTER WALKS and BASES isn't
    # if ((' ' in cleaned_market) or n >= 4) and (cleaned_market.islower() or is_uppercase):
    #     # Payday capitalizes every letter in the market name
    #     cleaned_market = cleaned_market.title()
    #
    # # remove any words in parentheses
    # if cleaned_market[-1] == ')':
    #     # include all but up to the space and open parenthesis
    #     cleaned_market = cleaned_market[:cleaned_market.index(' (')]
    #
    # if n < 4 and not is_uppercase:
    #     cleaned_market = cleaned_market.upper()
    #
    # # Use regex to find '+' that doesn't already have spaces around it
    # cleaned_market = re.sub(r'(?<!\s)\+(?!\s)', ' + ', cleaned_market)
    # # Use regex to add a space before uppercase letters that are immediately after lowercase letters
    # cleaned_market = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned_market)
    #
    # return MARKET_MAP.get(cleaned_market.strip(), cleaned_market.strip())