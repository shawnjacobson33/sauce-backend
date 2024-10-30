import re
from distutils.command.clean import clean
from typing import Optional


MARKET_MAP = {
    'Hitter Fantasy Score': 'Baseball Fantasy Points'
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
    cleaned_market = market.replace('_', ' ')
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


def clean_market(market: str, bookmaker: str, **kwargs) -> str:
    # a map for all specialized cleaner functions for each bookmaker
    bookmaker_cleaner_map = {
        'boom_fantasy': clean_boom_fantasy,
        'dabble': clean_dabble,
        'draft_kings_pick_6': clean_draft_kings_pick_6,
    }

    # get the specialized cleaner
    if special_cleaner := bookmaker_cleaner_map.get(bookmaker):
        # return the cleaned market
        return special_cleaner(market, **kwargs)

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