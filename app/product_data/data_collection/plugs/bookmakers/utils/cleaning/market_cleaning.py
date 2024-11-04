from typing import Optional


MARKET_MAP = {
    'Basketball': {
      # Points
        'POINTS': 'Points',  # BoomFantasy
      # 1Q Points
        '1Q POINTS': '1Q Points',  # BoomFantasy
      # Rebounds
        'REBOUNDS': 'Rebounds',  # BoomFantasy
      # 1Q Rebounds
        '1Q REBOUNDS': '1Q Rebounds',  # BoomFantasy
      # Assists
        'ASSISTS': 'Assists',  # BoomFantasy
      # 1Q Assists
        '1Q ASSISTS': '1Q Assists',  # BoomFantasy
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
        'FG Made': 'Field Goals Made',  # Dabble
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
          'Kicker Points': 'Kicking Points',
          'Total Kicking Points': 'Kicking Points',  # OddsShopper
        # Extra Points Made
          'Extra Point Made': 'Extra Points Made',  # Dabble
          'Total Extra Points Made': 'Extra Points Made',  # OddsShopper
          'XP Made': 'Extra Points Made',  # OwnersBox
        # Field Goals Made
          'Field Goal Made': 'Field Goals Made',  # Dabble
          'Total Field Goals Made': 'Field Goals Made',  # OddsShopper
          'FG Made': 'Field Goals Made',  # OwnersBox
        # First Touchdown Scorer
          'First Touchdown Scorer': 'First Touchdown Scorer',  # OddsShopper
        # Last Touchdown Scorer
          'Last Touchdown Scorer': 'Last Touchdown Scorer',  # OddsShopper
    },
    'Ice Hockey': {
        # Points
        'Points': 'Points',  # BetOnline, Dabble
        'POINTS': 'Points',  # BoomFantasy
        # Shots On Goal
        'Shots On Goal': 'Shots On Goal',
        'SHOTS_ON_GOAL': 'Shots On Goal',  # BoomFantasy
        # Blocked Shots
        'Blocked Shots': 'Blocked Shots',
        # Goalie Saves
        'Goalie Saves': 'Goalie Saves',
        # Goals Against
        'Goals Against': 'Goals Against',
        # Assists
        'Assists': 'Assists',
    }
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


def clean_market(market: str, sport: str, period_classifier: str = None) -> Optional[str]:
    # if the bookmaker does (1Q, 2Q) betting lines
    if period_classifier:
        # re-format the market to include the period classifier at the beginning of it if exists (Points Rebounds -> 1Q Points Rebounds)
        market = f'{clean_period_classifier(period_classifier)}{market}'

    # get the markets associated with the mapped sport name
    if partitioned_market_map := MARKET_MAP.get(sport):
        # map the market name to a standardized version in the MARKET_MAP, if it doesn't exist
        if mapped_market := partitioned_market_map.get(market):
            # return the standardized market name
            return mapped_market

    return market
