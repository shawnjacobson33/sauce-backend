from pipelines.utils.storing import Storing
from pipelines.utils.cleaning import Cleaning

# STANDARDIZATION MAPS
LEAGUE_NAME_STRD_MAP = {
    'NBA': 'NBA',
    'NCAAM': 'NCAAM',
    'NCAAF': 'NCAAF',
    'NCAAFB': 'NCAAF',
    'NFL': 'NFL',
    'MLB': 'MLB',
    'NHL': 'NHL',
}
MARKET_NAME_STRD_MAP = {
    'Gamelines': {
        # Moneyline
          'Moneyline': 'Moneyline',
          '1st Quarter Moneyline': '1st Quarter Moneyline',
          '2nd Quarter Moneyline': '2nd Quarter Moneyline',
          '1st Half Moneyline': '1st Half Moneyline',
        # Spread
          'Spread': 'Spread',
          'Point Spread': 'Point Spread',
          '1st Quarter Point Spread': '1st Quarter Point Spread',
          '2nd Quarter Point Spread': '2nd Quarter Point Spread',
          '1st Half Point Spread': '1st Half Point Spread',
        # Total
          'Total': 'Total',
          '1st Quarter Total': '1st Quarter Total',
          '2nd Quarter Total': '2nd Quarter Total',
          '1st Half Total': '1st Half Total',
    },
    'PlayerProps': {
        'Basketball': {
          # Points
            'Points': 'Points',
            'POINTS': 'Points',  # BoomFantasy
            'points': 'Points',  # HotStreak
            'PTS': 'Points',  # Payday
            'Total Points': 'Points',  # Rebet
            'Player points (incl. overtime)': 'Points',  # Rebet
            'Player Points': 'Points',  # PropProfessor
          # 1Q Points
            '1Q POINTS': '1Q Points',  # BoomFantasy
            'first_qtr_points': '1Q Points',  # Sleeper
          # 1H Points
            '1H POINTS': '1H Points',  # BoomFantasy
            '2H POINTS': '2H Points',  # BoomFantasy
          # Rebounds
            'Rebounds': 'Rebounds',
            'REBOUNDS': 'Rebounds',  # BoomFantasy
            'rebounds': 'Rebounds',  # HotStreak
            'REB': 'Rebounds',  # Payday
            'Total Rebounds': 'Rebounds',  # Rebet
            'Player rebounds (incl. overtime)': 'Rebounds',  # Rebet
            'Player Rebounds': 'Rebounds',  # PropProfessor
          # 1Q Rebounds
            '1Q REBOUNDS': '1Q Rebounds',  # BoomFantasy
            'first_qtr_rebounds': '1Q Rebounds',  # Sleeper
            '2H REBOUNDS': '2H Rebounds',  # BoomFantasy
          # Defensive Rebounds
            'Defensive Rebounds': 'Defensive Rebounds',  # PrizePicks
          # Offensive Rebounds
            'Offensive Rebounds': 'Offensive Rebounds',  # PrizePicks
          # Assists
            'Assists': 'Assists',
            'ASSISTS': 'Assists',  # BoomFantasy
            'assists': 'Assists',  # HotStreak
            'AST': 'Assists',  # Payday
            'Player assists (incl. overtime)': 'Assists',  # Rebet
            'Total Assists': 'Assists',  # Rebet
            'Player Assists': 'Assists',  # PropProfessor
          # 1Q Assists
            '1Q ASSISTS': '1Q Assists',  # BoomFantasy
            'first_qtr_assists': '1Q Assists',  # Sleeper
            '2H ASSISTS': '2H Assists',  # BoomFantasy
          # Points + Rebounds
            'Points + Rebounds': 'Points + Rebounds',
            'Pts + Reb': 'Points + Rebounds',  # ParlayPlay
            'Pts + Rebs': 'Points + Rebounds',  # OwnersBox
            'pr': 'Points + Rebounds',  # HotStreak
            'POINTS_AND_REBOUNDS': 'Points + Rebounds',  # BoomFantasy
            'P+R': 'Points + Rebounds',  # Payday
            'Pts+Rebs': 'Points + Rebounds',  # PrizePicks
            'Points+Rebounds': 'Points + Rebounds',  # Drafters
            'points_and_rebounds': 'Points + Rebounds',  # Sleeper
            'Pts + Rebounds': 'Points + Rebounds',  # SuperDraft
            'Player Points + Rebounds': 'Points + Rebounds',  # PropProfessor
          # Points + Assists
            'Points + Assists': 'Points + Assists',
            'Pts + Ast': 'Points + Assists',  # ParlayPlay
            'Pts + Asts': 'Points + Assists',  # OwnersBox
            'pa': 'Points + Assists',  # HotStreak
            'POINTS_AND_ASSISTS': 'Points + Assists',  # BoomFantasy
            'P+A': 'Points + Assists',  # Payday
            'Pts+Asts': 'Points + Assists',  # PrizePicks
            'Points+Assists': 'Points + Assists',  # Drafters
            'points_and_assists': 'Points + Assists',  # Sleeper
            'Pts + Assists': 'Points + Assists',  # SuperDraft
            'Player Points + Assists': 'Points + Assists',  # PropProfessor
          # Rebounds + Assists
            'Rebounds + Assists': 'Rebounds + Assists',
            'Reb + Ast': 'Rebounds + Assists',  # ParlayPlay
            'Rebs + Asts': 'Rebounds + Assists',  # OwnersBox
            'ra': 'Rebounds + Assists',  # HotStreak
            'REBOUNDS_AND_ASSISTS': 'Rebounds + Assists',  # BoomFantasy
            'R+A': 'Rebounds + Assists',  # Payday
            'Assists + Rebounds': 'Rebounds + Assists',  # DraftKings Pick6
            'Rebs+Asts': 'Rebounds + Assists',  # PrizePicks
            'Rebounds+Assists': 'Rebounds + Assists',  # Drafters
            'rebounds_and_assists': 'Rebounds + Assists',  # Sleeper
            'Rbs + Assists': 'Rebounds + Assists',  # SuperDraft
            'Player Rebounds + Assists': 'Rebounds + Assists',  # PropProfessor
          # Points + Rebounds + Assists
            'Points + Rebounds + Assists': 'Points + Rebounds + Assists',
            'Pts + Reb + Ast': 'Points + Rebounds + Assists',  # ParlayPlay, BetOnline
            'Pts + Rebs + Asts': 'Points + Rebounds + Assists',  # OwnersBox
            'pra': 'Points + Rebounds + Assists',  # HotStreak
            'POINTS_REBOUNDS_ASSISTS': 'Points + Rebounds + Assists',  # BoomFantasy
            'P+R+A': 'Points + Rebounds + Assists',  # Payday
            'Points + Assists + Rebounds': 'Points + Rebounds + Assists',  # DraftKings Pick6
            'Pts+Rebs+Asts': 'Points + Rebounds + Assists',  # PrizePicks
            'pts_reb_ast': 'Points + Rebounds + Assists',  # Sleeper
            'Pts + Rbs + Asts': 'Points + Rebounds + Assists',  # SuperDraft
            'Player Points + Assists + Rebounds': 'Points + Rebounds + Assists',  # PropProfessor
          # 1Q Points + Rebounds + Assists
            '1Q Pts + Rebs + Asts': '1Q Points + Rebounds + Assists',  # Underdog Fantasy
          # 1H Points + Rebounds + Assists
            '1H Pts+Rebs+Asts': '1H Points + Rebounds + Assists',  # PrizePicks
            '1H POINTS_REBOUNDS_ASSISTS': '1H Points + Rebounds + Assists',  # BoomFantasy
            '1H Pts + Rebs + Asts': '1H Points + Rebounds + Assists',  # Underdog Fantasy
          # 2H Points + Rebounds + Assists
            '2H Pts+Rebs+Asts': '2H Points + Rebounds + Assists',  # PrizePicks
            '2H POINTS_REBOUNDS_ASSISTS': '2H Points + Rebounds + Assists',  # BoomFantasy
          # 3-Pointers Made
            'Three Point Field Goals Made': '3-Pointers Made',  # BetOnline
            'MADE_THREE_POINTERS': '3-Pointers Made',  # BoomFantasy
            '3PT Made': '3-Pointers Made',  # Dabble, ParlayPlay
            '3-Pointers Made': '3-Pointers Made',  # DraftKings Pick6, OwnersBox
            '3-Pointers': '3-Pointers Made',  # OddsShopper
            '3PM': '3-Pointers Made',  # Payday
            'three_points_made': '3-Pointers Made',  # HotStreak
            '3-PT Made': '3-Pointers Made',  # PrizePicks
            '3 Pointers Made': '3-Pointers Made',  # Drafters
            'threes_made': '3-Pointers Made',  # Sleeper
            'Total 3-Point Field Goals': '3-Pointers Made',  # Rebet
            'Player 3-point field goals (incl. overtime)': '3-Pointers Made',  # Rebet
            'Player Threes Made': '3-Pointers Made',  # PropProfessor
          # 1Q 3-Pointers Made
            '1Q 3-Pointers Made': '1Q 3-Pointers Made',  # Underdog Fantasy
          # 1H 3-Pointers Made
            '1H 3-Pointers Made': '1H 3-Pointers Made',  # Underdog Fantasy
            '2H MADE_THREE_POINTERS': '2H 3-Pointers Made',  # BoomFantasy
          # 3-Pointers Attempted
            '3PT Attempted': '3-Pointers Attempted',  # Dabble
            '3-PT Attempted': '3-Pointers Attempted',  # PrizePicks
          # 2-Pointers Made
            '2PT Made': '2-Pointers Made',  # Dabble
          # 2-Pointers Attempted
            '2PT Attempted': '2-Pointers Attempted',  # Dabble
          # Field Goals Made
            'FG Made': 'Field Goals Made',  # Dabble
          # Field Goals Attempted
            'FG Attempted': 'Field Goals Attempted',  # Dabble
          # Free Throws Made
            'Free Throws Made': 'Free Throws Made',
            'FT Made': 'Free Throws Made',  # Dabble
          # Free Throws Attempted
            'FT Attempted': 'Free Throws Attempted',  # Dabble
          # Double Doubles
            'Double Double': 'Double Doubles',  # ParlayPlay
            'Total Double Doubles': 'Double Doubles',  # OddsShopper
            'double_double': 'Double Doubles',  # HotStreak
            'Double Double (incl. Overtime)': 'Double Doubles',  # Rebet
            'Player Double Double': 'Double Doubles',  # PropProfessor
          # Triple Doubles
            'Triple Double': 'Triple Doubles',  # ParlayPlay
            'Total Triple Doubles': 'Triple Doubles',  # OddsShopper
            'triple_double': 'Triple Doubles',  # HotStreak
            'Triple Double (incl. Overtime)': 'Triple Doubles',  # Rebet
            'Player Triple Double': 'Triple Doubles',  # PropProfessor
          # Turnovers
            'Turnovers': 'Turnovers',
            'TURNOVERS': 'Turnovers',  # BoomFantasy
            'turnovers': 'Turnovers',  # HotStreak
            'Turnovers  ': 'Turnovers',  # Underdog Fantasy
            'Player Turnovers': 'Turnovers',  # PropProfessor
          # Steals
            'Steals': 'Steals',
            'STEALS': 'Steals',  # BoomFantasy
            'steals': 'Steals',  # HotStreak
            'Player steals (incl. overtime)': 'Steals',  # Rebet
            'Player Steals': 'Steals',  # PropProfessor
          # Blocks
            'Blocks': 'Blocks',
            'BLOCKS': 'Blocks',  # BoomFantasy
            'Blocked Shots': 'Blocks',  # OwnersBox
            'blocks': 'Blocks',
            'Player blocks (incl. overtime)': 'Blocks',  # Rebet
            'Player Blocks': 'Blocks',  # PropProfessor
          # Blocks + Steals
            'BLOCKS_AND_STEALS': 'Blocks + Steals',  # BoomFantasy
            'stocks': 'Blocks + Steals',  # HotStreak
            'Steals + Blocks': 'Blocks + Steals',  # DraftKings Pick6, OddsShopper, OwnersBox
            'Stl + Blk': 'Blocks + Steals',  # ParlayPlay
            'Blks+Stls': 'Blocks + Steals',  # PrizePicks
            'Blocks+Steals': 'Blocks + Steals',  # Drafters
            'blocks_and_steals': 'Blocks + Steals',  # Sleeper
            'Player Blocks + Steals': 'Blocks + Steals',  # PropProfessor
          # Fantasy Points
            'fantasy_points': 'Fantasy Points',  # HotStreak
            'Fantasy Score': 'Fantasy Points',  # PrizePicks
          # 1H Fantasy Points
            '1H Fantasy Score': '1H Fantasy Points',  # PrizePicks
          # 2H Fantasy Points
            '2H Fantasy Score': '2H Fantasy Points',  # PrizePicks
          # 4Q Fantasy Points
            '4Q Fantasy Score': '4Q Fantasy Points',  # PrizePicks
          # Top Point Scorer
            'Top Point Scorer': 'Top Point Scorer',  # OddsShopper
          # First Basket
            'First Basket': 'First Basket',  # OddsShopper
            'First Point Scorer (incl. Overtime)': 'First Basket',  # Rebet

        },
        'Football': {
            # Passing Yards
              'PASSING_YARDS': 'Passing Yards',  # BoomFantasy
              'Total Passing Yards': 'Passing Yards',  # OddsShopper
              'Pass Yds': 'Passing Yards',  # ParlayPlay
              'PASSYDS': 'Passing Yards',  # Payday
              'Pass Yards': 'Passing Yards',  # PrizePicks
              'passing_yards': 'Passing Yards',  # Sleeper
              'PassingYards': 'Passing Yards',  # Vivid Picks
              'Player Passing Yards': 'Passing Yards',  # PropProfessor
            # 1Q Passing Yards
              '1Q Pass Yards': '1Q Passing Yards',  # PrizePicks
              '1Q PASSING_YARDS': '1Q Passing Yards',  # BoomFantasy
            # 1H Passing Yards
              '1H Pass Yards': '1H Passing Yards',  # PrizePicks
              '1H PASSING_YARDS': '1H Passing Yards',  # BoomFantasy
            # 2H Passing Yards
              '2H Pass Yards': '2H Passing Yards',  # PrizePicks
            # Passing Attempts
              'Pass Attempts': 'Passing Attempts',  # BetOnline, ParlayPlay
              'PASSING_ATTEMPTS': 'Passing Attempts',  # BoomFantasy
              'Total Passing Attempts': 'Passing Attempts',  # OddsShopper
              'passing_attempts': 'Passing Attempts',  # Sleeper
              'Player Passing Attempts': 'Passing Attempts',  # PropProfessor
            # Completions
              'Completions': 'Completions',  # DraftKingsPick6, OwnersBox
              'Passing Completions': 'Completions',  # Dabble
              'Pass Completions': 'Completions',  # BetOnline, ParlayPlay
              'PASSING_COMPLETIONS': 'Completions',  # BoomFantasy
              'Total Pass Completions': 'Completions',   # OddsShopper
              'CMP': 'Completions',  # Payday
              'pass_completions': 'Completions',  # Sleeper
              'Player Passing Completions': 'Completions',  # PropProfessor
            # Passing Touchdowns
              'Passing Touchdowns': 'Passing Touchdowns',  # MoneyLine, OwnersBox
              'Passing TDs': 'Passing Touchdowns',  # BetOnline, ParlayPlay
              'PASSING_TOUCHDOWNS': 'Passing Touchdowns',  # BoomFantasy
              'Total Passing Touchdowns': 'Passing Touchdowns',  # OddsShopper
              'Pass TDs': 'Passing Touchdowns',  # ParlayPlay
              'passing_touchdowns': 'Passing Touchdowns',  # Sleeper
              'PassingTouchdowns': 'Passing Touchdowns',  # Vivid Picks
              'PASSTD': 'Passing Touchdowns',  # Payday
              'Player Passing Touchdowns': 'Passing Touchdowns',  # PropProfessor
            # Interceptions Thrown -- IMPORTANT FOR SOME BOOKMAKERS THEY FORMAT Interceptions as Passing Interceptions, AND SOME DO Interceptions as actual Interceptions caught.
              'Pass Interceptions': 'Interceptions Thrown',  # BetOnline
              'INTERCEPTIONS_THROWN': 'Interceptions Thrown',  # BoomFantasy
              'Interceptions Thrown': 'Interceptions Thrown',  # DraftKingsPick6
              'Total Interceptions Thrown': 'Interceptions Thrown',  # OddsShopper
              'INT': 'Interceptions Thrown',  # PrizePicks
              'interceptions': 'Interceptions Thrown',  # Sleeper
              'passing_interceptions': 'Interceptions Thrown',  # HotStreak
              'Interception': 'Interceptions Thrown',  # ParlayPlay
              'Player Interceptions': 'Interceptions Thrown',  # PropProfessor
            # Longest Passing Completion
              'Longest Passing Completion': 'Longest Passing Completion',  # OddsShopper
              'Longest Passing Completion (Yards)': 'Longest Passing Completion',  # Dabble
              'Longest Pass': 'Longest Passing Completion',  # ParlayPlay
              'longest_pass': 'Longest Passing Completion',  # HotStreak
              'Longest Completion': 'Longest Passing Completion',  # Drafters
              'Player Longest Completion': 'Longest Passing Completion',  # PropProfessor
            # Rushing Yards
              'RUSHING_YARDS': 'Rushing Yards',  # BoomFantasy
              'Total Rushing Yards': 'Rushing Yards',  # OddsShopper
              'Rush Yds': 'Rushing Yards',  # ParlayPlay
              'RUSHYDS': 'Rushing Yards',  # Payday
              'Rush Yards': 'Rushing Yards',  # PrizePicks
              'rushing_yards': 'Rushing Yards',  # Sleeper
              'RushingYards': 'Rushing Yards',  # Vivid Picks
              'Player Rushing Yards': 'Rushing Yards',  # PropProfessor
            # 1Q Rushing Yards
              '1Q Rush Yards': '1Q Rushing Yards',  # PrizePicks
              '1Q RUSHING_YARDS': '1Q Rushing Yards',  # BoomFantasy
            # 1H Rushing Yards
              '1H Rush Yards': '1H Rushing Yards',  # PrizePicks
              '1H RUSHING_YARDS': '1H Rushing Yards',  # BoomFantasy
            # 2H Rushing Yards
              '2H Rush Yards': '2H Rushing Yards',  # PrizePicks
            # Carries
              'Carries': 'Carries',  # BetOnline
              'RUSHING_ATTEMPTS': 'Carries',  # BoomFantasy
              'Rushing Attempts': 'Carries',  # Dabble, OwnersBox
              'Total Rushing Attempts': 'Carries',  # OddsShopper
              'Rush Attempts': 'Carries',  # ParlayPlay
              'rushing_attempts': 'Carries',  # Sleeper
              'carries': 'Carries',  # HotStreak
              'Player Rushing Attempts': 'Carries',  # PropProfessor
            # Rushing Touchdowns
              'RUSHING_TOUCHDOWNS': 'Rushing Touchdowns',  # BoomFantasy
              'Total Rushing Touchdowns': 'Rushing Touchdowns',  # OddsShopper
              'rushing_touchdowns': 'Rushing Touchdowns',  # Sleeper
              'Rushing TDs': 'Rushing Touchdowns',  # Underdog Fantasy
              'RushingTouchdowns': 'Rushing Touchdowns',  # VividPicks
            # Longest Rush
              'Longest Rush': 'Longest Rush',  # OddsShopper, OwnersBox, ParlayPlay
              'Longest Rush (Yards)': 'Longest Rush',  # Dabble
              'longest_rush': 'Longest Rush',  # HotStreak
              'Player Longest Rush': 'Longest Rush',  # PropProfessor
            # Receiving Yards
              'RECEIVING_YARDS': 'Receiving Yards',  # BoomFantasy
              'Rec Yds': 'Receiving Yards',  # ParlayPlay
              'Total Receiving Yards': 'Receiving Yards',  # OddsShopper
              'RECYDS': 'Receiving Yards',  # Payday
              'receiving_yards': 'Receiving Yards',  # Sleeper
              'ReceivingYards': 'Receiving Yards',  # Vivid Picks
              'Player Receiving Yards': 'Receiving Yards',  # PropProfessor
            # 1Q Receiving Yards
              '1Q Receiving Yards': '1Q Receiving Yards',  # PrizePicks
              '1Q RECEIVING_YARDS': '1Q Receiving Yards',  # BoomFantasy
            # 1H Receiving Yards
              '1H Receiving Yards': '1H Receiving Yards',  # PrizePicks
              '1H RECEIVING_YARDS': '1H Receiving Yards',  # BoomFantasy
            # 2H Receiving Yards
              '2H Receiving Yards': '2H Receiving Yards',  # PrizePicks
            # Targets
              'Receiving Targets': 'Targets',  # ParlayPlay
              'Rec Targets': 'Targets',  # PrizePicks
              'Player Receiving Targets': 'Targets',  # PropProfessor
            # Receptions
              'RECEPTIONS': 'Receptions',  # BoomFantasy
              'Total Receptions': 'Receptions',  # OddsShopper
              'REC': 'Receptions',  # Payday
              'receptions': 'Receptions',  # Sleeper
              'Player Receptions': 'Receptions',  # PropProfessor
            # 2H Receptions
              '2H Receptions': '2H Receptions',  # PrizePicks
            # Receiving Touchdowns
              'RECEIVING_TOUCHDOWNS': 'Receiving Touchdowns',  # BoomFantasy
              'Total Receiving Touchdowns': 'Receiving Touchdowns',  # OddsShopper
              'receiving_touchdowns': 'Receiving Touchdowns',  # Sleeper
              'Receiving TDs': 'Receiving Touchdowns',  # UnderdogFantasy
            # Longest Reception
              'Longest Reception': 'Longest Reception',  # OddsShopper, OwnersBox, ParlayPlay
              'Longest Reception (Yards)': 'Longest Reception',  # Dabble
              'longest_reception': 'Longest Reception',  # HotStreak
              'Player Longest Reception': 'Longest Reception',  # PropProfessor
            # Passing + Rushing Touchdowns
            # Passing + Rushing Yards
              'Passing Yards + Rushing Yards': 'Passing + Rushing Yards',  # Dabble
              'Total Pass + Rush Yards': 'Passing + Rushing Yards',  # OddsShopper
              'Total Passing + Rushing Yards': 'Passing + Rushing Yards',  # OddsShopper
              'Pass + Rush Yards': 'Passing + Rushing Yards',  # OwnersBox, ParlayPlay
              'Pass+Rush Yds': 'Passing + Rushing Yards',  # PrizePicks
              'passing_and_rushing_yards': 'Passing + Rushing Yards',  # Sleeper
              'passing_plus_rushing_yards': 'Passing + Rushing Yards',  # HotStreak
              'Player Passing + Rushing Yards': 'Passing + Rushing Yards',  # PropProfessor
            # Rushing + Receiving Yards
              'Rushing + Receiving Yards': 'Rushing + Receiving Yards',  # MoneyLine
              'Receiving Yards + Rushing Yards': 'Rushing + Receiving Yards',  # Dabble
              'Rush + Rec Yards': 'Rushing + Receiving Yards',  # DraftKingsPick6, OwnersBox, ParlayPlay
              'Total Rush + Rec Yards': 'Rushing + Receiving Yards',  # OddsShopper
              'Total Rushing + Receiving Yards': 'Rushing + Receiving Yards',  # OddsShopper
              'Rush+Rec Yds': 'Rushing + Receiving Yards',  # PrizePicks
              'rushing_and_receiving_yards': 'Rushing + Receiving Yards',  # Sleeper
              'receiving_plus_rushing_yards': 'Rushing + Receiving Yards',  # HotStreak
              'Rush+Receiving Yds': 'Rushing + Receiving Yards',  # Drafters
              'Player Rushing + Receiving Yards': 'Rushing + Receiving Yards',  # PropProfessor
            # Rushing + Receiving Touchdowns
              'Rushing + Receiving Touchdowns': 'Rushing + Receiving Touchdowns',  # MoneyLine
              'Receiving + Rushing Touchdowns': 'Rushing + Receiving Touchdowns',  # Dabble
              'Rush + Rec TDs': 'Rushing + Receiving Touchdowns',  # DraftKingsPick6, OwnersBox
              'Total Rushing + Receiving TDs': 'Rushing + Receiving Touchdowns',  # OddsShopper
              'Rush + Rec Td': 'Rushing + Receiving Touchdowns',  # ParlayPlay
              'Rush+Rec TDs': 'Rushing + Receiving Touchdowns',  # PrizePicks
            # 1Q Rushing + Receiving Touchdowns
              '1Q Rush + Rec TDs': '1Q Rushing + Receiving Touchdowns',  # Underdog Fantasy
            # 1H Rushing + Receiving Touchdowns
              '1H Rush + Rec TD': '1H Rushing + Receiving Touchdowns',  # Underdog Fantasy
            # Total Touchdowns -- Includes Defensive Players
              'Total Touchdowns': 'Total Touchdowns',  # OddsShopper
              'TOTAL_TOUCHDOWNS': 'Total Touchdowns',  # BoomFantasy
              'Total Passing + Rushing + Receiving TDs': 'Total Touchdowns',  # OddsShopper
              'Pass+Rush+Rec TDs': 'Total Touchdowns',  # PrizePicks
              'anytime_touchdowns': 'Total Touchdowns',  # Sleeper
              'Anytime TD Scored': 'Total Touchdowns',  # Vivid Picks
              'Player Touchdowns': 'Total Touchdowns',  # PropProfessor
            # Fantasy Points
              'Fantasy Points': 'Fantasy Points',  # ParlayPlay
              'Fantasy Score': 'Fantasy Points',  # PrizePicks
              'fantasy_points': 'Fantasy Points',  # Sleeper
            # 1H Fantasy Points
              '1H Fantasy Score': '1H Fantasy Points',  # PrizePicks
            # 2H Fantasy Points
              '2H Fantasy Score': '2H Fantasy Points',  # PrizePicks
            # Sacks
              'Total Sacks': 'Sacks',  # OddsShopper
              'sacks': 'Sacks',  # Sleeper
              'Player Sacks': 'Sacks',  # PropProfessor
            # Total Tackles
              'Total Tackles': 'Total Tackles',
              'Tackles': 'Total Tackles',  # BetOnline
              'Tackles + Assists': 'Total Tackles',  # DraftKingsPick6, OwnersBox
              'Total Tackles + Assists': 'Total Tackles',  # OddsShopper
              'Tackles S+A': 'Total Tackles',  # ParlayPlay
              'Tackles Solo+Assists': 'Total Tackles',  # Payday
              'tackles_and_assists': 'Total Tackles',  # Sleeper
              'Player Tackles + Assists': 'Total Tackles',  # PropProfessor
            # Tackle Assists
              'Tackle Assists': 'Tackle Assists',  # Dabble
              'Total Assists': 'Tackle Assists',   # OddsShopper
              'assists': 'Tackle Assists',  # Sleeper
              'Assists': 'Tackle Assists',  # Underdog Fantasy
              'Player Tackles Assisted': 'Tackle Assists',  # PropProfessor
            # Solo Tackles
              'Solo Tackles': 'Solo Tackles',  # OwnersBox, ParlayPlay
              'Tackle Tackles': 'Solo Tackles',  # OddsShopper
              'tackles': 'Solo Tackles',  # Sleeper
              'Player Tackles': 'Solo Tackles',  # PropProfessor
            # Interceptions -- IMPORTANT FOR SOME BOOKMAKERS THEY FORMAT Interceptions as Passing Interceptions, AND SOME DO Interceptions as actual Interceptions caught.
               # Dabble, OwnersBox -- "Interceptions" TODO: IMPLEMENT LOGIC
               # ParlayPlay -- "Interception"
               # PrizePicks -- 'INT'
              'defensive_interceptions': 'Interceptions',
              'Defensive Interceptions': 'Interceptions',  # Underdog Fantasy
            # Kicking Points
              'Kicking Points': 'Kicking Points',  # DraftKingsPick6, OwnersBox
              'Kicker Points': 'Kicking Points',
              'Total Points': 'Kicking Points',  # ParlayPlay
              'Total Kicking Points': 'Kicking Points',  # OddsShopper
              'kicking_points': 'Kicking Points',  # Sleeper
              'Player Kicking Points': 'Kicking Points',  # PropProfessor
            # 2H Kicking Points
              '2H Kicking Points': '2H Kicking Points',  # PrizePicks
            # Extra Points Made
              'Extra Point Made': 'Extra Points Made',  # Dabble
              'Total Extra Points Made': 'Extra Points Made',  # OddsShopper
              'XP Made': 'Extra Points Made',  # OwnersBox
              'PAT': 'Extra Points Made',  # Payday
              'extra_points': 'Extra Points Made',  # HotStreak
              'Player Extra Points': 'Extra Points Made',  # PropProfessor
            # Field Goals Made
              'Field Goal Made': 'Field Goals Made',  # Dabble
              'Total Field Goals Made': 'Field Goals Made',  # OddsShopper
              'FG Made': 'Field Goals Made',  # OwnersBox
              'field_goal_made': 'Field Goals Made',  # Sleeper
              'field_goals': 'Field Goals Made',  # HotStreak
              'FGM': 'Field Goals Made',  # Payday
              'Player Field Goals Made': 'Field Goals Made',  # PropProfessor
            # First Touchdown Scorer
              'First Touchdown Scorer': 'First Touchdown Scorer',  # OddsShopper
              'First TD Scorer': 'First Touchdown Scorer',  # Underdog Fantasy
            # Last Touchdown Scorer
              'Last Touchdown Scorer': 'Last Touchdown Scorer',  # OddsShopper
            # Fumbles Lost
              'Fumbles Lost': 'Fumbles Lost',  # Underdog Fantasy
            # Rushing Yards in First 5 Attempts
              'Rush Yards in First 5 Attempts': 'Rushing Yards in First 5 Attempts',  # PrizePicks
            # Rushing + Receiving First Downs
              'Rush + Rec First Downs': 'Rushing + Receiving First Downs',  # Underdog Fantasy
            # Sacks Taken
              'Sacks Taken': 'Sacks Taken',  # Underdog Fantasy
            # Completion Percentage
              'Completion Percentage': 'Completion Percentage',  # Underdog Fantasy
            # Average Yards Per Punt
              'Avg Yards Per Punt': 'Average Yards Per Punt',  # Underdog Fantasy
            # Punts
              'Total Punts': 'Punts',  # Underdog Fantasy
              'Player Punts': 'Punts',  # PropProfessor
            # Punts Inside 20
              'Punts Inside 20': 'Punts Inside 20',  # PrizePicks
            # Tackles for Loss
              'Player Tackles for Loss': 'Tackles for Loss',  # PropProfessor
        },
        'Ice Hockey': {
            # Points
              'Points': 'Points',  # BetOnline, Dabble
              'POINTS': 'Points',  # BoomFantasy
              'Total Points': 'Points',  # OddsShopper
              'points': 'Points',  # Sleeper
              'Player points (incl. overtime)': 'Points',  # Rebet
              'Player Points': 'Points',  # PropProfessor
            # Shots On Goal
              'Shots On Goal': 'Shots On Goal',
              'SHOTS_ON_GOAL': 'Shots On Goal',  # BoomFantasy
              'Shots on Goal': 'Shots On Goal',  # DraftKingsPick6
              'Total Shots on Goal': 'Shots On Goal',  # OddsShopper
              'SOG': 'Shots On Goal',  # Payday
              'shots': 'Shots On Goal',  # Sleeper
              'Shots': 'Shots On Goal',  # SuperDraft
              'ShotsOnGoal': 'Shots On Goal',  # Vivid Picks
              'Player shots on goal (incl. overtime)': 'Shots On Goal',  # Rebet
              'Total Shots On Goal': 'Shots On Goal',  # Rebet
              'Player Shots on Goal': 'Shots On Goal',  # PropProfessor
            # 1st Period Shots On Goal
              '1st Period Shots': '1st Period Shots On Goal',  # UnderdogFantasy
            # Blocked Shots
              'Blocked Shots': 'Blocked Shots',
              'BLKS': 'Blocked Shots',  # Payday
              'Blocks': 'Blocked Shots',  # DraftKingsPick6
              'blocked_shots': 'Blocked Shots',  # Sleeper
              'Player Blocks': 'Blocked Shots',  # PropProfessor
            # Saves
              'Goalie Saves': 'Saves',
              'Saves': 'Saves',  # Drafters
              'Total Saves': 'Saves',  # OddsShopper
              'GoaltendingSaves': 'Saves',  # VividPicks
              'saves': 'Saves',  # Sleeper
              'SAV': 'Saves',  # Payday
              'SAVES': 'Saves',  # BoomFantasy
            # 1st Period Goalie Saves
              '1st Period Saves': '1st Period Saves',  # Underdog Fantasy
            # Goals Against
              'Goals Against': 'Goals Against',
              'Total Goals Against': 'Goals Against',  # OddsShopper
            # 1st Period Goals Against
              '1st Period Goals Against': '1st Period Goals Against',  # Underdog Fantasy
            # Assists
              'Assists': 'Assists',
              'Total Assists': 'Assists',  # OddsShopper
              'AST': 'Assists',  # Payday
              'assists': 'Assists',  # Sleeper
              'Player assists (incl. overtime)': 'Assists',  # Rebet
              'ASSISTS': 'Assists',  # BoomFantasy
              'Player Assists': 'Assists',  # PropProfessor
            # Goals
              'Total Player Goals': 'Goals',  # OddsShopper
              'Goals': 'Goals',  # OwnersBox
              'goals': 'Goals',  # Sleeper
              'GOL': 'Goals',  # Payday
              'Player goals (incl. overtime)': 'Goals',  # Rebet
              'GOALS': 'Goals',  # BoomFantasy
              'Player Goals': 'Goals',  # PropProfessor
            # Power Play Points
              'Power Play Points': 'Power Play Points',  # OddsShopper
              'powerplay_points': 'Power Play Points',  # Sleeper
              'Powerplay Points': 'Power Play Points',  # Dabble
              'power_play_points': 'Power Play Points',  # HotStreak
            # First Goal Scorer
              'First Goal Scorer': 'First Goal Scorer',  # OddsShopper
            # Last Goal Scorer
              'Last Goal Scorer': 'Last Goal Scorer',  # OddsShopper
            # Faceoffs Won
              'Faceoffs Won': 'Faceoffs Won',  # PrizePicks
              'Face Offs Won': 'Faceoffs Won',  # DraftKingsPick6
              'Player Faceoffs Won': 'Faceoffs Won',  # PropProfessor
            # Hits
              'Hits': 'Hits',  # PrizePicks
              'Player Hits': 'Hits',  # PropProfessor
            # Time On Ice
              'Time On Ice': 'Time On Ice',  # PrizePicks
              'Time on Ice': 'Time On Ice',  # Underdog Fantasy
              'Time On Ice (In Reg)': 'Time On Ice',  # Rebet
            # Fantasy Points
              'Fantasy Points': 'Fantasy Points',  # Underdog Fantasy
              'Fantasy Score': 'Fantasy Points',  # Ice Hockey
            # Shutouts
              'Total Shutouts': 'Shutouts',  # OddsShopper
            # Plus Minus
              'Plus Minus': 'Plus Minus',  # Underdog Fantasy
        }
    }
}
PERIOD_NAME_STRD_MAP = {
    'firstQuarter': '1Q',
    'secondQuarter': '2Q',
    'firstHalf': '1H',
    'thirdQuarter': '3Q',
    'secondHalf': '2H',
    'fourthQuarter': '4Q',
}
SUBJECT_NAME_STRD_MAP = {
    'NBA:alexandre sarr': 'Alex Sarr',
    'NBA:alexandre sarr:WAS': 'Alex Sarr',
    'NBA:alexandre sarr:C': 'Alex Sarr',
    'NBA:bub carrington': 'Carlton Carrington',
    'NBA:bub carrington:WAS': 'Carlton Carrington',
    'NBA:bub carrington:PG': 'Carlton Carrington',  # Todo: do you need to map
    'NBA:lu dort': 'Luguentz Dort',
    'NBA:lu dort:OKC': 'Luguentz Dort',
    'NBA:lu dort:SF': 'Luguentz Dort',
    'NBA:nic claxton': 'Nicolas Claxton',
    'NBA:nic claxton:BKN': 'Nicolas Claxton',
    'NBA:nic claxton:C': 'Nicolas Claxton',
    'NBA:cam johnson': 'Cameron Johnson',
    'NBA:cam johnson:BKN': 'Cameron Johnson',
    'NBA:cam johnson:SF': 'Cameron Johnson',
}


def load_in_subject_strd_identity_map(teams: list[dict], subjects: list[dict]):
    global SUBJECT_NAME_STRD_MAP
    subject_name_strd_identity_map = {}

    # STANDARDIZE TEAMS
    for team in teams:
        stored_subject_name = team['full_name']
        cleaned_subject_name = Cleaning.clean_subject_name(stored_subject_name)
        # One for just the league and subject name
        subject_key = Storing.get_subject_key(team['league'], cleaned_subject_name)
        if subject_key in subject_name_strd_identity_map:
            raise ValueError(f"Duplicate subject key found: '{subject_key}'")

        SUBJECT_NAME_STRD_MAP[subject_key] = stored_subject_name

    # STANDARDIZE INDIVIDUAL PLAYERS
    for subject in subjects:
        stored_subject_name = subject['name']
        cleaned_subject_name = Cleaning.clean_subject_name(stored_subject_name)
        # One for just the league and subject name
        subject_key = Storing.get_subject_key(subject['league'], cleaned_subject_name)
        if subject_key in subject_name_strd_identity_map:
            raise ValueError(f"Duplicate subject key found: '{subject_key}'")

        SUBJECT_NAME_STRD_MAP[subject_key] = stored_subject_name

        # Two for the league, subject name, and each subject attribute
        for subject_attribute_field in ['team', 'position']:
            subject_attribute_field = subject['team']['abbr_name'] if subject_attribute_field == 'team' else subject['position']
            subject_key = Storing.get_subject_key(subject['league'], cleaned_subject_name, subject_attribute_field)
            if subject_key in subject_name_strd_identity_map:
                raise ValueError(f"Duplicate subject key found: '{subject_key}'")

            SUBJECT_NAME_STRD_MAP[subject_key] = stored_subject_name
            # Todo: think about adding more data for each subject instead of only 'name'?

