TEAMS_MAP = {
    'NBA': {
        # PHX - Phoenix Suns
          'PHO': 'PHX',  # Dabble, OwnersBox
        # NYK - New York Knicks
          'NY': 'NYK',  # Dabble
        # GSW - Golden State Warriors
          'GS': 'GSW',  # OwnersBox, Dabble, Payday
        # WAS - Washington Wizards
          'WSH': 'WAS',  # Payday
        # NOP - New Orleans Pelicans
          'NO': 'NOP',  # Dabble
        # SA - San Antonio Spurs
          'SAS': 'SA',  # Underdog
        # UTA - Utah Jazz
          'UTAH': 'UTA',  # Payday
    },
    'NFL': {
        # JAX - Jacksonville Jaguars
          'JAC': 'JAX',  # MoneyLine, PrizePicks, VividPicks, SuperDraft
        # WAS - Washington Commanders
          'WSH': 'WAS',  # BoomFantasy, Payday
        # LAR - Los Angeles Rams
          'LA': 'LAR',  # VividPicks
        # LV - Las Vegas Raiders
          'OAK': 'LV',  # OwnersBox
        # LAC - Los Angeles Chargers
          'SD': 'LAC',  # OwnersBox
    },
    'MLB': {},
    'NHL': {
        # CBJ - Columbus Blue Jackets
          'CLS': 'CBJ',  # OwnersBox, SuperDraft
        # LA - Los Angeles Kings
          'LA': 'LA',  # MoneyLine, SuperDraft, VividPicks, PrizePicks, OwnersBox
          'LAK': 'LA',  # ParlayPlay, DraftKingsPick6
        # NJ - New Jersey Devils
          'NJD': 'NJ',  # ParlayPlay, DraftKingsPick6
        # ANA - Anaheim Ducks
          'ANH': 'ANA',  # OwnersBox
        # SJ - San Jose Sharks
          'SJS': 'SJ',  # ParlayPlay
        # MTL - Montreal Canadiens
          'MON': 'MTL',  # SuperDraft, OwnersBox
        # VGK - Vegas Golden Knights
          'VGS': 'VGK',  # Payday
        # TB - Tampa Bay Lightning
          'TBL': 'TB',  # ParlayPlay
        # WPG - Winnipeg Jets
          'WPJ': 'WPG',  # ParlayPlay
    },
    'WNBA': {},
    'NCAA': {
        # SF - San Francisco
          'SF': 'SF',  # ParlayPlay, VividPicks, PrizePicks, Underdog Fantasy, BoomFantasy
          'SAN FRANCISCO': 'San Francisco',  # Payday
        # USU - Utah State
          'USU': 'USU',  # ParlayPlay,
          'UTS': 'USU',  # VividPicks
        # PEPP - Pepperdine
          'PEPP': 'PEPP',  # ParlayPlay, VividPicks, PrizePicks
          'PEPPERDINE': 'Pepperdine',  # Payday
        # WSU - Washington State
          'WSU': 'WSU',  # ParlayPlay, Dabble, Payday
          'WST': 'WSU',  # VividPicks
          'WASHINGTON STATE': 'Washington State',  # Payday
          'WASHST': 'Washington State',  # Dabble
        # UNLV - UNLV
          'UNLV': 'UNLV',  # ParlayPlay, VividPicks, Sleeper, Payday
        # WASH - Washington
          'WASH': 'WASH', # ParlayPlay, VividPicks, PrizePicks, Underdog Fantasy, Sleeper, Dabble
          'WSH': 'WASH',  # BoomFantasy, Payday
          'WAS': 'WASH',  # VividPicks
        # AFA - Air Force
          'AFA': 'AFA',  # ParlayPlay
          'AF': 'AFA',  # VividPicks
        # FRES - Fresno State
          'FRES': 'FRES',  # ParlayPlay
          'FRE': 'FRES',  # VividPicks
        # NEV - Nevada
          'NEV': 'NEV',  # ParlayPlay, VividPicks, PrizePicks, Underdog Fantasy, Sleeper
        # HOU - Houston
          'HOU': 'HOU',  # ParlayPlay, VividPicks, PrizePicks, Underdog Fantasy, Sleeper, Dabble, BoomFantasy
        # BSU - Boise State
          'BSU': 'BSU',
          'BOISE': 'Boise State',  # ParlayPlay
          'BOIS': 'BSU',  # Payday
        # HAW - Hawaii
          'HAW': 'HAW',  # VividPicks
          'HAWAII': 'Hawaii',  # ParlayPlay
        # BYU - BYU
          'BYU': 'BYU',  # ParlayPlay
        # UTAH - Utah
          'UTAH': 'UTAH',  # ParlayPlay
          'UTH': 'UTAH',  # VividPicks
        # OKLA - Oklahoma
          'OKL': 'OKLA',  # VividPicks
        # MIZZ - Missouri
          'MIZ': 'MIZZ',  # VividPicks
          'MIZZOU': 'MIZZ',  # Dabble
        # BAMA - Alabama
          'BAMA': 'BAMA',  # VividPicks
          'ALA': 'BAMA',  # ParlayPlay
        # IND - Indiana
          'IND': 'IND',  # VividPicks
          'INDIANA': 'Indiana',  # Payday
        # WAKE - Wake Forest
          'WAKE': 'WAKE',  # VividPicks
          'WAKE FOREST': 'Wake Forest',  # Payday
        # GONZ - Gonzaga
          'GONZAGA': 'Gonzaga',  # Payday
        # MICH - Michigan
          'MICHIGAN': 'Michigan',  # Payday
        # MARQ - Marquette
          'MARQUETTE': 'Marquette',  # Payday
        # PUR - Purdue
          'PURDUE': 'Purdue',  # Payday, Dabble
        # BALL - Ball State
          'BALLST': 'Ball State',  # ParlayPlay
        # BUFF - Buffalo
          'BUF': 'BUFF',  # ParlayPlay, VividPicks
        # OSU - Ohio State
          'OHIOST': 'Ohio State',  # Dabble
          'OHIO STATE': 'Ohio State',  # Payday
        # ARK - Arkansas
          'ARKA': 'ARK',  # Drafters
        # CAL - Cal
          'CALI': 'CAL',  # Drafters
        # LOU - Louisville
          'LOUI': 'LOU',  # Drafters
        # NEB - Nebraska
          'NEBRA': 'Nebraska',  # Drafters
        # WIS - Wisconsin
          'WISCO': 'WIS',  # Drafters
        # COLO - Colorado
          'COLOR': 'COLO',  # Drafters
          'COL': 'COLO',  # Underdog
        # TEX - Texas
          'TEXAS': 'TEX',  # Drafters
        # TENN - Tennessee
          'TEN': 'TENN',  # VividPicks
        # NW - Northwestern
          'NU': 'NW',  # Drafters
        # TULN - Tulane
          'TUL': 'TULN',  # VividPicks
        # KSU - Kansas State
          'KST': 'KSU',  # VividPicks
          'KANST': 'KSU',  # ParlayPlay
        # RUTG - Rutgers
          'RUT': 'RUTG',  # VividPicks
        # WAKE - Wake Forest
          'WF': 'WAKE',  # VividPicks
        # UNC - North Carolina
          'NC': 'UNC',  # Underdog
        # CLEM - Clemson
          'CLE': 'CLEM',  # Underdog
        # KU - Kansas
          'KAN': 'KU',  # Underdog
          'KANSAS': 'KU',  # Dabble
        # SC - South Carolina
          'SCAR': 'SC',  # Underdog
          'SOUTH CAROLINA': 'SC',  # Payday
        # PITT - Pittsburgh
          'PIT': 'PITT',  # Underdog
        # STAN - Stanford
          'STA': 'STAN',  # Underdog
        # SJSU - San Jose State
          'SJS': 'SJSU',  # Underdog
        # MARQ - Marquette
          'MAR': 'MARQ',  # Underdog
        # ORST - Oregon State
          'ORS': 'ORST',  # Underdog
        # WKU - Western Kentucky
          'WKY': 'WKU',  # VividPicks
          'WKENT': 'WKU',  # ParlayPlay
        # NAVY - Navy
          'NAV': 'NAVY',  # VividPicks
        # ISU - Iowa State
          'IOWAST': 'ISU',  # ParlayPlay
          'IAST': 'ISU',  # Dabble
        # ASU - Arizona State
          'ARZST': 'ASU',  # ParlayPlay
          'ARIST': 'ASU',  # Dabble
        # SYR - Syracuse
          'SYRA': 'SYR',  # ParlayPlay
        # LT - Louisiana Tech
          'LOUTCH': 'LT',  # ParlayPlay
        # XAV - Xavier
          'XAVIER': 'XAV',  # Dabble
        # USD - San Diego
          'SD': 'USD',  # Dabble
          'SAN DIEGO': 'USD', # Payday
        # ORE - Oregon
          'OREGON': 'ORE',  # Dabble
        # CREI - Creighton
          'CRE': 'CREI',  # Dabble
          'CREIGHTON': 'CREI',  # Payday
        # POR - Portland
          'PORT': 'POR',  # Underdog
        # AUB - Auburn
          'AUBURN': 'AUB',  # Dabble
        # BAY - Baylor
          'BAYLOR': 'BAY',  # Dabble
        # MD - Maryland
          'UMD': 'MD',  # BoomFantasy
        # MINN - Minnesota
          'MINNESOTA': 'MINN',  # Payday
        # ND - Notre Dame
          'NOTRE DAME': 'ND',  # Payday
        # GTOWN - Georgetown
          'GEORGETOWN': 'GTOWN',  # Payday
        # FAU - FAU
          'FLAATL': 'FAU',  # Dabble

    }
}


def clean_team(team: str, league: str) -> str:
    # get the teams associated with the mapped league name
    if partitioned_team_map := TEAMS_MAP.get(league):
        # map the team name to a standardized version in the TEAM_MAP and return
        return partitioned_team_map.get(team, team)

    # otherwise return original team name
    return team