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
    },
    'NFL': {
        # JAX - Jacksonville Jaguars
          'JAC': 'JAX',  # MoneyLine, PrizePicks, VividPicks, SuperDraft
        # WAS - Washington Commanders
          'WSH': 'WAS',  # BoomFantasy, Payday
        # LAR - Los Angeles Rams
          'LA': 'LAR',  # VividPicks
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
        # UNLV - UNLV
          'UNLV': 'UNLV',  # ParlayPlay, VividPicks, Sleeper, Payday
        # WASH - Washington
          'WASH': 'WASH', # ParlayPlay, VividPicks, PrizePicks, Underdog Fantasy, Sleeper, Dabble
          'WSH': 'WASH',  # BoomFantasy, Payday
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
        # BAMA - Alabama
          'BAMA': 'BAMA',  # VividPicks
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
    }
}


def clean_team(team: str, league: str):
    # get the teams associated with the mapped league name
    if partitioned_team_map := TEAMS_MAP.get(league):
        # map the team name to a standardized version in the TEAM_MAP, if it doesn't exist
        if mapped_team := partitioned_team_map.get(team):
            # return the standardized team name
            return mapped_team

    # otherwise return original team name
    return team