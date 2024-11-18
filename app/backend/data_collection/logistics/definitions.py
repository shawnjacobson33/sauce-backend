from .games import *
from .rosters import *


BOX_SCORE_RETRIEVERS = {
    'cbssports-nba': BasketballBoxScoreRetriever,
    'cbssports-ncaam': BasketballBoxScoreRetriever,
    'cbssports-nfl': FootballBoxScoreRetriever,
    'cbssports-ncaaf': FootballBoxScoreRetriever,
    'cbssports-nhl': IceHockeyBoxScoreRetriever,
}


SCHEDULE_RETRIEVERS = {
    'cbssports-nba': BasketballScheduleRetriever,
    'cbssports-ncaam': BasketballScheduleRetriever,
    'cbssports-nfl': FootballScheduleRetriever,
    'cbssports-ncaaf': FootballScheduleRetriever,
    'cbssports-nhl': IceHockeyScheduleRetriever,
}


ROSTER_RETRIEVERS = {
    'cbssports-nba': BasketballRosterRetriever,
    'cbssports-ncaam': BasketballRosterRetriever,
    'cbssports-nfl': FootballRosterRetriever,
    'cbssports-ncaaf': FootballRosterRetriever,
    'cbssports-nhl': IceHockeyRosterRetriever,
}