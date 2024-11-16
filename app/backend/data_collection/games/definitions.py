from app.backend.data_collection.games import schedules as sch
from app.backend.data_collection.games import box_scores as bxs


BOX_SCORE_RETRIEVERS = {
    'cbssports-nba': bxs.BasketballBoxScoreRetriever,
    'cbssports-ncaam': bxs.BasketballBoxScoreRetriever,
    'cbssports-nfl': bxs.FootballBoxScoreRetriever,
    'cbssports-ncaaf': bxs.FootballBoxScoreRetriever,
    'cbssports-nhl': bxs.IceHockeyBoxScoreRetriever,
}


SCHEDULE_RETRIEVERS = {
    'cbssports-nba': sch.BasketballScheduleRetriever,
    'cbssports-ncaam': sch.BasketballScheduleRetriever,
    'cbssports-nfl': sch.FootballScheduleRetriever,
    'cbssports-ncaaf': sch.FootballScheduleRetriever,
    'cbssports-nhl': sch.IceHockeyScheduleRetriever,
}