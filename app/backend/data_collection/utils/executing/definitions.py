from app.backend.data_collection import games as gms
from app.backend.data_collection import bookmakers as bkm


BOX_SCORE_RETRIEVERS = {
    'cbssports-nba': gms.BasketballBoxScoreRetriever,
    'cbssports-ncaam': gms.BasketballBoxScoreRetriever,
    'cbssports-nfl': gms.BasketballBoxScoreRetriever,
    'cbssports-ncaaf': gms.BasketballBoxScoreRetriever,
    'cbssports-nhl': gms.BasketballBoxScoreRetriever,
}


SCHEDULE_RETRIEVERS = {
    'cbssports-nba': gms.BasketballScheduleRetriever,
    'cbssports-ncaam': gms.BasketballScheduleRetriever,
    'cbssports-nfl': gms.BasketballScheduleRetriever,
    'cbssports-ncaaf': gms.BasketballScheduleRetriever,
    'cbssports-nhl': gms.BasketballScheduleRetriever,
}


LINES_RETRIEVERS = {
    # "BetOnline": bkm.BetOnline,
    "BoomFantasy": bkm.BoomFantasy,
    "Dabble": bkm.Dabble,
    "Drafters": bkm.Drafters,
    "DraftKingsPick6": bkm.DraftKingsPick6,
    "HotStreak": bkm.HotStreak,
    "MoneyLine": bkm.MoneyLine,
    "OwnersBox": bkm.OwnersBox,
    "ParlayPlay": bkm.ParlayPlay,
    "Payday": bkm.Payday,
    "PrizePicks": bkm.PrizePicks,
    "Rebet": bkm.Rebet,
    "Sleeper": bkm.Sleeper,
    "SuperDraft": bkm.SuperDraft,
    "UnderdogFantasy": bkm.UnderdogFantasy,
    "VividPicks": bkm.VividPicks,
    "OddsShopper": bkm.OddsShopper
}
