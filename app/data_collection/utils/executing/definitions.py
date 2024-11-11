from app.data_collection import games as gms
from app.data_collection import bookmakers as bkm


SCHEDULE_RETRIEVERS = {
    'basketball-reference': gms.NBAScheduleCollector,
    'pro-football-reference': gms.NFLScheduleCollector,
    'hockey-reference': gms.NHLScheduleCollector,
    'sports-reference': gms.NCAABScheduleCollector,
    'cbssports': gms.NCAAFScheduleCollector,
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
