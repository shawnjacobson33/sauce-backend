from app.backend.data_collection.bookmakers import hubs as hbs
from app.backend.data_collection.bookmakers import primary as prm


LINES_RETRIEVERS = {
    "BoomFantasy": prm.BoomFantasy,
    "Dabble": prm.Dabble,
    "Drafters": prm.Drafters,
    "DraftKingsPick6": prm.DraftKingsPick6,
    "HotStreak": prm.HotStreak,
    "MoneyLine": prm.MoneyLine,
    "OwnersBox": prm.OwnersBox,
    "ParlayPlay": prm.ParlayPlay,
    "Payday": prm.Payday,
    # "PrizePicks": prm.PrizePicks,
    "Rebet": prm.Rebet,
    "Sleeper": prm.Sleeper,
    "SuperDraft": prm.SuperDraft,
    "UnderdogFantasy": prm.UnderdogFantasy,
    "VividPicks": prm.VividPicks,
    "OddsShopper": hbs.OddsShopper
}