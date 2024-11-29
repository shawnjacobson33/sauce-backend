from app.backend.data_collection.bookmakers import hubs as hbs
from app.backend.data_collection.bookmakers import direct as drt


LINE_RETRIEVERS = {
    'A': {
        "PropProfessor": hbs.PropProfessor
    },
    'B': {
        "OddsShopper": hbs.OddsShopper,
    },
    'C': {
        "Drafters": drt.Drafters,
        "MoneyLine": drt.MoneyLine,
        "OwnersBox": drt.OwnersBox,
        "Payday": drt.Payday,
        "SuperDraft": drt.SuperDraft,
        "VividPicks": drt.VividPicks,
    },
    'D': {
        "BoomFantasy": drt.BoomFantasy,
        "Dabble": drt.Dabble,
        "DraftKingsPick6": drt.DraftKingsPick6,
        "HotStreak": drt.HotStreak,
        "PrizePicks": drt.PrizePicks,
        "UnderdogFantasy": drt.UnderdogFantasy,
        "Rebet": drt.Rebet,
        "ParlayPlay": drt.ParlayPlay,
        "Sleeper": drt.Sleeper,
    }
}