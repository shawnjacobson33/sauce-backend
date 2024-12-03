from .hubs import *
from .direct import *
from .utils import *


LINE_WORKERS = {
    'A': {
        "PropProfessor": PropProfessor,
        "Drafters": Drafters,
        "MoneyLine": MoneyLine,
        "OwnersBox": OwnersBox,
        "Payday": Payday,
        "SuperDraft": SuperDraft,
        "VividPicks": VividPicks,
        "Rebet": Rebet,
        "Sleeper": Sleeper,
        "UnderdogFantasy": UnderdogFantasy,
    },
    'B': {
        "OddsShopper": OddsShopper,
        "Drafters": Drafters,
        "MoneyLine": MoneyLine,
        "OwnersBox": OwnersBox,
        "Payday": Payday,
        "SuperDraft": SuperDraft,
        "VividPicks": VividPicks,
        "BoomFantasy": BoomFantasy,
        "HotStreak": HotStreak,
        "Rebet": Rebet,
        "Sleeper": Sleeper,
        "UnderdogFantasy": UnderdogFantasy,
    },
    'Backups': {
        "Dabble": Dabble,
        "DraftKingsPick6": DraftKingsPick6,
        "PrizePicks": PrizePicks,
        "ParlayPlay": ParlayPlay,
    }
}


__all__ = ['LinesSource', 'LinesRetriever', 'LINE_WORKERS', 'HUB_BOOKMAKERS']
