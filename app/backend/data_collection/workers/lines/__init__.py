from .hubs import *
from .direct import *
from .utils import *


LINE_WORKERS = {
    'A': {
        "PropProfessor": PropProfessor,
    },
    'B': {
        "OddsShopper": OddsShopper,
    },
    'C': {
        "Drafters": Drafters,
        "MoneyLine": MoneyLine,
        "OwnersBox": OwnersBox,
        "Payday": Payday,
        "SuperDraft": SuperDraft,
        "VividPicks": VividPicks,
    },
    'D': {
        "BoomFantasy": BoomFantasy,
        "Dabble": Dabble,
        "DraftKingsPick6": DraftKingsPick6,
        "HotStreak": HotStreak,
        "PrizePicks": PrizePicks,
        "UnderdogFantasy": UnderdogFantasy,
        "Rebet": Rebet,
        "ParlayPlay": ParlayPlay,
        "Sleeper": Sleeper,
    }
}


__all__ = ['LinesSource', 'LinesRetriever', 'LINE_WORKERS', 'HUB_BOOKMAKERS']
