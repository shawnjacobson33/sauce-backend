
from app.services.utils.hashing import Hashing
from app.services.utils.standardization import maps


def load_subject_name_strd_map(rosters: list[dict]):
    subject_name_strd_map = {}

    for roster in rosters:
        for player in roster['players']:
            for attribute_field in ['team', 'position']:
                subject_key = Hashing.get_subject_key(player, attribute_field)
                subject_hash = Hashing.generate_hash(subject_key)
                subject_name_strd_map[subject_hash] = player['name']
                # Todo: think about adding more data for each subject instead of only 'name'?

    return subject_name_strd_map


class Standardizer:

    def __init__(self, rosters: list[dict] = None):
        self.rosters = rosters

        self.subject_name_strd_map = load_subject_name_strd_map(rosters) if rosters else {}

    @staticmethod
    def get_sport(league: str) -> str:
        if sport := maps.SPORT_MAP.get(league):
            return sport

        raise ValueError(f"No sport mapping found for league: '{league}'")

    @staticmethod
    def standardize_league_name(league_name: str) -> str:
        if strd_league_name := maps.LEAGUE_NAME_STRD_MAP.get(league_name):
            return strd_league_name

        raise ValueError(f"League name not found in standardization map: '{league_name}'")

    @staticmethod
    def standardize_period_name(period: str) -> str:
        if strd_period_name := maps.PERIOD_NAME_STRD_MAP.get(period):
            return strd_period_name

        raise ValueError(f"Period '{period}' not found in period map")

    @staticmethod
    def standardize_market_name(market_name: str, sport: str, period: str = None) -> str:
        if period:
            market_name = f'{period} {market_name}'

        if market_map_sport_filtered := maps.MARKET_NAME_STRD_MAP.get(sport):
            if strd_market_name := market_map_sport_filtered.get(market_name):
                return strd_market_name

            raise ValueError(f"Market '{market_name}' not found in '{sport}' market map")

        raise ValueError(f"Sport '{sport}' not found in market map")

    @staticmethod
    def standardize_subject_name(subject_name: str, subject_attribute_field: str) -> str:

        if strd_subject_name := maps.SUBJECT_NAME_STRD_MAP.get(subject_name):
            return strd_subject_name

        raise ValueError(f"Subject '{subject_name}' not found in subject map")