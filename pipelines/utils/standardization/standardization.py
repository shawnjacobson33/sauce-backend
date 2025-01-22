
from pipelines.utils.exceptions import StandardizationError
from pipelines.utils.standardization import standardization_maps as maps


class Standardizer:

    def __init__(self, configs: dict, teams: list[dict] = None, subjects: list[dict] = None):
        self.configs = configs

        self.subject_name_strd_map = {}
        if subjects:
            maps.load_in_subject_strd_identity_map(teams, subjects)
            self.subject_name_strd_map = maps.SUBJECT_NAME_STRD_MAP

    @staticmethod
    def standardize_league_name(league_name: str) -> str:
        if strd_league_name := maps.LEAGUE_NAME_STRD_MAP.get(league_name):
            return strd_league_name

        raise StandardizationError(f"League name not found in standardization map: '{league_name}'")

    @staticmethod
    def standardize_period_name(period: str) -> str:
        if strd_period_name := maps.PERIOD_NAME_STRD_MAP.get(period):
            return strd_period_name

        raise StandardizationError(f"Period '{period}' not found in period map")

    def standardize_market_name(self, market_name: str, market_domain: str, sport: str, period: str = None) -> str:
        if period:
            market_name = f'{period} {market_name}'

        market_map_filtered = maps.MARKET_NAME_STRD_MAP[market_domain]
        if market_domain == 'PlayerProps':
            market_map_filtered = market_map_filtered[sport]

        strd_market_name = market_map_filtered.get(market_name)
        if strd_market_name not in self.configs['invalid_markets']:
            if strd_market_name:
                return strd_market_name

            raise StandardizationError(f"Market '{market_name}' not found in '{sport}' market map")

    def standardize_subject_name(self, subject_key: str) -> str:
        if strd_subject_name := self.subject_name_strd_map.get(subject_key):  # Todo: Store more info about each subject?
            return strd_subject_name

        raise StandardizationError(f"Subject '{subject_key}' not found in subject strd map")