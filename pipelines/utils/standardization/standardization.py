from pipelines.utils.exceptions import StandardizationError
from pipelines.utils.standardization import standardization_maps as maps


class Standardizer:
    """
    A class used to standardize various names such as leagues, periods, markets, and subjects.

    Attributes:
        configs (dict): Configuration settings for standardization.
        subject_name_strd_map (dict): A map of standardized subject names.
    """

    def __init__(self, configs: dict, teams: list[dict] = None, subjects: list[dict] = None):
        """
        Initializes the Standardizer with the given configurations, teams, and subjects.

        Args:
            configs (dict): Configuration settings for standardization.
            teams (list[dict], optional): A list of team dictionaries. Defaults to None.
            subjects (list[dict], optional): A list of subject dictionaries. Defaults to None.

        Raises:
            StandardizationError: If there is an error loading the subject standardization map.
        """
        self.configs = configs

        self.subject_name_strd_map = {}

        if subjects:
            try:
                maps.load_in_subject_strd_identity_map(teams, subjects)
                self.subject_name_strd_map = maps.SUBJECT_NAME_STRD_MAP

            except Exception as e:
                raise StandardizationError(f"Failed to load in subject strd map: {e}")

    @staticmethod
    def standardize_league_name(league_name: str) -> str:
        """
        Standardizes the given league name.

        Args:
            league_name (str): The name of the league to be standardized.

        Returns:
            str: The standardized league name.

        Raises:
            StandardizationError: If the league name is not found in the standardization map.
        """
        if strd_league_name := maps.LEAGUE_NAME_STRD_MAP.get(league_name):
            return strd_league_name

        raise StandardizationError(f"League name not found in standardization map: '{league_name}'")

    @staticmethod
    def standardize_period_name(period: str) -> str:
        """
        Standardizes the given period name.

        Args:
            period (str): The name of the period to be standardized.

        Returns:
            str: The standardized period name.

        Raises:
            StandardizationError: If the period name is not found in the period map.
        """
        if strd_period_name := maps.PERIOD_NAME_STRD_MAP.get(period):
            return strd_period_name

        raise StandardizationError(f"Period '{period}' not found in period map")

    def standardize_market_name(self, market_name: str, market_domain: str, sport: str, period: str = None) -> str:
        """
        Standardizes the given market name based on the market domain, sport, and optional period.

        Args:
            market_name (str): The name of the market to be standardized.
            market_domain (str): The domain of the market.
            sport (str): The sport associated with the market.
            period (str, optional): The period associated with the market. Defaults to None.

        Returns:
            str: The standardized market name.

        Raises:
            StandardizationError: If the market name is not found in the market map or is invalid.
        """
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
        """
        Standardizes the given subject name based on the subject key.

        Args:
            subject_key (str): The key of the subject to be standardized.

        Returns:
            str: The standardized subject name.

        Raises:
            StandardizationError: If the subject key is not found in the subject standardization map.
        """
        if strd_subject_name := self.subject_name_strd_map.get(
                subject_key):  # Todo: Store more info about each subject?
            return strd_subject_name

        raise StandardizationError(f"Subject '{subject_key}' not found in subject strd map")