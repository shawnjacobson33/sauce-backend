from app.services.utils.standardization import maps


class Standardization:

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

