from app.product_data.data_collection.utils.constants import IN_SEASON_LEAGUES


MARKET_INVALID_KEYWORDS = ['Season', 'Regular Season', 'Significant Strikes', 'Takedowns', '(Combo)']


def is_league_valid(league: str) -> bool:
    return league in IN_SEASON_LEAGUES


def is_market_valid(market: str) -> bool:
    return not any(invalid_keyword in market for invalid_keyword in MARKET_INVALID_KEYWORDS)
