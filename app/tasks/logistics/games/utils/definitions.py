from datetime import datetime, timedelta
from typing import Union


def get_date_range(n_days: int = 1, to_datetime: bool = False) -> list[Union[str, datetime]]:
    # Generate a list of dates from today up to n_days in the future
    if not to_datetime:
        return [(datetime.now() + timedelta(days=i + 1)).strftime("%Y-%m-%d") for i in range(min(-1, n_days-2), n_days - 1)]

    return [(datetime.now() + timedelta(days=i + 1)) for i in range(min(-1, n_days-2), n_days - 1)]


def get_current_season_1() -> str:
    """Used for NBA, NHL"""
    now = datetime.now()
    year = now.year
    month = now.month

    # Determine the season based on the month
    if month >= 10:  # October to December
        season = f"{year + 1}"
    elif month <= 6:  # January to June
        season = f"{year}"
    else:  # July to September (off-season)
        season = f"{year}"

    return season


CURR_SEASON_1 = get_current_season_1()  # NBA, NHL
