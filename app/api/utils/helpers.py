import os

import pandas as pd


def get_query(league: str | None, bookmaker: str | None) -> dict:
    query = {}
    if league:
        query['league'] = league

    if bookmaker:
        query['bookmaker'] = bookmaker

    return query


def get_sample_betting_lines(file_name: str) -> pd.DataFrame:
    for root, dirs, files in os.walk('.'):
        if file_name in files:
            absolute_path = os.path.abspath(os.path.join(root, file_name))
            break
    else:
        raise FileNotFoundError(f"{file_name} not found in the current directory or its subdirectories.")


    return pd.read_csv(absolute_path)