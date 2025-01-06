from fastapi import APIRouter
import pandas as pd
import asyncio
import os

from app.cache import session
from app.services.betting_lines import run_betting_lines_pipeline


router = APIRouter()


@router.get('/start_pipeline')
async def start_betting_lines_pipeline():
    asyncio.create_task(run_betting_lines_pipeline())
    return {'message': 'Pipeline started'}


def _get_query(league: str | None, bookmaker: str | None):
    query = []
    if league:
        query.append(league)

    if bookmaker:
        query.append(bookmaker)

    return query


@router.get('/betting_lines')
async def betting_lines(league: str | None = None, bookmaker: str | None = None):
    query = _get_query(league, bookmaker)
    curr_betting_lines = session.betting_lines.getlines(query=query)
    return (
        curr_betting_lines.head(20).to_dict(orient='records')
    )


# file_name = 'evaluated_betting_lines.csv'
#
# # Search for the file in the current directory and subdirectories
# for root, dirs, files in os.walk('.'):
#     if file_name in files:
#         absolute_path = os.path.abspath(os.path.join(root, file_name))
#         break
# else:
#     raise FileNotFoundError(f"{file_name} not found in the current directory or its subdirectories.")
#
# curr_betting_lines = pd.read_csv(absolute_path)