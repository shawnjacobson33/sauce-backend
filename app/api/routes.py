from fastapi import APIRouter
import pandas as pd
import asyncio

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
    curr_betting_lines = await session.betting_lines.getlines(query=query)
    return (
        pd.DataFrame(curr_betting_lines).head(20).to_dict(orient='records')
    )
