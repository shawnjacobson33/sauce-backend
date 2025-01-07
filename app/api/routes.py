import asyncio
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, Request

from app.db import db
from app.services import run_betting_lines_pipeline

router = APIRouter()


@router.get('/start_pipeline')
async def start_betting_lines_pipeline():
    asyncio.create_task(run_betting_lines_pipeline())
    return {'message': 'Betting lines pipeline started.'}


def _to_dict(**kwargs):
    return {k: v for k, v in kwargs.items() if v is not None}

@router.get('/betting_lines')
async def betting_lines(bookmaker: str | None = None, league: str | None = None, subject: str | None = None):
    query = _to_dict(bookmaker=bookmaker, league=league, subject=subject)
    curr_betting_lines_list = await db.betting_lines.get_betting_lines(query)
    # await db.betting_lines.delete_betting_lines()  # todo: temporary
    curr_betting_lines_df = pd.DataFrame(curr_betting_lines_list)
    curr_betting_lines_df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return (
        curr_betting_lines_df.head(20).to_dict(orient='records')
    )


