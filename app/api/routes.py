import asyncio

import pandas as pd
from fastapi import APIRouter

from app.db import db
from app.services import run_pipeline

router = APIRouter()


@router.get('/start_pipeline')
async def start_betting_lines_pipeline():
    asyncio.create_task(run_pipeline())
    return {'message': 'Betting lines pipeline started.'}


def _to_dict(**kwargs):
    return {k: v for k, v in kwargs.items() if v is not None}


@router.get('/betting_lines')
async def betting_lines(bookmaker: str | None = None, league: str | None = None, subject: str | None = None):
    query = _to_dict(bookmaker=bookmaker, league=league, subject=subject)
    if most_recent_betting_lines := await db.betting_lines.get_betting_lines(query, most_recent=True):
        most_recent_betting_lines.sort(key=lambda x: x.get('ev'), reverse=True)  # Todo: set up an index in mongodb for this
        most_recent_betting_lines_df = pd.DataFrame(most_recent_betting_lines)
        most_recent_betting_lines_df['batch_timestamp'] = most_recent_betting_lines_df['batch_timestamp'].astype(str)
        most_recent_betting_lines_df['collection_timestamp'] = most_recent_betting_lines_df['collection_timestamp'].astype(str)
        return (
            most_recent_betting_lines_df.head(20).to_dict(orient='records')
        )

    return {'message': f'No betting lines found for query: {query}.'}
